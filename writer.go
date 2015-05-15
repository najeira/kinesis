package kinesis

import (
	"bytes"
	"fmt"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Writer struct {
	conn  *kinesis.Kinesis
	name  string
	delay int

	queue  *queue
	logger Logger
	stats  writerStats

	mu      sync.Mutex
	wg      sync.WaitGroup
	running bool
}

func NewWriter(conn *kinesis.Kinesis, name string) *Writer {
	w := &Writer{}
	w.conn = conn
	w.name = name
	w.delay = 0
	w.queue = &queue{}
	w.logger = nil
	w.running = false
	return w
}

func (w *Writer) SetLogger(l Logger) {
	w.logger = l
}

func (w *Writer) Add(b []byte) error {
	err := w.queue.addRecord(b)
	w.runFlusher()
	return err
}

func (w *Writer) runFlusher() {
	w.mu.Lock()
	r := w.running
	w.running = true
	w.mu.Unlock()

	if !r {
		w.debugf("wakeup flusher")
		w.wg.Add(1)
		go w.flusher()
	}
}

func (w *Writer) flusher() {
	w.debugf("flusher start")

	for {
		wait := (w.delay * w.delay) + 1
		w.debugf("wait %d seconds", wait)
		<-time.After(time.Duration(wait) * time.Second)

		//w.logStats()
		w.flush()
		//w.logStats()

		if w.queue.empty() {
			w.debugf("empty")
			break
		}
	}

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	if !w.queue.empty() {
		w.runFlusher()
	}

	w.debugf("flusher end")
	w.wg.Done()
}

func (w *Writer) Wait() {
	w.wg.Wait()
}

func (w *Writer) flush() {
	start := time.Now()
	beforeCount := w.stats.Count()

	w.updateRemainingCount()

	noErr := w.flushQueue()

	elapsed := time.Now().Sub(start)
	w.stats.setLatency(elapsed)

	afterCount := w.stats.Count()
	throughput := float64(beforeCount - afterCount)
	if elapsed > time.Second {
		throughput /= elapsed.Seconds()
	}
	w.stats.setThroughput(throughput)

	w.updateRemainingCount()

	if noErr {
		w.delay = 0
	} else {
		w.delay += 1
	}
}

func (w *Writer) flushQueue() bool {
	var errCnt int32 = 0

	var wg sync.WaitGroup
	for {
		c := w.queue.popChunk()
		if c == nil {
			w.debugf("no chunks")
			break
		}

		if len(c.entries) <= 0 {
			panic("no entries")
		}

		w.debugf("flushing")

		wg.Add(1)
		go func() {
			if !w.putRecords(c) {
				atomic.AddInt32(&errCnt, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	return errCnt <= 0
}

func (w *Writer) Stats() WriterStats {
	return &w.stats
}

func (w *Writer) updateRemainingCount() {
	w.stats.setRemainingCount(int64(w.queue.count()))
}

func (w *Writer) putRecords(c *chunk) bool {
	params := &kinesis.PutRecordsInput{
		Records:    c.entries,
		StreamName: aws.String(w.name),
	}
	resp, err := w.conn.PutRecords(params)
	return w.handleResponse(c, resp, err)
}

func (w *Writer) handleResponse(c *chunk, resp *kinesis.PutRecordsOutput, err error) bool {
	if err != nil {
		var cnt int64 = int64(len(c.entries))
		w.stats.incrFailedCount(cnt)

		if awserr := aws.Error(err); awserr != nil {
			if 500 <= awserr.StatusCode && awserr.StatusCode <= 599 {
				// temporary error. retry all
				w.warnf("%d %s", awserr.StatusCode, awserr.Error())
				w.queue.pushChunk(c)
				w.stats.incrRetriedCount(cnt)
				return false
			} else if awserr.StatusCode != 200 {
				// 400 or other error. should not retry
				w.errorf("%d %s", awserr.StatusCode, awserr.Error())
				return false
			}
		} else {
			// internal error. retry all
			w.errorf(err.Error())
			w.queue.pushChunk(c)
			w.stats.incrRetriedCount(cnt)
			return false
		}
	}

	// retry failed records
	return w.handleOutput(c.entries, resp)
}

func (w *Writer) handleOutput(entries []*kinesis.PutRecordsRequestEntry, resp *kinesis.PutRecordsOutput) bool {
	if resp == nil {
		w.errorf("PutRecordsOutput is nil")
		return false
	} else if resp.FailedRecordCount == nil {
		w.errorf("PutRecordsOutput.FailedRecordCount is nil")
		return false
	} else if resp.Records == nil {
		w.errorf("PutRecordsOutput.Records is nil")
		return false
	}

	failedCount := *resp.FailedRecordCount
	succeededCount := len(entries) - int(failedCount)

	w.stats.incrCount(int64(succeededCount))

	retriedCount := w.handleResults(entries, resp.Records)
	w.infof("put %d, failed %d, retry %d", succeededCount, failedCount, retriedCount)

	if failedCount > 0 || retriedCount > 0 {
		// update stats
		w.stats.incrFailedCount(int64(failedCount))
		w.stats.incrRetriedCount(int64(retriedCount))
		return false
	}

	// all records were putted successfully
	return true
}

func (w *Writer) handleResults(entries []*kinesis.PutRecordsRequestEntry, results []*kinesis.PutRecordsResultEntry) int {
	if results == nil {
		w.errorf("PutRecordsOutput.Records is nil")
		return 0
	}

	sentBytes := 0
	retriedCount := 0
	for i, r := range results {
		if r != nil {
			entry := entries[i]
			code := stringValue(r.ErrorCode)
			msg := stringValue(r.ErrorMessage)
			if code != "" {
				w.infof("%s: %s", code, msg)
				w.queue.addEntry(entry)
				retriedCount += 1
			} else {
				seq := stringValue(r.SequenceNumber)
				shard := stringValue(r.ShardID)
				w.verbosef("%s to %s", seq, shard)
				sentBytes += len(entry.Data)
			}
		}
	}

	// update stats
	w.stats.incrBytes(int64(sentBytes))

	return retriedCount
}

func (w *Writer) logStats() {
	if w.logger != nil {
		w.debugf(w.queue.stats())
	}
}

func (w *Writer) verbosef(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Verbosef(format, v...)
	}
}

func (w *Writer) debugf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Debugf(format, v...)
	}
}

func (w *Writer) infof(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Infof(format, v...)
	}
}

func (w *Writer) warnf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Warnf(format, v...)
	}
}

func (w *Writer) errorf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Errorf(format, v...)
	}
}

func (w *Writer) criticalf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Criticalf(format, v...)
	}
}

type WriterStats interface {
	// The number of total rows were sent to Kinesis
	Count() int64

	// The number of total bytes were sent to Kinesis
	Bytes() int64

	// The number of total failued record count
	FailedCount() int64

	// The number of total retries count
	RetriedCount() int64

	// The number of rows in the writer
	RemainingCount() int64

	// The last latency of Kinesis
	Latency() time.Duration

	// The throughput
	Throughput() float64
}

type writerStats struct {
	count          int64
	bytes          int64
	failedCount    int64
	retriedCount   int64
	remainingCount int64
	latency        int64
	throughput     int64
}

func (s *writerStats) Count() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *writerStats) Bytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

func (s *writerStats) FailedCount() int64 {
	return atomic.LoadInt64(&s.failedCount)
}

func (s *writerStats) RetriedCount() int64 {
	return atomic.LoadInt64(&s.retriedCount)
}

func (s *writerStats) RemainingCount() int64 {
	return atomic.LoadInt64(&s.remainingCount)
}

func (s *writerStats) Latency() time.Duration {
	v := atomic.LoadInt64(&s.latency)
	return time.Duration(v)
}

func (s *writerStats) Throughput() float64 {
	v := atomic.LoadInt64(&s.throughput)
	return float64(v / 1000000)
}

func (s *writerStats) incrCount(value int64) {
	atomic.AddInt64(&s.count, value)
}

func (s *writerStats) incrBytes(value int64) {
	atomic.AddInt64(&s.bytes, value)
}

func (s *writerStats) incrFailedCount(value int64) {
	atomic.AddInt64(&s.failedCount, value)
}

func (s *writerStats) incrRetriedCount(value int64) {
	atomic.AddInt64(&s.retriedCount, value)
}

func (s *writerStats) setRemainingCount(value int64) {
	atomic.StoreInt64(&s.remainingCount, value)
}

func (s *writerStats) setLatency(value time.Duration) {
	atomic.StoreInt64(&s.latency, value.Nanoseconds())
}

func (s *writerStats) setThroughput(value float64) {
	atomic.StoreInt64(&s.throughput, int64(value*1000000))
}

type chunk struct {
	entries []*kinesis.PutRecordsRequestEntry
	size    int
	full    bool
	created time.Time
}

func (c *chunk) add(entry *kinesis.PutRecordsRequestEntry) error {
	if len(entry.Data) > maxSizePerRecord {
		return errRecordTooLarge
	} else if len(c.entries) >= maxCountPerCall {
		// mark the chunk as full
		// this chunk will be sent at next batch
		c.full = true
		return errChunkFull
	} else if c.size+len(entry.Data) > maxSizePerCall {
		// mark the chunk as full
		// this chunk will be sent at next batch
		c.full = true
		return errChunkFull
	}

	if c.entries == nil {
		c.entries = make([]*kinesis.PutRecordsRequestEntry, 0, maxCountPerCall)
		c.created = time.Now()
	}

	c.entries = append(c.entries, entry)
	c.size += len(entry.Data)
	return nil
}

func (c *chunk) ready() bool {
	if c.full {
		return true
	} else if c.entries == nil || len(c.entries) <= 0 {
		return false
	}
	delta := time.Now().Sub(c.created)
	return delta > (1 * time.Second)
}

type queue struct {
	mu     sync.RWMutex
	chunks []*chunk
}

func (q *queue) empty() bool {
	q.mu.RLock()
	n := len(q.chunks)
	q.mu.RUnlock()
	return n <= 0
}

func (q *queue) popChunk() *chunk {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.chunks) <= 0 {
		return nil // no chunks
	}

	c := q.chunks[0]
	if !c.ready() {
		return nil // not ready
	}

	q.chunks = q.chunks[1:]
	return c
}

func (q *queue) pushChunk(c *chunk) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.chunks = append(q.chunks, c)
}

func (q *queue) addRecord(data []byte) error {
	entry := &kinesis.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: aws.String(strconv.Itoa(rand.Int())),
	}
	return q.addEntry(entry)
}

func (q *queue) addEntry(entry *kinesis.PutRecordsRequestEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.chunks) <= 0 {
		q.chunks = append(q.chunks, &chunk{})
	}

	// a chunk that will be added the record
	c := q.chunks[len(q.chunks)-1]

	if err := c.add(entry); err != nil {
		if err != errChunkFull {
			return err
		}

		// append a new chunk to tail of the queue
		nc := &chunk{}
		q.chunks = append(q.chunks, nc)

		// add the record to the new chunk
		if err2 := nc.add(entry); err2 != nil {
			return err2
		}
	}
	return nil
}

func (q *queue) count() int {
	q.mu.RLock()
	var ret int = 0
	for _, c := range q.chunks {
		ret += len(c.entries)
	}
	q.mu.RUnlock()
	return ret
}

func (q *queue) stats() string {
	var buf bytes.Buffer
	q.mu.RLock()
	buf.WriteString(fmt.Sprintf("%d chunks [", len(q.chunks)))
	for i, c := range q.chunks {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("{%d, %d}", len(c.entries), c.size))
	}
	q.mu.RUnlock()
	buf.WriteString("]")
	return buf.String()
}
