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
	wait  time.Duration
	delay int

	queue  *queue
	logger Logger

	mu      sync.Mutex
	wg      sync.WaitGroup
	running bool
}

func NewWriter(conn *kinesis.Kinesis, name string) *Writer {
	w := &Writer{}
	w.conn = conn
	w.name = name
	w.wait = 1 * time.Second
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
		w.debugf("waikup flusher")
		w.wg.Add(1)
		go w.flusher()
	}
}

func (w *Writer) flusher() {
	w.debugf("flusher start")

	for {
		w.debugf("wait %d seconds", int(w.wait/time.Second))
		<-time.After(w.wait)

		w.logStats()
		w.Flush()
		w.logStats()

		if w.queue.empty() {
			w.debugf("empty")
			break
		}
	}

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	w.debugf("flusher end")
	w.wg.Done()
}

func (w *Writer) Wait() {
	w.wg.Wait()
}

func (w *Writer) Flush() {
	var errCnt int32 = 0
	var wg sync.WaitGroup
	for {
		c := w.queue.popChunk()
		if c == nil {
			w.debugf("no chunks")
			break
		}

		if c.entries == nil || len(c.entries) <= 0 {
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

	if errCnt > 0 {
		w.delay += 1
	} else {
		w.delay = 0
	}

	w.wait = time.Second * time.Duration((w.delay*w.delay)+1)
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
		if awserr := aws.Error(err); awserr != nil {
			if 500 <= awserr.StatusCode && awserr.StatusCode <= 599 {
				// temporary error. retry all
				w.warnf("%d %s", awserr.StatusCode, awserr.Error())
				w.queue.pushChunk(c)
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

	count := w.handleResults(entries, resp.Records)
	w.infof("put %d, failed %d, retry %d", succeededCount, failedCount, count)

	if failedCount > 0 || count > 0 {
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

	count := 0
	for i, r := range results {
		if r != nil {
			code := stringValue(r.ErrorCode)
			msg := stringValue(r.ErrorMessage)
			if code != "" {
				w.infof("%s: %s", code, msg)
				entry := entries[i]
				w.queue.addEntry(entry)
				count += 1
			} else {
				seq := stringValue(r.SequenceNumber)
				shard := stringValue(r.ShardID)
				w.verbosef("%s to %s", seq, shard)
			}
		}
	}
	return count
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
	mu     sync.Mutex
	chunks []*chunk
}

func (q *queue) empty() bool {
	return len(q.chunks) <= 0
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

func (q *queue) stats() string {
	q.mu.Lock()
	defer q.mu.Unlock()

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%d chunks [", len(q.chunks)))
	for i, c := range q.chunks {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("{%d, %d}", len(c.entries), c.size))
	}
	buf.WriteString("]")
	return buf.String()
}
