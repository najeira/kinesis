package kinesis

import (
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"testing"
)

func TestChunkNewAndReady(t *testing.T) {
	c := &chunk{}
	if c.ready() {
		t.Errorf("chunk is ready")
	}
}

func TestChunkSingleAdd(t *testing.T) {
	c := &chunk{}
	entity := &kinesis.PutRecordsRequestEntry{
		Data:         []byte("hoge"),
		PartitionKey: aws.String("test"),
	}
	err := c.add(entity)
	if err != nil {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.entries) != 1 {
		t.Errorf("chunk.entries is invalid")
	}
	if c.size != len(entity.Data) {
		t.Errorf("chunk.size is invalid")
	}
	if c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestChunkAddJustMaxSizePerRecord(t *testing.T) {
	c := &chunk{}
	buf := make([]byte, maxSizePerRecord)
	entity := &kinesis.PutRecordsRequestEntry{
		Data:         buf,
		PartitionKey: aws.String("test"),
	}
	err := c.add(entity)
	if err != nil {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.entries) != 1 {
		t.Errorf("chunk.entries is invalid")
	}
	if c.size != len(entity.Data) {
		t.Errorf("chunk.size is invalid")
	}
	if c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestChunkErrRecordTooLarge(t *testing.T) {
	c := &chunk{}
	buf := make([]byte, maxSizePerRecord+1)
	entity := &kinesis.PutRecordsRequestEntry{
		Data:         buf,
		PartitionKey: aws.String("test"),
	}
	err := c.add(entity)
	if err != errRecordTooLarge {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.entries) != 0 {
		t.Errorf("chunk.entries is invalid")
	}
	if c.size != 0 {
		t.Errorf("chunk.size is invalid")
	}
	if c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestChunkAddLessLimit(t *testing.T) {
	c := &chunk{}
	for i := 0; i < maxCountPerCall; i++ {
		entity := &kinesis.PutRecordsRequestEntry{
			Data:         []byte("hoge"),
			PartitionKey: aws.String("test"),
		}
		err := c.add(entity)
		if err != nil {
			t.Errorf("chunk.add returns %s", err)
		}
		if len(c.entries) != (i + 1) {
			t.Errorf("chunk.entries is invalid")
		}
		if c.size != (len(entity.Data) * (i + 1)) {
			t.Errorf("chunk.size is invalid")
		}
		if c.full {
			t.Errorf("chunk.full is invalid")
		}
	}
}

func TestChunkAddFullCount(t *testing.T) {
	c := &chunk{}
	entity := &kinesis.PutRecordsRequestEntry{
		Data:         []byte("hoge"),
		PartitionKey: aws.String("test"),
	}
	for i := 0; i < maxCountPerCall; i++ {
		err := c.add(entity)
		if err != nil {
			t.Errorf("chunk.add returns %s", err)
		}
		if len(c.entries) != (i + 1) {
			t.Errorf("chunk.entries is invalid")
		}
		if c.size != (len(entity.Data) * (i + 1)) {
			t.Errorf("chunk.size is invalid")
		}
		if c.full {
			t.Errorf("chunk.full is invalid")
		}
	}
	err := c.add(entity)
	if err != errChunkFull {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.entries) != maxCountPerCall {
		t.Errorf("chunk.entries is invalid")
	}
	if c.size != (len(entity.Data) * maxCountPerCall) {
		t.Errorf("chunk.size is invalid")
	}
	if !c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestChunkAddFullSize(t *testing.T) {
	c := &chunk{}
	entity := &kinesis.PutRecordsRequestEntry{
		Data:         make([]byte, maxSizePerRecord),
		PartitionKey: aws.String("test"),
	}
	limit := maxSizePerCall / maxSizePerRecord
	for i := 0; i < limit; i++ {
		err := c.add(entity)
		if err != nil {
			t.Errorf("chunk.add returns %s", err)
		}
		if len(c.entries) != (i + 1) {
			t.Errorf("chunk.entries is invalid")
		}
		if c.size != (len(entity.Data) * (i + 1)) {
			t.Errorf("chunk.size is invalid")
		}
		if c.full {
			t.Errorf("chunk.full is invalid")
		}
	}
	err := c.add(entity)
	if err != errChunkFull {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.entries) != limit {
		t.Errorf("chunk.entries is invalid")
	}
	if c.size != (len(entity.Data) * limit) {
		t.Errorf("chunk.size is invalid")
	}
	if !c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestQueueEmpty(t *testing.T) {
	q := &queue{}
	if !q.empty() {
		t.Errorf("queue is not empty")
	}

	err := q.addRecord([]byte("hoge"))
	if err != nil {
		t.Errorf("queue.addRecord returns %s", err)
	}

	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
}

func TestQueueCount(t *testing.T) {
	q := &queue{}
	if q.count() != 0 {
		t.Errorf("queue is not zero")
	}

	err := q.addRecord([]byte("hoge"))
	if err != nil {
		t.Errorf("queue.addRecord returns %s", err)
	}

	if q.count() != 1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueSingleAdd(t *testing.T) {
	q := &queue{}
	d := []byte("hoge")
	err := q.addRecord(d)
	if err != nil {
		t.Errorf("queue.addRecord returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
	if q.count() != 1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueAddJustMaxSizePerRecord(t *testing.T) {
	q := &queue{}
	d := make([]byte, maxSizePerRecord)
	err := q.addRecord(d)
	if err != nil {
		t.Errorf("queue.addRecord returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
	if q.count() != 1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueErrRecordTooLarge(t *testing.T) {
	q := &queue{}
	d := make([]byte, maxSizePerRecord+1)
	err := q.addRecord(d)
	if err != errRecordTooLarge {
		t.Errorf("queue.addRecord returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
	if q.count() != 0 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueAddLessLimit(t *testing.T) {
	q := &queue{}
	d := []byte("hoge")
	for i := 0; i < maxCountPerCall; i++ {
		err := q.addRecord(d)
		if err != nil {
			t.Errorf("queue.addRecord returns %s", err)
		}
		if q.empty() {
			t.Errorf("queue.empty is invalid")
		}
		if q.count() != (i + 1) {
			t.Errorf("queue.count is invalid")
		}
		if len(q.chunks) != 1 {
			t.Errorf("queue.chunks is invalid")
		}
	}
}

func TestQueueAddMultiChunks(t *testing.T) {
	q := &queue{}
	d := []byte("hoge")
	for i := 0; i < maxCountPerCall; i++ {
		err := q.addRecord(d)
		if err != nil {
			t.Errorf("queue.addRecord returns %s", err)
		}
		if q.empty() {
			t.Errorf("queue.empty is invalid")
		}
		if q.count() != (i + 1) {
			t.Errorf("queue.count is invalid")
		}
	}

	if len(q.chunks) != 1 {
		t.Errorf("queue.chunks is invalid")
	}

	err := q.addRecord(d)
	if err != nil {
		t.Errorf("queue.addRecord returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue should not empty")
	}
	if q.count() != (maxCountPerCall + 1) {
		t.Errorf("queue.count is invalid")
	}
	if len(q.chunks) != 2 {
		t.Errorf("queue.chunks is invalid")
	}
}

func TestQueuePopChunkWhenEmpty(t *testing.T) {
	q := &queue{}
	c := q.popChunk()
	if c != nil {
		t.Errorf("queue.popChunk is invalid")
	}
}

func TestQueuePopChunkWhenNotReady(t *testing.T) {
	q := &queue{}
	for i := 0; i < maxCountPerCall; i++ {
		q.addRecord([]byte("hoge"))
		c := q.popChunk()
		if c != nil {
			t.Errorf("queue.popChunk is invalid")
		}
	}
}
