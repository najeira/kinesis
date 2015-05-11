package kinesis

import (
	"fmt"
)

const (
	maxSizePerRecord = 50000   // 50KB
	maxSizePerCall   = 4500000 // 4.5MB
	maxCountPerCall  = 500
)

var (
	errChunkFull      = fmt.Errorf("chunk is full")
	errRecordTooLarge = fmt.Errorf("record is too large")
)

type Logger interface {
	Verbosef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Criticalf(format string, v ...interface{})
}

func stringValue(sp *string) string {
	if sp != nil {
		return *sp
	}
	return ""
}
