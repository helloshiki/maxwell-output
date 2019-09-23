package mw2es

import "github.com/Shopify/sarama"

type Dump interface {
	Initialize() error
	LastOffset() (int64, error)
	Dump(msg *sarama.ConsumerMessage) error
}

type RowOpt struct {
	Database string
	Table    string
	PkID     string // optional
	Offset   int    // optional
	Limit    int    // optional
}

type LogOpt struct {
	Database  string
	Table     string
	BeginTime string // optional
	EndTime   string // optional
	Offset    int    // optional
	Limit     int    // optional
	Reverse bool  // optional
}
