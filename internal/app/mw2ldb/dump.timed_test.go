package mw2ldb

import (
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

func TestTimedDump(t *testing.T) {
	var dump Dump = &timedDump{
		size:3,
	}
	dump.Initialize()

	dump.Dump(&sarama.ConsumerMessage{})
	dump.Dump(&sarama.ConsumerMessage{})
	dump.Dump(&sarama.ConsumerMessage{})
	dump.Dump(&sarama.ConsumerMessage{})

	time.Sleep(time.Second*4)
}