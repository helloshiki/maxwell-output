package mw2ldb

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"log"
	"os"
	"time"
)

type Server struct {
	logger     *zap.Logger
	configFile string
	*config
	dumper Dump
}

type opts struct {
	Config string
	Logger *zap.Logger
}

func NewServer(opts opts) *Server {
	return &Server{
		configFile: opts.Config,
		logger:     opts.Logger.With(zap.String("mod", "server")),
	}
}

func (c *Server) Start() error {
	//log := c.logger

	cfg, err := parseConfig(c.configFile)
	if err != nil {
		return err
	}

	if len(os.Getenv("PRINT_CONFIG")) > 0 {
		bs, _ := json.MarshalIndent(cfg, "", "  ")
		fmt.Println(string(bs))
	}

	c.config = cfg
	return c.dump()
}

func (c *Server) initialize() error {
	//log := c.logger
	return nil
}

func (c *Server) dump() error {
	var err error
	logger := c.logger

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version, err = sarama.ParseKafkaVersion(c.KafkaVersion)
	if err != nil {
		logger.Fatal(`bad kafka version`, zap.String("e", err.Error()))
		return err

	}

	// consumer []string{"192.168.100.181:9092"}
	consumer, err := sarama.NewConsumer(c.Brokers, config)
	if err != nil {
		logger.Fatal("create consumer error", zap.String("e", err.Error()))
		return err
	}

	defer func() { _ = consumer.Close() }()

	var dump Dump = &timedDump{
		logger:  c.logger,
		size:    c.CacheSize,
		config:  c.config,
		dir:     c.Dir,
		ldbName: c.LDBName,
	}

	if err := dump.Initialize(); err != nil {
		return err
	}

	offset, err := dump.LastOffset()
	if err != nil {
		return err
	}

	if offset < 0 {
		offset = sarama.OffsetOldest
	} else {
		offset++
	}

	c.logger.Info(`start`, zap.Int64(`offset`, offset))
	partitionConsumer, err := consumer.ConsumePartition(c.Topic, 0, offset)
	if err != nil {
		log.Fatal("ConsumePartition", zap.String("e", err.Error()))
		return err
	}
	defer func() { _ = partitionConsumer.Close() }()

	c.dumper = dump
	go c.startHttpServer()

	tick := time.NewTicker(time.Second * 5)
	delTick := time.NewTicker(time.Second * 15)
	for {
		select {
		case <-tick.C:
			if err := dump.Dump(nil); err != nil {
				panic(err)
			}
		case msg := <-partitionConsumer.Messages():
			if err := dump.Dump(msg); err != nil {
				panic(err)
			}
		case <-delTick.C:
			//if err := dump.ClearLog(); err != nil {
			//	panic(err)
			//}
		case err := <-partitionConsumer.Errors():
			logger.Fatal(`kafka`, zap.String("e", err.Error()))
			os.Exit(1)
		}
	}
}
