package mw2ldb

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"

	"github.com/helloshiki/maxwell-output/pkg/configparser"
)

type config struct {
	DBConfigs    map[string]*DBConfig    `mapstructure:"DBConfigs"`
	TableConfigs map[string]*TableConfig `mapstructure:"TableConfigs"`
	*MaxWell     `mapstructure:"MaxWell"`
	*Misc        `mapstructure:"Misc"`
}

type DBConfig struct {
	Database string `mapstructure:"Database"`
	Disable  bool   `mapstructure:"Disable"`
}

type Misc struct {
	ListenAddr string `mapstructure:"ListenAddr"`
}

type MaxWell struct {
	CacheSize    int      `mapstructure:"CacheSize"`
	WithoutMeta  bool     `mapstructure:"WithoutMeta"`
	KafkaVersion string   `mapstructure:"KafkaVersion"`
	Brokers      []string `mapstructure:"Brokers"`
	Topic        string   `mapstructure:"Topic"`
	Dir          string   `mapstructure:"Dir"`
	LDBName      string   `mapstructure:"Prefix"`
}

type TableConfig struct {
	TableName  string `mapstructure:"TableName"`
	DisableRow bool   `mapstructure:"DisableRow"`
	DisableLog bool   `mapstructure:"DisableLog"`
	Retention time.Duration `mapstructure:"Retention"`
}

func defaultConfig() *config {
	return &config{
		DBConfigs: map[string]*DBConfig{
			"@default": {},
		},
		TableConfigs: map[string]*TableConfig{
			"@default": {
				DisableLog: true,
				Retention: 0,
			},
		},
		MaxWell: &MaxWell{
			CacheSize:    1000,
			KafkaVersion: "2.3.0",
			Dir:          "/tmp",
			Brokers:      []string{"192.168.100.181:9092"},
			LDBName:      "maxwell",
		},
		Misc: &Misc{
			ListenAddr: ":8080",
		},
	}
}

func (c *config) DBConfig(database string) *DBConfig {
	if c, ok := c.DBConfigs[strings.ToLower(database)]; ok {
		return c
	}

	return c.DBConfigs["@default"]
}

func (c *config) TableConfig(table string) *TableConfig {
	if c, ok := c.TableConfigs[strings.ToLower(table)]; ok {
		return c
	}

	return c.TableConfigs["@default"]
}

func (c *config) validateBasic() error {
	_, err := sarama.ParseKafkaVersion(c.KafkaVersion)
	return err
}

func parseConfig(configFile string) (*config, error) {
	conf := defaultConfig()

	if err := configparser.LoadConfig(&conf, "AD", configFile); err != nil {
		return nil, err
	}

	if err := conf.validateBasic(); err != nil {
		return nil, fmt.Errorf("Error in config file: %v", err)
	}

	return conf, nil
}
