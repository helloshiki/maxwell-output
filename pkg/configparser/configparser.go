package configparser

import (
	"strings"

	"github.com/spf13/viper"
)

func LoadConfig(res interface{}, envPrefix, configPath string) error {
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return viper.Unmarshal(res)
}
