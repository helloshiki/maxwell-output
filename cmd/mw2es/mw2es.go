package main

import (
	"github.com/helloshiki/maxwell-output/internal/app/mw2es"
	"github.com/helloshiki/maxwell-output/internal/app/mw2ldb"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	config string
	logger *zap.Logger
)

func main() {
	err := Execute()
	if err != nil {
		panic(err)
	}
}

type opts struct {
	fileEnable    bool
	filename      string
	level         string
	consoleEnable bool
}

func initLogger(opts opts) *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "T",
		LevelKey:      "L",
		NameKey:       "K",
		CallerKey:     "N",
		MessageKey:    "M",
		StacktraceKey: "S",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("0102 15:04:05"))
		},
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}

	getDefaultString := func(newS, def string) string {
		s := newS
		if len(newS) == 0 {
			s = def
		}
		return s
	}

	var allCore []zapcore.Core
	if opts.fileEnable {
		hook := lumberjack.Logger{
			Filename:   getDefaultString(opts.filename, "/tmp/all.log"),
			MaxSize:    10,
			MaxBackups: 3,
			MaxAge:     7,
			Compress:   true,
			LocalTime:  false,
		}
		fileW := zapcore.AddSync(&hook)
		level := zapcore.DebugLevel
		_ = level.Set(getDefaultString(opts.level, "debug"))
		core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), fileW, level)
		allCore = append(allCore, core)
	}

	if opts.consoleEnable {
		consoleW := zapcore.Lock(os.Stdout)
		level := zapcore.DebugLevel
		_ = level.Set(getDefaultString(opts.level, "debug"))
		core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), consoleW, level)
		allCore = append(allCore, core)
	}

	core := zapcore.NewTee(allCore...)
	return zap.New(core, zap.AddCaller())
}

func Execute() error {
	rootCmd := &cobra.Command{
		Use: "mw2es",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logger = initLogger(opts{consoleEnable: true, fileEnable: true})

			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&config, "config", "c", "config.toml", "config file")
	rootCmd.AddCommand(newLdbCommand())
	rootCmd.AddCommand(newESCommand())
	return rootCmd.Execute()
}

func newLdbCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ldb",
		Short: "ldb",
		//Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := struct {
				Config string
				Logger *zap.Logger
			}{Logger: logger, Config: config}
			mw2ldb.NewServer(opts).Start()
			return nil
		},
	}
	return cmd
}


func newESCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "es",
		Short: "es",
		//Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := struct {
				Config string
				Logger *zap.Logger
			}{Logger: logger, Config: config}
			mw2es.NewServer(opts).Start()
			return nil
		},
	}
	return cmd
}
