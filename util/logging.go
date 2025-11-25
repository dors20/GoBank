package util

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerConfig struct {
	Component string
	ServerID  string
	LogPath   string
}

func NewLogger(cfg LoggerConfig) (*zap.Logger, error) {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stack",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	encoder := zapcore.NewJSONEncoder(encoderCfg)

	level := zap.NewAtomicLevelAt(zap.InfoLevel)

	logFileName := cfg.LogPath
	if logFileName == "" {
		logFileName = "logs/server_" + cfg.ServerID + ".log"
	}
	logFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(logFile), level)

	logger := zap.New(core).With(
		zap.String("component", cfg.Component),
		zap.String("server_id", cfg.ServerID),
	)

	return logger, nil
}

func ErrorField(err error) zap.Field {
	return zap.Error(err)
}
