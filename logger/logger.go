package logger

import (
	"fmt"
	"log"
	"os"
	"paxos/constants"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Initialize the logger
// https://dev.to/ronnymedina/golang-logging-configuration-with-zap-practical-implementation-tips-6g7
func InitLogger(Id int, isServer bool) *zap.SugaredLogger {

	var fileName string
	if isServer {
		fileName = fmt.Sprintf("server_%d.log", Id)
	} else {
		fileName = fmt.Sprintf("client_%d.log", Id)
	}
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open lof file %s %v", fileName, err)
	}

	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encoder := zapcore.NewJSONEncoder(config)
	fileSync := zapcore.AddSync(logFile)

	core := zapcore.NewCore(encoder, fileSync, constants.LOG_LEVEL)
	var logger *zap.Logger
	if isServer {
		logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.Fields(zap.Int("serverID", Id)))
	} else {
		logger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1), zap.Fields(zap.Int("clientID", Id)))
	}
	return logger.Sugar()
}
