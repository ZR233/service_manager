/*
@Time : 2019-10-14 10:16
@Author : zr
*/
package service_manager

import (
	"fmt"
	"log"
)

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Panic(args ...interface{})
}

type defaultLogger struct {
}

func (d defaultLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

func (d defaultLogger) Info(args ...interface{}) {
	log.Println(args...)
}

func (d defaultLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

func (d defaultLogger) Panic(args ...interface{}) {
	log.Panic(args...)
}

type zkLogger struct {
	logger Logger
}

func (z zkLogger) Printf(format string, msg ...interface{}) {
	z.logger.Info(fmt.Sprintf(format, msg...))
}
