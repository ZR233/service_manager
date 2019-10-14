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
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Panic(msg string)
}

type defaultLogger struct {
}

func (d defaultLogger) Debug(msg string) {
	log.Println(msg)
}

func (d defaultLogger) Info(msg string) {
	log.Println(msg)
}

func (d defaultLogger) Warn(msg string) {
	log.Println(msg)
}

func (d defaultLogger) Panic(msg string) {
	log.Panic(msg)
}

type zkLogger struct {
	logger Logger
}

func (z zkLogger) Printf(msg string, msg2 ...interface{}) {
	msg = fmt.Sprintln(msg, msg2)
	z.logger.Info(msg)
}
