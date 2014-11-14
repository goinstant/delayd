package delayd

import (
	"fmt"
	"log"
)

const module = "delayd"

func prefix(level string) string {
	return fmt.Sprintf("[%s] %s:", level, module)
}

func makeFmt(level, format string) string {
	return fmt.Sprintf("%s %s", prefix(level), format)
}

func callLogf(level, format string, v ...interface{}) {
	realFormat := makeFmt(level, format)
	log.Printf(realFormat, v...)
}

func callLog(level string, a ...interface{}) {
	log.Println(append([]interface{}{prefix(level)}, a...)...)
}

// Debug logs a message at debug level
func Debug(a ...interface{}) {
	callLog("DEBUG", a...)
}

// Debugf logs a formatted message at debug level
func Debugf(format string, v ...interface{}) {
	callLogf("DEBUG", format, v...)
}

// Info logs a message at info level
func Info(a ...interface{}) {
	callLog("INFO", a...)
}

// Infof logs a formatted message at info level
func Infof(format string, v ...interface{}) {
	callLogf("INFO", format, v...)
}

// Warn logs a message at warn level
func Warn(a ...interface{}) {
	callLog("WARN", a...)
}

// Warnf logs a formatted message at warn level
func Warnf(format string, v ...interface{}) {
	callLogf("WARN", format, v...)
}

// Error logs a message at error level
func Error(a ...interface{}) {
	callLog("ERROR", a...)
}

// Errorf logs a formatted message at error level
func Errorf(format string, v ...interface{}) {
	callLogf("ERROR", format, v...)
}

// Fatal logs a message at error level then exits the process
func Fatal(a ...interface{}) {
	log.Fatalln(append([]interface{}{prefix("ERROR")}, a...)...)
}

// Fatalf logs a formatted message at error level then exits the process
func Fatalf(format string, v ...interface{}) {
	realFormat := makeFmt("ERROR", format)
	log.Fatalf(realFormat, v...)
}

// Panic logs a message at error level then panics
func Panic(a ...interface{}) {
	log.Panicln(append([]interface{}{prefix("ERROR")}, a...)...)
}

// Panicf logs a formatted message at error level then panics
func Panicf(format string, v ...interface{}) {
	realFormat := makeFmt("ERROR", format)
	log.Panicf(realFormat, v...)
}
