package pkg

import (
	"log"
	"os"
)

var infoLogger = log.New(os.Stderr, "[INFO] ", log.Lmicroseconds)
var debugLogger = log.New(os.Stderr, "[DEBUG] ", log.Lmicroseconds)

var InfoMode bool = true
var DebugMode bool = true

// debug print a message (to STDERR) only if debug mode is enabled
func debug(msg string, params ...interface{}) {
	if DebugMode {
		debugLogger.Printf(msg, params...)
	}
}

// debugInd is like 'debug()' but can handle indentation as well
func debugInd(ind int, msg string, params ...interface{}) {
	if DebugMode {
		indentation := ""
		for i := 0; i < ind; i++ {
			indentation += "    "
		}
		debugLogger.Printf(indentation+msg, params...)
	}
}

// info print a message (to STDERR) only if info mode is enabled
func info(msg string, params ...interface{}) {
	if InfoMode {
		infoLogger.Printf(msg, params...)
	}
}
