package main

import (
	"io"
	"log"
	"os"
)

type Logger struct {
	silent bool
}

var logger *log.Logger

func initLogger(silent bool) {
	if silent {
		logger = log.New(io.Discard, "", 0)
	} else {
		logger = log.New(os.Stderr, "", 0)
	}
}
