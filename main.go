package main

import (
	"log"
	"os"
)

func init() {
	log.SetFlags(0)
}

func main() {
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}
