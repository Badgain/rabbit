package utils

import "log"

func FailHandler(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
