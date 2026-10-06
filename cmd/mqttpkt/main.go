// Command mqttpkt 是 MQTT 报文的场景入口。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"example.com/mqtt-packet/mqttpkt"
)

func emit(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}

func main() {
	sample := flag.String("sample", "", "varlen|wildcard|hash|qos|dup|work")
	flag.Parse()

	for _, s := range mqttpkt.Samples() {
		if s.Name == *sample {
			emit(s.Run())
			return
		}
	}
	if *sample == "work" {
		emit(mqttpkt.Work(800))
		return
	}
	fmt.Fprintln(os.Stderr, "未知场景："+*sample)
	os.Exit(2)
}
