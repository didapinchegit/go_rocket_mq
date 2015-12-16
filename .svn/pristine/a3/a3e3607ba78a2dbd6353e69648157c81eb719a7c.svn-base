package rocketmq

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"
)

var ch = make(chan *RemotingCommand)
var client = NewDefaultRemotingClient()

func TestConnect(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	broker := "192.168.1.197:10911"
	namesrv := "192.168.1.234:9876"

	data, err := ioutil.ReadFile("request.txt")
	if err != nil {
		log.Print(err)
	}

	lines := strings.Split(string(data), "\n")

	var lastHeader, lastBody []byte
	for _, line := range lines {
		if strings.HasPrefix(line, "header=") {
			if lastHeader != nil {
				cmd := new(RemotingCommand)
				cmd.Body = lastBody

				err = json.Unmarshal(lastHeader, cmd)
				if err != nil {
					log.Print(err)
					return
				}
				callback := func(responseFuture *ResponseFuture) {
				}
				switch cmd.Code {
				case 101:
					getKvCallback := func(responseFuture *ResponseFuture) {
						jsonCmd, _ := json.Marshal(responseFuture.responseCommand)
						log.Printf("resp=%s", string(jsonCmd))
					}
					err := client.invokeAsync(namesrv, cmd, 5000, getKvCallback)
					if err != nil {
						log.Print(err)
					}
				case 105:
					getRouteInfoCallback := func(responseFuture *ResponseFuture) {
						jsonCmd, _ := json.Marshal(responseFuture.responseCommand)

						log.Printf("resp=%s", string(jsonCmd))
						log.Print(string(responseFuture.responseCommand.Body))
					}
					err := client.invokeAsync(namesrv, cmd, 5000, getRouteInfoCallback)
					if err != nil {
						log.Print(err)
					}
				case 34:
					err := client.invokeAsync(broker, cmd, 5000, callback)
					if err != nil {
						log.Print(err)
					}
				case 38:
					log.Print("getConsumerListCallback")
					getConsumerListCallback := func(responseFuture *ResponseFuture) {
						jsonCmd, _ := json.Marshal(responseFuture.responseCommand)

						log.Printf("getConsumerListCallback=%s", string(jsonCmd))
						log.Print(string(responseFuture.responseCommand.Body))
					}
					log.Print(cmd)
					err := client.invokeAsync(broker, cmd, 5000, getConsumerListCallback)
					if err != nil {
						log.Print(err)
					}
				case 11:
					pullCallback := func(responseFuture *ResponseFuture) {
						//if responseFuture.responseCommand.Code == 0 && len(responseFuture.responseCommand.Body) > 0 {
						//msgs := decodeMessage(responseFuture.responseCommand.Body)
						//for _, msg := range msgs {
						//log.Print(string(msg.Body))
						//}
						//}
					}
					err := client.invokeAsync(broker, cmd, 5000, pullCallback)
					if err != nil {
						log.Print(err)
					}
				}
			}
		}

		if strings.HasPrefix(line, "header=") {
			lastHeader = []byte(strings.TrimLeft(line, "header="))
		}
		if strings.HasPrefix(line, "body=") {
			lastBody = []byte(strings.TrimLeft(line, "body="))
		}
	}

	time.Sleep(1000 * time.Second)
}
