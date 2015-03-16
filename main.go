package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
)

var (
	lookupdHTTPAddrs = app.StringArray{}
)

type TopicResponse struct {
	Data struct {
		Topics []string `json:"topics"`
	} `json:"data"`
	StatusCode float64 `json:"status_code"`
	StatusTxt  string  `json:"status_txt"`
}

type RateEstimator struct {
	α          float64
	β          float64
	eventTimes chan time.Time
}

func (re RateEstimator) HandleMessage(m *nsq.Message) error {
	t := time.Now()
	select {
	case eventTimes <- t:
	default:
		<-eventTimes
		eventTimes <- t
		re.Update()
	}
	return nil
}

func (re RateEstimator) Update() {
	var xhat float64
	// TODO calculate xhat
	re.α += 1
	re.β += xhat
}

func NewConsumer() {
	cCfg := nsq.NewConfig()
	cCfg.MaxInFlight = maxInFlight
	consumer, err := nsq.NewConsumer(topic, "rate_estimator#ephemeral", cCfg)
	if err != nil {
		log.Fatal(err)
	}
	consumer.AddHandler(handler)
	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func main() {

	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Parse()

	// get a list of topics from nsqlookupd
	var topics []string

	for lookupd := range lookupdHTTPAddrs {
		resp, err := http.Get("http://127.0.0.1:4161/topics")
		if err != nil {
			log.Println(err)
			return
		}
		var topicInfo TopicResponse
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(&topicInfo)
		if err != nil {
			log.Println(err)
			return
		}
		topics = append(topics, topicInfo.Data.Topics...)
	}

	// create a reader per topic
	var consumers map[string]*nsq.Consumer

	// create a handler per topic
	var handlers map[string]*http.Handler

	// wait for ctrl-c
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

}
