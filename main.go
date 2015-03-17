package main

import (
	"encoding/json"
	"flag"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/app"
)

var (
	n                = flag.Int("n", 10, "number of samples to collect before updating")
	βprior           = flag.Float64("beta", 0, "prior rate")
	αprior           = flag.Float64("alpha", 0, "prior shape")
	lookupdHTTPAddrs = app.StringArray{}
	maxInFlight      = flag.Int("max-in-flight", 10, "max number of messages to allow in flight")
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
	timeChan   chan time.Time
	topic      string
}

func NewRateEstimator(α, β float64, n int, topic string) *RateEstimator {
	re := &RateEstimator{
		α:          α,
		β:          β,
		eventTimes: make(chan time.Time, n),
		timeChan:   make(chan time.Time),
		topic:      topic,
	}
	go re.Start()
	return re
}

func (re *RateEstimator) HandleMessage(m *nsq.Message) error {
	re.timeChan <- time.Now()
	return nil
}

func (re *RateEstimator) Start() {
	for {
		t := <-re.timeChan
		select {
		case re.eventTimes <- t:
		default:
			re.Update()
			re.eventTimes <- t
		}
	}
}

func (re *RateEstimator) Update() {
	var Δhat float64
	N := len(re.eventTimes)
	ti := <-re.eventTimes
	n := 1
	for t := range re.eventTimes {
		Δ := t.Sub(ti).Seconds()
		Δhat += Δ / float64(n)
		ti = t
		n++
		if n == N {
			break
		}
	}
	re.α += float64(n)
	re.β += float64(n) * Δhat
	log.Println(re.topic, "  α:", re.α, "  β:", re.β, "  mean:", re.α/re.β, "  variance:", re.α/math.Pow(re.β, 2))
}

func main() {

	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Parse()

	// get a list of topics from nsqlookupd
	var topics []string

	for _, lookupd := range lookupdHTTPAddrs {
		u, err := url.Parse(lookupd)
		if err != nil {
			log.Fatal(err)
		}
		u.Path = "/topics"
		log.Println(u.String())
		resp, err := http.Get(u.String())
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

	rateEstimators := make(map[string]*RateEstimator)
	conf := nsq.NewConfig()
	conf.MaxInFlight = *maxInFlight
	for _, topic := range topics {
		consumer, err := nsq.NewConsumer(topic, "rate_estimator#ephemeral", conf)
		if err != nil {
			log.Fatal(err)
		}
		handler := NewRateEstimator(*αprior, *βprior, *n, topic)
		consumer.AddHandler(handler)
		rateEstimators[topic] = handler
		err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
		if err != nil {
			log.Fatal(err)
		}
	}

	// wait for ctrl-c
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

}
