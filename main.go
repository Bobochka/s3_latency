package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func percentile(values []float64, perc float64) float64 {
	ps := []float64{perc}

	scores := make([]float64, len(ps))
	size := len(values)
	if size > 0 {
		for i, p := range ps {
			pos := p * float64(size+1) //ALTERNATIVELY, DROP THE +1
			if pos < 1.0 {
				scores[i] = float64(values[0])
			} else if pos >= float64(size) {
				scores[i] = float64(values[size-1])
			} else {
				lower := float64(values[int(pos)-1])
				upper := float64(values[int(pos)])
				scores[i] = lower + (pos-math.Floor(pos))*(upper-lower)
			}
		}
	}
	return scores[0]
}

var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		MaxIdleConnsPerHost:   10,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

func main() {
	region := flag.String("region", "", "s3 region")
	bucket := flag.String("bucket", "", "s3 bucket")
	key := flag.String("key", "", "s3 key")
	size := flag.Int("size", 30, "sample size")
	ssl := flag.Bool("ssl", false, "whether enable ssl")

	flag.Parse()

	log.Println(*region, *bucket, *key, *size)

	s, err := session.NewSession(aws.NewConfig().WithDisableSSL(!*ssl))
	if err != nil {
		log.Fatalln(err)
	}

	svc := s3.New(s, aws.NewConfig().WithRegion(*region).WithHTTPClient(httpClient))
	input := &s3.GetObjectInput{
		Bucket: aws.String(*bucket),
		Key:    aws.String(*key),
	}

	wg := sync.WaitGroup{}
	wg.Add(10)

	for j := 0; j < 10; j++ {
		go func() {
			i := 0
			samples := []float64{}

			for {
				i++
				t := time.Now()

				obj, err := svc.GetObject(input)
				if err != nil {
					log.Println(err.Error())
				}

				if obj != nil && obj.Body != nil {
					ioutil.ReadAll(obj.Body)
					obj.Body.Close()
				}

				d := time.Since(t) / time.Millisecond
				samples = append(samples, float64(d))

				if i%*size == 0 {
					str := fmt.Sprintf("Sampled %v: ", len(samples))

					sort.Float64s(samples)

					str += fmt.Sprintf("min: %.3fms; ", samples[0])
					str += fmt.Sprintf("max: %.3fms; ", samples[len(samples)-1])
					str += fmt.Sprintf("50p: %.3fms; ", percentile(samples, 0.5))
					str += fmt.Sprintf("75p: %.3fms; ", percentile(samples, 0.75))
					str += fmt.Sprintf("95p: %.3fms; ", percentile(samples, 0.95))

					samples = []float64{}
					i = 0

					log.Println(str)
				}

			}
		}()
	}

	wg.Wait()
}
