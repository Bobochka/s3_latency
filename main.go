package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
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
	concurrency := flag.Int("concurrency", 10, "concurrency")
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
	wg.Add(*concurrency)

	for j := 0; j < *concurrency; j++ {
		go func(threadID int) {
			i := 0
			firstByteSamples := []float64{}
			beforeBodySamples := []float64{}
			samples := []float64{}

			var tt time.Time

			trace := &httptrace.ClientTrace{
				GotConn: func(_ httptrace.GotConnInfo) { tt = time.Now() },
				GotFirstResponseByte: func() {
					dtt := float64(time.Since(tt) / time.Millisecond)
					firstByteSamples = append(firstByteSamples, dtt)
				},
			}

			var reqID, reqID2, maxReqID, maxReqID2 string
			var maxDur time.Duration

			for {
				i++
				t := time.Now()

				req, output := svc.GetObjectRequest(input)
				if err != nil {
					log.Println(err)
				}

				req.HTTPRequest = req.HTTPRequest.WithContext(httptrace.WithClientTrace(context.Background(), trace))

				req.ApplyOptions(func(r *request.Request) {
					start := time.Now()
					r.Handlers.Complete.PushBack(func(req *request.Request) {
						dtbb := float64(time.Since(start) / time.Millisecond)
						beforeBodySamples = append(beforeBodySamples, dtbb)

						reqID = req.RequestID
						reqID2 = req.HTTPResponse.Header.Get("x-amz-id-2")
					})
				})

				err := req.Send()

				if err != nil {
					log.Println(err.Error())
				}

				if output != nil && output.Body != nil {
					io.Copy(ioutil.Discard, output.Body)
					output.Body.Close()
				}

				d := time.Since(t)
				samples = append(samples, float64(d/time.Millisecond))

				if d > maxDur {
					maxReqID = reqID
					maxReqID2 = reqID2
					maxDur = d
				}

				if i%*size == 0 {
					logResults(threadID, firstByteSamples, beforeBodySamples, samples, maxReqID, maxReqID2)

					samples = []float64{}
					firstByteSamples = []float64{}
					beforeBodySamples = []float64{}
					maxDur = 0
					i = 0
				}
			}
		}(j)
	}

	wg.Wait()
}

func logResults(threadID int, firstByteSamples, beforeBodySamples, samples []float64, reqID, reqID2 string) {
	sort.Float64s(firstByteSamples)
	sort.Float64s(beforeBodySamples)
	sort.Float64s(samples)

	str := fmt.Sprintf("---------------------- thread %v sampled %v\n", threadID, len(samples))
	str += fmt.Sprintf("min: first byte: %.3fms; up to body: %.3fms; with body: %.3fms;\n", firstByteSamples[0], beforeBodySamples[0], samples[0])
	str += fmt.Sprintf("max: first byte: %.3fms; up to body: %.3fms; with body: %.3fms;\n", firstByteSamples[len(firstByteSamples)-1], beforeBodySamples[len(beforeBodySamples)-1], samples[len(samples)-1])
	str += fmt.Sprintf("50p: first byte: %.3fms; up to body: %.3fms; with body: %.3fms;\n", percentile(firstByteSamples, 0.5), percentile(beforeBodySamples, 0.5), percentile(samples, 0.5))
	str += fmt.Sprintf("75p: first byte: %.3fms; up to body: %.3fms; with body: %.3fms;\n", percentile(firstByteSamples, 0.75), percentile(beforeBodySamples, 0.75), percentile(samples, 0.75))
	str += fmt.Sprintf("95p: first byte: %.3fms; up to body: %.3fms; with body: %.3fms;\n", percentile(firstByteSamples, 0.95), percentile(beforeBodySamples, 0.95), percentile(samples, 0.95))
	str += fmt.Sprintf("Slowest request: req-id: %s x-amz-id-2: %s\n", reqID, reqID2)
	log.Println(str)
}
