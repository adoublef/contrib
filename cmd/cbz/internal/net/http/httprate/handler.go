package httprate

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

func Handler(h http.Handler, r int, dur time.Duration) http.Handler {
	lim := rate.NewLimiter(rate.Every(dur/time.Duration(r)), r)
	var n atomic.Int64 // ? is this set
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Load() > 1 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if lim.Allow() {
			h.ServeHTTP(w, r)
			return
		}
		_ = n.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	})
}

// Default values for ExponentialBackOff.
const (
	DefaultInitialInterval     = 500 * time.Millisecond
	DefaultRandomizationFactor = 0.5
	DefaultMultiplier          = 1.5
	DefaultMaxInterval         = 60 * time.Second
)

type retryTransport struct {
	rt  http.RoundTripper
	lim *rate.Limiter
	it  *exponentialBackOff
}

func Transport(rt http.RoundTripper, r int, dur time.Duration) http.RoundTripper {
	lim := rate.NewLimiter(rate.Every(dur/time.Duration(r)), r)
	it := &exponentialBackOff{
		InitialInterval:     DefaultInitialInterval,
		RandomizationFactor: DefaultRandomizationFactor,
		Multiplier:          DefaultMultiplier,
		MaxInterval:         DefaultMaxInterval,
	}
	return &retryTransport{rt: rt, lim: lim, it: it}
}

func (t *retryTransport) RoundTrip(req *http.Request) (res *http.Response, err error) {
	ctx := req.Context()
	for {
		if err = t.lim.Wait(ctx); err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(t.it.next()): // exp
				continue
			}
		}
		res, err = t.rt.RoundTrip(req)
		if err != nil {
			return
		}
		switch res.StatusCode {
		// case http.StatusMovedPermanently: // Todo
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		default: // just return body
			return
		}
		d := t.delay(res.Header.Get("Retry-After"))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
	}
}

func (tr *retryTransport) delay(s string) time.Duration {
	if s == "" {
		return tr.it.next()
	}
	if n, err := strconv.ParseUint(s, 10, 64); err == nil {
		return time.Second * time.Duration(n)
	}
	if t, err := time.Parse(http.TimeFormat, s); err == nil {
		return time.Until(t)
	}
	return tr.it.next()
}

type exponentialBackOff struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration

	currentInterval time.Duration
}

func (b *exponentialBackOff) next() time.Duration {
	if b.currentInterval == 0 {
		b.currentInterval = b.InitialInterval
	}

	next := getRandomValueFromInterval(b.RandomizationFactor, rand.Float64(), b.currentInterval)
	b.incrementCurrentInterval()
	return next
}

func (b *exponentialBackOff) incrementCurrentInterval() {
	// Check for overflow, if overflow is detected set the current interval to the max interval.
	if float64(b.currentInterval) >= float64(b.MaxInterval)/b.Multiplier {
		b.currentInterval = b.MaxInterval
	} else {
		b.currentInterval = time.Duration(float64(b.currentInterval) * b.Multiplier)
	}
}

// [currentInterval - randomizationFactor * currentInterval, currentInterval + randomizationFactor * currentInterval].
func getRandomValueFromInterval(randomizationFactor, random float64, currentInterval time.Duration) time.Duration {
	if randomizationFactor == 0 {
		return currentInterval // make sure no randomness is used when randomizationFactor is 0.
	}
	var delta = randomizationFactor * float64(currentInterval)
	var minInterval = float64(currentInterval) - delta
	var maxInterval = float64(currentInterval) + delta
	// Get a random value from the range [minInterval, maxInterval].
	// The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
	// we want a 33% chance for selecting either 1, 2 or 3.
	return time.Duration(minInterval + (random * (maxInterval - minInterval + 1)))
}
