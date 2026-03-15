package http_test

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adoublef/contrib/cmd/evetech/internal/evetech"
	. "github.com/adoublef/contrib/cmd/evetech/internal/net/http"
	"github.com/adoublef/contrib/cmd/evetech/internal/net/nettest"
)

var numRegions, numPages, numOrders int

// parRegions is concerned with number of parallel region requests can occure at once
// parRegions is concerned with number of parallel page request can occure at once
var parRegions, parPages int

func init() {
	flag.IntVar(&numRegions, "regions", 1, "number of regions")
	flag.IntVar(&numPages, "pages", 1, "number of pages")
	flag.IntVar(&numOrders, "orders", 1, "number of orders")
	flag.IntVar(&parRegions, "par-regions", 1, "parallel region processes")
	flag.IntVar(&parPages, "par-pages", 1, "parallel pages processes")
}

func TestHandler(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := t.Context()

		proxyS := newProxyServer(t)

		apiC, apiURL := apiClient(t, proxyS, numRegions, numPages, numOrders)

		c, sURL := testClient(t, apiC)

		v := make(url.Values)
		v.Set("base_url", apiURL)
		v.Set("par_regions", strconv.Itoa(parRegions))
		v.Set("par_pages", strconv.Itoa(parPages))

		url := fmt.Sprintf(`%s?%s`, sURL, v.Encode())
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := c.Do(req)
		ok(t, cmp.Or(err1, err2))
		defer res.Body.Close()

		equal(t, res.StatusCode, http.StatusOK)

		cr := csv.NewReader(res.Body)
		cr.ReuseRecord = true

		n := 0
	LOOP:
		for {
			rr, err := cr.Read()
			if err == io.EOF {
				break LOOP
			}
			// check the size
			ok(t, err)
			equal(t, len(rr), 12)
			n++
		}
		ok(t, res.Body.Close())

		equal(t, n, 1+(numRegions*numPages*numOrders)) // include the header
	})
}

func BenchmarkHandler(b *testing.B) { benchmarkHandler(b, 10, 50, 100, 1, 1) }

func benchmarkHandler(b *testing.B, numRegions, numPages, numOrders, parRegions, parPages int) {
	apiC, apiURL := apiClient(b, nil, numRegions, numPages, numOrders)
	httpC, sURL := testClient(b, apiC)

	v := make(url.Values)
	v.Set("base_url", apiURL)
	v.Set("par_regions", strconv.Itoa(parRegions))
	v.Set("par_pages", strconv.Itoa(parPages))
	url := fmt.Sprintf(`%s?%s`, sURL, v.Encode())

	for b.Loop() {
		req, err1 := http.NewRequest(http.MethodGet, url, nil)
		res, err2 := httpC.Do(req)
		if err := cmp.Or(err1, err2); err != nil || res.StatusCode != http.StatusOK {
			b.Fail()
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
}

func testClient(t testing.TB, httpC *http.Client) (*http.Client, string) {
	t.Helper()

	s := httptest.NewServer(Handler(httpC))
	t.Cleanup(func() { s.Close() })
	return s.Client(), s.URL
}

func ok(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", t.Name(), err)
	}
}

func equal[K comparable](t testing.TB, got, want K) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v; want %v", t.Name(), got, want)
	}
}

func apiClient(t testing.TB, proxyS *nettest.Server, regions, max, orders int) (httpC *http.Client, baseURL string) {
	t.Helper()

	mux := http.NewServeMux()

	const start = 10000000
	var rr = make([]int, regions)
	for i := range regions {
		rr[i] = start + (i + 1)
	}

	mux.HandleFunc("GET /v1/universe/regions", func(w http.ResponseWriter, r *http.Request) {
		// select a slice of this

		p, err := json.Marshal(rr)
		if err != nil {
			t.Fail()
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(p)))

		if _, err := w.Write(p); err != nil {
			t.Fail()
		}
	})

	mux.HandleFunc("HEAD /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
		_, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			t.Fail()
		}
		w.Header().Set("x-pages", strconv.Itoa(max))
	})

	var oo = make([]evetech.Order, orders)
	for i := range orders {
		oo[i] = evetech.Order{
			OrderID:      i + 1,
			IsBuyOrder:   false, // or true
			Issued:       "issued",
			LocationID:   1,
			MinVolume:    1,
			Price:        1,
			Range:        "range",
			SystemID:     1,
			TypeID:       1,
			VolumeRemain: 1,
			VolumeTotal:  1,
		}
	}

	mux.HandleFunc("GET /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
		_, err1 := strconv.ParseUint(r.PathValue("id"), 10, 64)
		_, err2 := strconv.ParseUint(r.URL.Query().Get("page"), 10, 64)
		if err := cmp.Or(err1, err2); err != nil {
			t.Fail()
		}

		p, err := json.Marshal(oo)
		if err != nil {
			t.Fail()
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(p)))

		if _, err := w.Write(p); err != nil {
			t.Fail()
		}
	})

	s := httptest.NewServer(mux)
	// See https://martin.baillie.id/wrote/gotchas-in-the-go-network-packages-defaults/
	if tr, ok := s.Client().Transport.(*http.Transport); ok {
		tr.MaxIdleConns = 100
		tr.MaxIdleConnsPerHost = 100
		tr.IdleConnTimeout = 90 * time.Second
	}

	if proxyS == nil {
		return s.Client(), s.URL
	}

	// applying a proxy infront of the api seems to make a difference in the speeds
	// when configured to regions
	hostport, err := proxyS.Client().Proxy("api", strings.TrimPrefix(s.URL, "http://"))
	ok(t, err)

	set, stop := proxyS.Client().Bandwidth("api", nettest.Downstream, 1000) // 1mbps
	if set {
		t.Cleanup(stop)
	}
	equal(t, set, true)

	set, stop = proxyS.Client().Latency("api", nettest.Downstream, 50, 25)
	if set {
		t.Cleanup(stop)
	}
	equal(t, set, true)

	parsed, err := url.Parse(s.URL)
	ok(t, err)
	parsed.Host = hostport

	return s.Client(), parsed.String()
	// return s.Client(), s.URL
	// no-proxy (53.001152ms), proxy (62.206ms)
}

func newProxyServer(t testing.TB) *nettest.Server {
	s := nettest.NewServer()
	t.Cleanup(func() { s.Close() })
	return s
}
