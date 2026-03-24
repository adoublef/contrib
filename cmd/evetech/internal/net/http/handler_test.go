package http_test

import (
	"cmp"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/adoublef/contrib/cmd/evetech/internal/evetech"
	. "github.com/adoublef/contrib/cmd/evetech/internal/net/http"
	"github.com/adoublef/contrib/cmd/evetech/internal/net/nettest"
)

func TestHandler(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := t.Context()

		const numRegions = 1 << 3
		const numPages = 1 << 3
		const numOrders = 1 << 3

		apiC, apiURL := apiClient(t, numRegions, numPages, numOrders)
		c, sURL := testClient(t, apiC)

		url := fmt.Sprintf(`%s/?base_url=%s`, sURL, apiURL)
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := c.Do(req)
		ok(t, cmp.Or(err1, err2))
		defer res.Body.Close()

		equal(t, res.StatusCode, http.StatusOK)
		equal(t, res.Header.Get("Content-Type"), "text/csv")
		// check content-disposition

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

		// do we include the header?

		equal(t, n, 0+(numRegions*numPages*numOrders)) // include the header
	})
}

// func BenchmarkHandler_0(b *testing.B) { benchmarkHandler(b, 1<<0, 1<<0, 1<<0) }
// func BenchmarkHandler_1(b *testing.B) { benchmarkHandler(b, 1<<1, 1<<1, 1<<1) }
// func BenchmarkHandler_2(b *testing.B) { benchmarkHandler(b, 1<<2, 1<<2, 1<<2) }
// func BenchmarkHandler_3(b *testing.B) { benchmarkHandler(b, 1<<3, 1<<3, 1<<3) }
// func BenchmarkHandler_4(b *testing.B) { benchmarkHandler(b, 1<<4, 1<<4, 1<<4) }
// func BenchmarkHandler_5(b *testing.B) { benchmarkHandler(b, 1<<5, 1<<5, 1<<5) }

func benchmarkHandler(b *testing.B, numRegions, numPages, numOrders int) {
	apiC, apiURL := apiClient(b, numRegions, numPages, numOrders)
	httpC, sURL := testClient(b, apiC)

	v := make(url.Values)
	v.Set("base_url", apiURL)
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

func apiClient(t testing.TB, regions, max, orders int) (httpC *http.Client, baseURL string) {
	t.Helper()

	mux := http.NewServeMux()

	{ // GET /v1/universe/regions
		const start = 10000000
		var rr = make([]int, regions)
		for i := range regions {
			rr[i] = start + (i + 1)
		}
		p, err := json.Marshal(rr)
		if err != nil {
			t.Fail()
		}

		mux.HandleFunc("GET /v1/universe/regions", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(len(p)))

			if _, err := w.Write(p); err != nil {
				t.Fail()
			}
		})
	}

	{ // HEAD /v1/markets/{id}/orders
		mux.HandleFunc("HEAD /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
			_, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
			if err != nil {
				t.Fail()
			}
			w.Header().Set("x-pages", strconv.Itoa(max))
		})
	}

	{ // GET /v1/markets/{id}/orders
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
		p, err := json.Marshal(oo)
		if err != nil {
			t.Fail()
		}

		mux.HandleFunc("GET /v1/markets/{id}/orders", func(w http.ResponseWriter, r *http.Request) {
			_, err1 := strconv.ParseUint(r.PathValue("id"), 10, 64)
			_, err2 := strconv.ParseUint(r.URL.Query().Get("page"), 10, 64)
			if err := cmp.Or(err1, err2); err != nil {
				t.Fail()
			}

			w.Header().Set("Content-Length", strconv.Itoa(len(p)))

			if _, err := w.Write(p); err != nil {
				t.Fail()
			}
		})
	}

	s := httptest.NewServer(mux)
	// See https://martin.baillie.id/wrote/gotchas-in-the-go-network-packages-defaults/
	if tr, ok := s.Client().Transport.(*http.Transport); ok {
		tr.MaxIdleConns = 100
		tr.MaxIdleConnsPerHost = 100
		tr.IdleConnTimeout = 90 * time.Second
	}

	return s.Client(), s.URL
}

func newProxyServer(t testing.TB) *nettest.Server {
	s := nettest.NewServer()
	t.Cleanup(func() { s.Close() })
	return s
}
