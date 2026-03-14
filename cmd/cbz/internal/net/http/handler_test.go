package http_test

import (
	"archive/zip"
	"cmp"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/adoublef/contrib/cmd/cbz/internal/net/http"
	"github.com/adoublef/contrib/cmd/cbz/internal/net/http/httprate"
	"github.com/adoublef/contrib/cmd/cbz/internal/net/netproxy"
	"github.com/krolaw/zipstream"
)

var numChapters, numImages int

func init() {
	flag.IntVar(&numChapters, "chapters", 1, "number of chapters")
	flag.IntVar(&numImages, "images", 1, "number of images")
}

func TestHandler(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		ctx := t.Context()

		// proxyS := newProxyServer(t)

		const numChapters = 100
		const numImages = 100
		const compress = 1 << 0

		apiC, apiURL := apiClient(t, nil, numChapters, numImages)

		testC, testURL := testClient(t, apiC)

		url := fmt.Sprintf("%s/?series_url=%s", testURL, apiURL+"/series/1")
		req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		res, err2 := testC.Do(req)
		ok(t, cmp.Or(err1, err2))
		equal(t, res.StatusCode, http.StatusOK) // stream means this is always going to be the case

		// bufio.Reader?

		// stream zip
		zr := zipstream.NewReader(res.Body)

		var i int
		for {
			fh, err := zr.Next()
			if err == io.EOF {
				break
			}
			i++
			ok(t, err)
			// is another file inside
			equal(t, len(fh.Name) > 0 && fh.Name[len(fh.Name)-1] == '/', false)
			// method is store
			equal(t, fh.Method, zip.Store)

			// internal zip
			r := zipstream.NewReader(zr)
			var j int
			for {
				fh, err := r.Next()
				if err == io.EOF {
					break
				}
				j++
				ok(t, err)
				// is another file inside
				equal(t, len(fh.Name) > 0 && fh.Name[len(fh.Name)-1] == '/', false)
				// method is store
				equal(t, fh.Method, zip.Store)

				n, err := io.Copy(io.Discard, r)
				ok(t, err)
				equal(t, n, 86387) // equal(t, n, 20028)
			}
			// count number of images
			equal(t, j, numImages)
		}
		equal(t, i, numChapters)
	})
}

func testClient(t testing.TB, httpC *http.Client) (*http.Client, string) {
	s := httptest.NewServer(Handler(httpC))
	t.Cleanup(s.Close)

	return s.Client(), s.URL
}

func ok(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: unexpected error: %v", t.Name(), err)
	}
}

func equal[K comparable](t testing.TB, got, want K) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got=%v; want=%v", t.Name(), got, want)
	}
}

//go:embed testdata/*.html testdata/*.xml testdata/*.jpg
var embedFS embed.FS

func apiClient(t testing.TB, proxyS *netproxy.Server, chapters, images int) (apiC *http.Client, apiURL string) {
	t.Helper()

	funcMap := template.FuncMap{
		// See https://stackoverflow.com/a/22716709
		"N":    func(n int) []struct{} { return make([]struct{}, n) },
		"sub":  func(a, b int) int { return a - b },
		"inc":  func(i int) int { return i + 1 },
		"iota": func(i int) string { return strconv.Itoa(i) },
		"join": func(sep string, s ...string) string { return strings.Join(s, sep) },
		"url": func(base string, s ...string) string {
			u, err := url.JoinPath(base, s...)
			if err != nil {
				t.Fatal(err)
			}
			return u
		},
	}

	series, err1 := template.New("series.html").Funcs(funcMap).ParseFS(embedFS, "testdata/series.html")
	chapter, err3 := template.New("chapter.html").Funcs(funcMap).ParseFS(embedFS, "testdata/chapter.html")
	if err := cmp.Or(err1, err3); err != nil {
		t.Fatal(err)
	}

	// we want to not get ip blocked
	// use a token bucket for rate limiting
	mux := http.NewServeMux()
	// return series
	mux.HandleFunc("GET /series/{series}/full-chapter-list", func(w http.ResponseWriter, r *http.Request) {
		// some formatted string
		_, err := strconv.ParseUint(r.PathValue("series"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		data := struct {
			N       int
			BaseURL string
		}{
			N:       chapters,
			BaseURL: apiURL,
		}
		if err := series.Execute(w, data); err != nil {
			t.Fatal(err)
		}
	})
	// return chapters
	mux.HandleFunc("GET /chapters/{chapter}/images", func(w http.ResponseWriter, r *http.Request) {
		// some formatted string
		id, err := strconv.ParseUint(r.PathValue("chapter"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		data := struct {
			N       int
			Chapter int
			BaseURL string
		}{
			N:       images,
			Chapter: int(id),
			BaseURL: apiURL,
		}
		if err := chapter.Execute(w, data); err != nil {
			t.Fatal(err)
		}
	})
	// return a file
	mux.HandleFunc("GET /images/{image}", func(w http.ResponseWriter, r *http.Request) {
		// parse path?

		f, err1 := embedFS.Open("testdata/image.jpg") // base64
		fi, err2 := f.Stat()
		if err := cmp.Or(err1, err2); err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Content-Length
		w.Header().Set("Content-Length", strconv.Itoa(int(fi.Size())))
		// Content-Type ?

		if r.Method != http.MethodHead {
			if _, err := io.Copy(w, f); err != nil {
				t.Fatal(err)
			}
		}
	})

	s := httptest.NewServer(httprate.Handler(mux, 100, time.Second))
	t.Cleanup(s.Close)

	// customize http client
	if _, ok := s.Client().Transport.(*http.Transport); ok {
		// tr.MaxIdleConns = 100
		// tr.MaxIdleConnsPerHost = 100
	}
	// works
	s.Client().Transport = httprate.Transport(s.Client().Transport, 100, time.Second)

	if proxyS == nil {
		return s.Client(), s.URL
	}

	// setup proxy
	p, err := proxyS.Client().Proxy("api", strings.TrimPrefix(s.URL, "http://"))
	ok(t, err)
	parsed, err := url.Parse(s.URL)
	ok(t, err)
	parsed.Host = p.Listen()

	set, stop := p.Bandwidth(netproxy.Downstream, 1000) // 1mbps
	if set {
		t.Cleanup(stop)
	}
	equal(t, set, true)

	return s.Client(), parsed.String()
}

func newProxyServer(t testing.TB) *netproxy.Server {
	s := netproxy.NewServer()
	t.Cleanup(func() { s.Close() })
	return s
}
