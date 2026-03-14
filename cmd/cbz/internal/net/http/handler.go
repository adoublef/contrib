package http

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adoublef/contrib/cmd/cbz/internal/encoding/html"
	"github.com/adoublef/contrib/cmd/cbz/internal/sync/pool"
	"golang.org/x/sync/errgroup"
)

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

func (h HandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h(w, r)
	if err == nil {
		return
	}

	if err, ok := errors.AsType[StatusCode](err); ok {
		http.Error(w, err.Error(), int(err))
		return
	}
	log.Printf("unexpected error occured: %v\n", err)
}

type StatusCode int

func (e StatusCode) Error() string { return http.StatusText(int(e)) }

func Handler(httpC *http.Client) http.Handler {
	s := &seriesHandler{
		N:      1, // Todo dyn
		client: httpC,
		bufs:   pool.Of[bytes.Buffer](),
	}

	return handleSeries(s)
}

type seriesHandler struct {
	N      int // concurrent image requests
	client *http.Client
	bufs   *sync.Pool // have a pool for different sizes?
}

func (h *seriesHandler) Do(ctx context.Context, series *url.URL) io.ReadCloser {
	g, ctx := errgroup.WithContext(ctx)

	pr, pw := io.Pipe()
	zips := make(chan io.Reader) // sequential
	g.Go(func() error {
		ctx, task := trace.NewTask(ctx, "series")
		defer func() { close(zips); task.End() }()

		res, err := get(ctx, h.client, series.JoinPath("full-chapter-list"))
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			return fmt.Errorf("failed series request")
		}

		zw := zip.NewWriter(pw)
		defer zw.Flush()

		var count int
		for url, err := range html.Anchors(res.Body) {
			if err != nil {
				return err
			}
			count++

			path := strings.TrimPrefix(url.Path, "/")
			first, rest, more := strings.Cut(path, "/")
			if !more || first == "" || rest == "" || strings.Contains(rest, "/") {
				return fmt.Errorf("invalid chapter url")
			}

			z := h.next(ctx, url)

			fh := &zip.FileHeader{
				Name:     strconv.Itoa(count) + ".zip",
				Method:   zip.Store,
				Modified: time.Now(),
			}
			w, err1 := zw.CreateHeader(fh)
			_, err2 := io.Copy(w, z)
			if err := cmp.Or(err1, err2); err != nil {
				return err
			}
		}
		return zw.Close()
	})

	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}

func (h *seriesHandler) next(ctx context.Context, chapter *url.URL) io.Reader {
	g, ctx := errgroup.WithContext(ctx)

	type File struct {
		*bytes.Buffer
		Name string
	}

	files := make(chan *File, h.N)
	g.Go(func() error {
		ctx, task := trace.NewTask(ctx, "chapter")
		defer func() { close(files); task.End() }()

		res, err := get(ctx, h.client, chapter.JoinPath("images"))
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if c := res.StatusCode; c != http.StatusOK {
			return fmt.Errorf("invalid chapter query")
		}

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(h.N)

		for url, err := range html.Images(res.Body) {
			if err != nil {
				return err
			}
			// check the url is valid
			name := path.Base(url.Path)
			n, err := fmt.Sscanf(name, "%d-%d.%s", new(uint), new(uint), new(string))
			if err != nil {
				return err // bad verb '%d' for string
			} else if n != 3 {
				return fmt.Errorf("unexpected scan count")
			}

			g.Go(func() error {
				ctx, task := trace.NewTask(ctx, "image")
				defer task.End()

				res, err := get(ctx, h.client, url)
				if err != nil {
					return err
				}
				defer res.Body.Close()
				if c := res.StatusCode; c != http.StatusOK {
					return fmt.Errorf("failed image query")
				}

				// contentLength needs to be set
				mbr := http.MaxBytesReader(nil, res.Body, res.ContentLength)

				f := &File{
					Name:   name,
					Buffer: h.bufs.Get().(*bytes.Buffer),
				}
				f.Reset()

				// See https://destel.dev/blog/on-the-fly-content-type-detection-in-go
				// write to the first
				if _, err = io.CopyN(f, mbr, 512); err != nil {
					return err
				}
				switch ct := http.DetectContentType(f.Bytes()); path.Dir(ct) { // get the top part
				case "image": // content-type can be spoofed
				default:
					return fmt.Errorf("unsupported image type: %q", ct)
				}
				// read the rest
				if _, err := io.Copy(f, mbr); err != nil {
					// if _, err := io.Copy(buf, image.GrayReader(mbr, image.PNG)); err != nil {
					return err
				}

				wait := trace.StartRegion(ctx, "wait")
				select {
				case <-ctx.Done():
					wait.End()
					return ctx.Err()
				case files <- f:
					wait.End()
				}
				return nil
			})
		}
		return g.Wait()
	})

	pr, pw := io.Pipe()
	g.Go(func() error {
		_, task := trace.NewTask(ctx, "zip")
		defer task.End()

		zw := zip.NewWriter(pw)
		defer zw.Close()

		for f := range files { // chapter includes the name
			fh := &zip.FileHeader{
				Name:               f.Name, // handle proper naming
				Method:             zip.Store,
				Modified:           time.Now(),
				UncompressedSize64: uint64(f.Len()),
			}
			w, err1 := zw.CreateHeader(fh)
			_, err2 := io.Copy(w, f)
			h.bufs.Put(f.Buffer)
			if err := cmp.Or(err1, err2); err != nil {
				return fmt.Errorf("failed to zip image: %v", err)
			}
		}
		return zw.Close()
	})
	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}

func handleSeries(s *seriesHandler) HandlerFunc {
	// ?series_url&format=[png|jpeg|auto]&grayscale
	parse := func(_ http.ResponseWriter, r *http.Request) (*url.URL, error) {
		parsed, err := url.Parse(r.URL.Query().Get("series_url"))
		if err != nil {
			return nil, StatusCode(http.StatusBadRequest)
		}
		// ensure path is formatted explicitly as /[series]/[id]
		path := strings.TrimPrefix(parsed.Path, "/")
		first, rest, more := strings.Cut(path, "/")
		if !more || first == "" || rest == "" || strings.Contains(rest, "/") {
			return nil, StatusCode(http.StatusUnprocessableEntity)
		}
		return parsed, nil
	}

	return func(w http.ResponseWriter, r *http.Request) error {
		ctx, task := trace.NewTask(r.Context(), "handleSeries")
		defer task.End()

		series, err := parse(w, r)
		if err != nil {
			return err
		}

		z := s.Do(ctx, series)
		defer z.Close()

		_, err = io.Copy(w, z)
		return err
	}
}

func get(ctx context.Context, c *http.Client, url fmt.Stringer, f ...func(*http.Request)) (*http.Response, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	for _, f := range f {
		f(req)
	}
	res, err2 := c.Do(req)
	return res, cmp.Or(err1, err2)
}
