package http

import (
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/trace"
	"strconv"

	"github.com/adoublef/contrib/cmd/evetech/internal/encoding/json/jsonstream"
	"github.com/adoublef/contrib/cmd/evetech/internal/evetech"
	"golang.org/x/sync/errgroup"
)

type (
	Server = http.Server
	Client = http.Client
)

func IsServerClosed(err error) bool {
	return errors.Is(err, http.ErrServerClosed)
}

func Handler(httpC *http.Client) http.Handler {
	return handleFunc(httpC)
}

func handleFunc(httpC *http.Client) handlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (base *url.URL, pages, orders int, err error) {
		u, err := url.Parse(r.URL.Query().Get("base_url"))
		if err != nil {
			return nil, 0, 0, httpError(http.StatusBadRequest)
		} else if u.Path != "" || u.String() == "" { // cannot be empty
			return nil, 0, 0, httpError(http.StatusUnprocessableEntity)
		}
		// pages (optional)
		// parse & with best-effort use the value
		// dont fully care for the error
		pages, _ = strconv.Atoi(r.URL.Query().Get("par_regions"))
		orders, _ = strconv.Atoi(r.URL.Query().Get("par_pages"))
		return u, max(min(pages, 100), 1), max(min(orders, 100), 1), err
	}

	csvReader := func(ctx context.Context, base *url.URL, parRegions, parPages int) io.ReadCloser {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(5)

		regions := make(chan int)
		g.Go(func() error {
			ctx, task := trace.NewTask(ctx, "region")
			defer task.End()

			defer close(regions)

			base := base.JoinPath("v1", "universe", "regions")
			req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
			// NOTE seem to not be allowed to set a region
			// around a http request. this may be conflicting with
			// internal package calls? chek issues
			// call := trace.StartRegion(ctx, "httpRequest")
			res, err2 := httpC.Do(req)
			if err := cmp.Or(err1, err2); err != nil {
				// call.End()
				return err
			}
			// call.End()
			defer res.Body.Close()

			if c := res.StatusCode; c != http.StatusOK {
				return fmt.Errorf("failed query returned %d status code", c)
			}

			defer trace.StartRegion(ctx, "decode").End()
			for id, err := range jsonstream.Decode[int](io.LimitReader(res.Body, res.ContentLength)) {
				if err != nil {
					return err
				}
				wait := trace.StartRegion(ctx, "wait")
				select {
				case <-ctx.Done():
					wait.End()
					return ctx.Err()
				case regions <- id:
				}
				wait.End()
			}

			return nil
		})

		type page struct{ region, n int }
		pages := make(chan page)
		g.Go(func() error {
			defer close(pages)

			g, ctx := errgroup.WithContext(ctx)
			g.SetLimit(parRegions)
			for region := range regions {
				g.Go(func() error {
					ctx, task := trace.NewTask(ctx, "pages")
					defer task.End()

					// net call
					base := base.JoinPath("v1", "markets", strconv.Itoa(region), "orders")
					req, err1 := http.NewRequestWithContext(ctx, http.MethodHead, base.String(), nil)
					// call := trace.StartRegion(ctx, "syscall")
					res, err2 := httpC.Do(req)
					if err := cmp.Or(err1, err2); err != nil {
						// call.End()
						return err
					}
					// call.End()
					defer res.Body.Close()

					if c := res.StatusCode; c != http.StatusOK {
						return fmt.Errorf("failed query returned %d status code", c)
					}

					max, err := strconv.Atoi(res.Header.Get("x-pages"))
					if err != nil {
						return err
					}

					for n := 1; n <= max; n++ {
						wait := trace.StartRegion(ctx, "wait")
						select {
						case <-ctx.Done():
							wait.End()
							return ctx.Err()
						case pages <- page{region, n}:
						}
						wait.End()
					}
					return nil
				})
			}
			return g.Wait()
		})

		records := make(chan []string)
		g.Go(func() error {
			defer close(records)

			g, ctx := errgroup.WithContext(ctx)
			g.SetLimit(parPages)
			for page := range pages {
				g.Go(func() error {
					ctx, task := trace.NewTask(ctx, "orders")
					defer task.End()

					base := base.JoinPath("v1", "markets", strconv.Itoa(page.region), "orders")
					v := make(url.Values)
					v.Set("page", strconv.Itoa(page.n))
					base.RawQuery = v.Encode()

					req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
					// call := trace.StartRegion(ctx, "syscall")
					res, err2 := httpC.Do(req)
					if err := cmp.Or(err1, err2); err != nil {
						// call.End()
						return err
					}
					// call.End()
					defer res.Body.Close()
					if c := res.StatusCode; c != http.StatusOK {
						return fmt.Errorf("failed query returned %d status code", c)
					}

					defer trace.StartRegion(ctx, "decode").End()
					for o, err := range jsonstream.Decode[evetech.Order](io.LimitReader(res.Body, res.ContentLength)) {
						if err != nil {
							return err
						}
						wait := trace.StartRegion(ctx, "wait")
						select {
						case <-ctx.Done():
							wait.End()
							return ctx.Err()
						case records <- o.Record():
						}
						wait.End()
					}
					return nil
				})
			}
			return g.Wait()
		})

		pr, pw := io.Pipe()
		g.Go(func() error {
			ctx, task := trace.NewTask(ctx, "pipe")
			defer task.End()

			cw := csv.NewWriter(&ctxWriter{ctx: ctx, Writer: pw}) // need to wrap the wrtier
			cw.UseCRLF = true

			// write the header
			if err := cw.Write([]string{
				"duration",
				"is_buy_order",
				"issued",
				"location_id",
				"min_volume",
				"order_id",
				"price",
				"range",
				"system_id",
				"type_id",
				"volume_remain",
				"volume_total",
			}); err != nil {
				return err
			}

			for record := range records {
				if err := cw.Write(record); err != nil {
					return err
				}
			}
			cw.Flush()
			if err := cw.Error(); err != nil {
				return err
			}
			return nil
		})

		go func() { pw.CloseWithError(g.Wait()) }()
		return pr
	}
	return func(w http.ResponseWriter, r *http.Request) error {
		ctx, task := trace.NewTask(r.Context(), "handleFunc")
		defer task.End()

		base, pages, orders, err := parse(w, r)
		if err != nil {
			return err
		}

		cr := csvReader(ctx, base, pages, orders)
		defer cr.Close()

		_, err = io.Copy(w, cr)
		return err
	}
}

type handlerFunc func(w http.ResponseWriter, r *http.Request) error

func (h handlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h(w, r)
	if err == nil {
		return
	}

	if err, ok := errors.AsType[httpError](err); ok {
		http.Error(w, err.Error(), int(err))
		return
	}
	log.Printf("unexpected error: %v\n", err)
}

type httpError int

func (e httpError) Error() string { return http.StatusText(int(e)) }

type ctxWriter struct {
	ctx context.Context
	io.Writer
}

func (w *ctxWriter) Write(p []byte) (int, error) {
	// trace per call or just make a region
	n, err := w.Writer.Write(p)
	return n, cmp.Or(err, w.ctx.Err())
}
