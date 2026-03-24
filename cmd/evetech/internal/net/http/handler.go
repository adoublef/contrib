package http

import (
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/trace"
	"strconv"

	"github.com/adoublef/contrib/cmd/evetech/internal/encoding/jsonl"
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
	return handleCSV(httpC)
}

func csvStream(ctx context.Context, httpC *http.Client, u *url.URL, hasHeader bool) io.ReadCloser {
	g, ctx := errgroup.WithContext(ctx)

	regions := make(chan uint64)
	g.Go(func() error {
		ctx, task := trace.NewTask(ctx, "regions")
		defer func() { close(regions); task.End() }()

		rc, err := get(ctx, httpC, "%s://%s/v1/universe/regions", u.Scheme, u.Host)
		if err != nil {
			return fmt.Errorf("failed to query regions: %w", err)
		}
		defer rc.Close()

		for id, err := range jsonl.Decode[uint64](rc) {
			if err != nil {
				return err
			}
			wait := trace.StartRegion(ctx, "wait")
			select {
			case <-ctx.Done():
				wait.End()
				return ctx.Err()
			case regions <- id:
				wait.End()
			}
		}
		return nil
	})

	type query struct {
		region, page uint64 // n > 0
	}
	queries := make(chan query)
	g.Go(func() error {
		defer func() { close(queries) }()

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(1 << 0)
		for region := range regions {
			g.Go(func() error {
				ctx, task := trace.NewTask(ctx, "pages")
				defer func() { task.End() }()

				h, err1 := head(ctx, httpC, "%s://%s/v1/markets/%d/orders", u.Scheme, u.Host, region)
				n, err2 := strconv.ParseUint(h.Get("x-pages"), 10, 64)
				if err := cmp.Or(err1, err2); err != nil {
					return fmt.Errorf("failed to fetch max-page")
				} // else if n < 1

				for i := range n {
					wait := trace.StartRegion(ctx, "wait")
					select {
					case <-ctx.Done():
						wait.End()
						return ctx.Err()
					case queries <- query{region, i + 1}:
						wait.End()
					}
				}
				return nil
			})
		}
		return g.Wait()
	})

	records := make(chan [12]string)
	g.Go(func() error {
		defer func() { close(records) }()

		// send the header if hasHeader is set
		if hasHeader {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case records <- [12]string{
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
			}:
			}
		}

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(1 << 0)
		for query := range queries {
			g.Go(func() error {
				ctx, task := trace.NewTask(ctx, "orders")
				defer func() { task.End() }()

				rs, err := get(ctx, httpC, "%s://%s/v1/markets/%d/orders?page=%d", u.Scheme, u.Host, query.region, query.page)
				if err != nil {
					return fmt.Errorf("failed to fetch orders: %w", err)
				}
				defer rs.Close()

				for o, err := range jsonl.Decode[evetech.Order](rs) {
					if err != nil {
						return err
					}
					wait := trace.StartRegion(ctx, "wait")
					select {
					case <-ctx.Done():
						wait.End()
						return ctx.Err()
					case records <- o.Record():
						wait.End()
					}
				}
				return nil
			})
		}
		return g.Wait()
	})

	pr, pw := io.Pipe() // there is no buffer here
	g.Go(func() error {
		cw := csv.NewWriter(pw) // 4*1<<10 buffer
		cw.UseCRLF = true

		for record := range records {
			// or do i create record local here
			if err := cmp.Or(cw.Write(record[:]), ctx.Err()); err != nil {
				return err
			}
		}
		cw.Flush()

		return cw.Error()
	})

	go func() { pw.CloseWithError(g.Wait()) }()
	return pr
}

func handleCSV(httpC *http.Client) HandlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (base *url.URL, hasHeader bool, err error) {
		u, err := url.Parse(r.URL.Query().Get("base_url"))
		// we need a bool but its optional
		return u, false, err
	}

	return func(w http.ResponseWriter, r *http.Request) error {
		ctx, task := trace.NewTask(r.Context(), "handleFunc")
		defer task.End()

		u, has, err := parse(w, r)
		if err != nil {
			return err
		}

		cr := csvStream(ctx, httpC, u, has)
		defer cr.Close()

		h := w.Header()
		h.Set("Content-Type", "text/csv")
		h.Set("Content-Disposition", "attachment; filename=\"evetech.csv\"")

		_, err = io.CopyBuffer(w, cr, nil)
		return err
	}
}

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

func (h HandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) { _ = h(w, r) }

type StatusCode int

func (e StatusCode) Error() string { return http.StatusText(int(e)) }

func get(ctx context.Context, httpC *http.Client, format string, v ...any) (io.ReadCloser, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(format, v...), nil)
	res, err2 := httpC.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return nil, err
	}
	if c := res.StatusCode; c != http.StatusOK {
		_ = res.Body.Close()
		return nil, StatusCode(c)
	}
	return res.Body, nil
}

func head(ctx context.Context, httpC *http.Client, format string, v ...any) (http.Header, error) {
	req, err1 := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf(format, v...), nil)
	res, err2 := httpC.Do(req)
	if err := cmp.Or(err1, err2); err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if c := res.StatusCode; c != http.StatusOK {
		return make(http.Header), StatusCode(c)
	}
	return res.Header, nil
}
