package zip_test

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"strconv"
	"testing"
)

func Benchmark_archive_0158_006_Store(b *testing.B) {
	benchmark_archive(b, "testdata/0158-006.png", zip.Store)
}

func Benchmark_archive_0158_006_Deflate(b *testing.B) {
	benchmark_archive(b, "testdata/0158-006.png", zip.Deflate)
}

func Benchmark_archive_0158_007_Store(b *testing.B) {
	benchmark_archive(b, "testdata/0158-007.png", zip.Store)
}

func Benchmark_archive_0158_007_Deflate(b *testing.B) {
	benchmark_archive(b, "testdata/0158-007.png", zip.Deflate)
}

func Benchmark_archive_image_Store(b *testing.B) {
	benchmark_archive(b, "testdata/image.jpg", zip.Store)
}

func Benchmark_archive_image_Deflate(b *testing.B) {
	benchmark_archive(b, "testdata/image.jpg", zip.Deflate)
}

func benchmark_archive(b *testing.B, name string, method uint16) {
	f, err := os.Open(name)
	if err != nil {
		b.Fail()
	}
	b.Cleanup(func() { f.Close() })

	p, err := io.ReadAll(f)
	if err != nil {
		b.Fail()
	}

	var n int
	for b.Loop() {
		n++
		zw := zip.NewWriter(io.Discard)
		fh := &zip.FileHeader{Name: strconv.Itoa(n), Method: method}
		w, err1 := zw.CreateHeader(fh)
		_, err2 := io.Copy(w, bytes.NewReader(p))
		err3 := zw.Close()
		if err1 != nil || err2 != nil || err3 != nil {
			b.Fail()
		}
		b.ReportMetric(float64(fh.CompressedSize64)/float64(fh.UncompressedSize64), "ratio/op")
	}
}
