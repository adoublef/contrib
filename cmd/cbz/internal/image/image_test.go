package image_test

import (
	"io"
	"os"
	"testing"

	. "github.com/adoublef/contrib/cmd/cbz/internal/image"
)

// https://golangprojectstructure.com/converting-colour-images-to-greyscale/
// https://rentafounder.com/convert-image-to-grayscale-golang/
func TestGray(t *testing.T) {
	f, err := os.Open("testdata/image.jpg") // not much colour
	ok(t, err)
	t.Cleanup(func() { f.Close() })
	df, err := os.Create("testdata/gray.jpg")
	ok(t, err)
	t.Cleanup(func() { df.Close() })

	gr := GrayReader(f, PNG)
	// write to a dest
	_, err = io.Copy(df, gr)
	ok(t, err)
}

func ok(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
