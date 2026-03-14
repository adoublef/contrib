// 530
package image

import (
	"image"
	"image/draw"
	"image/jpeg"
	_ "image/jpeg"
	"image/png"
	"io"
)

const (
	JPEG = 0
	PNG  = 1
)

// GrayReader creates a new grayscale image as a [io.Reader]
func GrayReader(r io.Reader, typ int) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()

		img, _, err := image.Decode(r)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		// See https://rentafounder.com/convert-image-to-grayscale-golang/
		// need a pool for the creation of a new image
		// default: should not be allowed
		gray := image.NewGray(img.Bounds())
		draw.Draw(gray, gray.Bounds(), img, img.Bounds().Min, draw.Src)
		switch typ {
		case JPEG:
			err = jpeg.Encode(pw, gray, nil)
		case PNG:
			err = png.Encode(pw, gray)
		}
		if err != nil {
			pw.CloseWithError(err)
			return
		}
	}() // 2kb overhead
	return pr
}
