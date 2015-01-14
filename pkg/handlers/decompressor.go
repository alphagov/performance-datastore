package handlers

import (
	"compress/gzip"
	"fmt"
	"github.com/Sirupsen/logrus"
	"io"
	"net/http"
)

type gzipBombError struct{}

func (e gzipBombError) Error() string {
	return fmt.Sprintf("Maximum upload size encountered. Treating as a potential zip bomb.")
}

type gzipReader struct {
	body      io.ReadCloser       // underlying Request.Body
	zr        io.Reader           // lazily-initialized gzip reader
	maxSize   int                 // maximum size gzip request that we'll handle. Used to defend against zip bombs
	readBytes int                 // the number bytes that have been read by this gzipReader
	response  http.ResponseWriter // the context http.ResponseWriter
	logger    *logrus.Logger
}

func (gz *gzipReader) Read(p []byte) (n int, err error) {
	if gz.zr == nil {
		gz.zr, err = gzip.NewReader(gz.body)
		if err != nil {
			return 0, err
		}
	}

	n, err = gz.zr.Read(p)

	gz.readBytes += n

	if gz.readBytes > gz.maxSize {
		gz.logger.Infof("Exceeded gzip decompression limit (%v) - aborting", gz.maxSize)
		return n, &gzipBombError{}
	}

	return
}

func (gz *gzipReader) Close() error {
	return gz.body.Close()
}

// NewDecompressingHandler returns a http.Handler middleware which can decompress request bodies on the fly.
func NewDecompressingHandler(h http.Handler, maxSize int) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		contentEncoding := req.Header["Content-Encoding"]
		for _, v := range contentEncoding {
			if v == "gzip" {
				logger := getLogger(req)
				logger.Debugln("Decompressing request")
				req.Body = &gzipReader{body: req.Body, maxSize: maxSize, response: res, logger: logger}
			}
		}

		h.ServeHTTP(res, req)
	})
}
