package handlers

import (
	"compress/gzip"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"io"
	"net/http"
)

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
		gz.logger.Infof("Exceeded gzip decompression limit (%s) - aborting", gz.maxSize)
		gz.response.WriteHeader(http.StatusRequestEntityTooLarge)
		return 0, fmt.Errorf("Maximum upload size encounted. Treating as a potential zip bomb.")
	}

	return
}

func (gz *gzipReader) Close() error {
	return gz.body.Close()
}

func NewDecompressingMiddleware(maxSize int) martini.Handler {
	return func(res http.ResponseWriter, req *http.Request, c martini.Context, logger *logrus.Logger) {
		contentEncoding := req.Header["Content-Encoding"]
		for _, v := range contentEncoding {
			if v == "gzip" {
				logger.Debugln("Decompressing request")
				req.Body = &gzipReader{body: req.Body, maxSize: maxSize, response: res, logger: logger}
			}
		}

		c.Next()
	}
}
