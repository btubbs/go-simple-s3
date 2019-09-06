package s3

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPutGetObject(t *testing.T) {
	server := newFakeS3()
	defer server.Close()

	bucket, err := NewBucket("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "us-east-1", "testbucket", UseSSL(false), Endpoint(server.URL))
	assert.Nil(t, err)

	// first try to get it.  should fail
	obj, err := bucket.GetObject("blah")
	assert.Nil(t, obj)
	assert.NotNil(t, err)

	// then write the object.  should succeed
	err = bucket.PutObject("blah", "text/plain", bytes.NewReader([]byte("test contents here!")))
	assert.Nil(t, err)

	// now try getting it again.  Should succeed
	obj, err = bucket.GetObject("blah")
	assert.Nil(t, err)
	contents, err := ioutil.ReadAll(obj)
	assert.Nil(t, err)
	assert.Equal(t, []byte("test contents here!"), contents)
}

func newFakeS3() *httptest.Server {
	server := httptest.NewServer(&fakeS3{
		keys: map[string][]byte{},
	})
	parsedURL, err := url.Parse(server.URL)
	if err != nil {
		panic(nil)
	}
	server.URL = "localhost:" + parsedURL.Port()
	return server
}

type fakeS3 struct {
	keys map[string][]byte
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if bytes, ok := f.keys[r.URL.Path]; ok {
			w.Header().Add("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
			w.Header().Add("x-amz-request-id", "0A49CE4060975EAC")
			w.Header().Add("Date", "Wed, 12 Oct 2009 17:50:00 GMT")
			w.Header().Add("ETag", "1b2cf535f27731c974343645a3985328")
			w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))
			w.Header().Add("Content-Type", "text/plain")
			w.Header().Add("Connection", "close")
			w.Header().Add("Server", "AmazonS3")
			w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("NoSuchKey"))
		}
	} else if r.Method == http.MethodPut {
		bytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		f.keys[r.URL.Path] = bytes
		w.Header().Add("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
		w.Header().Add("x-amz-request-id", "0A49CE4060975EAC")
		w.Header().Add("Date", "Wed, 12 Oct 2009 17:50:00 GMT")
		w.Header().Add("ETag", "1b2cf535f27731c974343645a3985328")
		w.Header().Add("Content-Length", "0")
		w.Header().Add("Connection", "close")
		w.Header().Add("Server", "AmazonS3")

	}
}
