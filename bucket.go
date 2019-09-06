package s3

import (
	"bytes"
	"io"
	"io/ioutil"

	session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/btubbs/envcfg"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"
)

type BucketOptions struct {
	Endpoint string
	UseSSL   bool
}

func defaultBucketOptions() BucketOptions {
	return BucketOptions{
		Endpoint: "s3.amazonaws.com",
		UseSSL:   true,
	}
}

// Endpoint set the endpoint option on the S3 connection.
func Endpoint(endpoint string) func(*BucketOptions) {
	return func(opts *BucketOptions) {
		opts.Endpoint = endpoint
	}
}

// UseSSL sets the SSL option on the S3 connection.
func UseSSL(useSSL bool) func(*BucketOptions) {
	return func(opts *BucketOptions) {
		opts.UseSSL = useSSL
	}
}

// NewBucket returns a new Bucket instance.  (It doesn't actually create a new bucket in S3.)
func NewBucket(accessKeyID, secretAccessKey, region, bucketName string, optionFuncs ...func(*BucketOptions)) (*Bucket, error) {
	options := defaultBucketOptions()
	for _, o := range optionFuncs {
		o(&options)
	}

	// configure S3 client
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		Endpoint:    aws.String(options.Endpoint),
		Region:      aws.String(region),
		DisableSSL:  aws.Bool(!options.UseSSL),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)

	return &Bucket{
		client:     s3Client,
		bucketName: bucketName,
	}, nil
}

// Bucket implements methods for getting and putting s3 objects.
type Bucket struct {
	client     *awsS3.S3
	bucketName string
}

// An Object represents a file on s3.  It's not safe to read one from multiple goroutines.
type Object struct {
	io.ReadCloser
	ContentType   string
	ContentLength int
}

// GetObject gets an object from S3.
func (b *Bucket) GetObject(objectName string) (*Object, error) {
	obj, err := b.client.GetObject(&awsS3.GetObjectInput{
		Bucket: &b.bucketName,
		Key:    &objectName,
	})
	if err != nil {
		return nil, err
	}

	return &Object{
		ReadCloser:    obj.Body,
		ContentType:   *obj.ContentType,
		ContentLength: int(*obj.ContentLength),
	}, nil
}

// PutObject puts an object into S3.  Because the upstream s3 library wants a ReadSeeker, but we
// only require callers to provide the more flexible Reader, this function reads the whole body into
// memory to convert between the two.  So don't use this for huge bodies (hundreds of MBs for
// example).
func (b *Bucket) PutObject(objectName string, contentType string, body io.Reader) error {
	//  Convert the Reader we were given to a ReadSeeker like the upstream library wants.
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	readSeekerBody := bytes.NewReader(bodyBytes)
	_, err = b.client.PutObject(&s3.PutObjectInput{
		Body:   readSeekerBody,
		Bucket: &b.bucketName,
		Key:    &objectName,
	})
	return err
}

func init() {
	// Auto-load the env var parser for envcfg.
	if err := envcfg.RegisterParser(func(accessKeyID, secretAccessKey, region, bucketName string) (*Bucket, error) {
		return NewBucket(accessKeyID, secretAccessKey, region, bucketName)
	}); err != nil {
		panic(err)
	}
}
