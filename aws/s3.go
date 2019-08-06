package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pipedrive/uncouch/config"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

func S3FileWriter(file io.Reader, filename string) (error) {

	//file := strings.NewReader(str.String())
	_, keyName := filepath.Split(filename)

	bucketName := config.S3_BUCKET

	upParams := &s3manager.UploadInput{
		Bucket: &bucketName,
		Key:    &keyName,
		Body:   file,
		ServerSideEncryption: aws.String(config.S3_SERVER_ENCRYPTION),
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = config.S3_PART_SIZE // 5MB per part
		u.LeavePartsOnError = config.S3_LEAVE_PARTS_ON_ERROR // delete the parts if the upload fails.
	})

	// Perform an upload.
	result, err := uploader.Upload(upParams)
	if err != nil {
		slog.Error(err)
		return err
	}

	fmt.Println("File location: " + result.Location)

	return nil
}

func S3FileReader(filename string) ([]byte, int64, error) {
	// The S3 client the S3 Downloader will use
	s3Svc := s3.New(sess)

	buf := &aws.WriteAtBuffer{}

	u, err := url.Parse(filename)
	if err != nil {
		slog.Error(err)
		return nil, 0, err
	}


	// Create a downloader with the s3 client and custom options
	downloader := s3manager.NewDownloaderWithClient(s3Svc, func(d *s3manager.Downloader) {
		d.PartSize = 5 * 1024 * 1024 // 5MB per part
	})

	n, err := downloader.Download(buf, &s3.GetObjectInput {
		Bucket: aws.String(u.Host),
		Key: aws.String(u.Path),
	})
	if err != nil {
		slog.Error(err)
		return nil, 0, err
	}

	log.Info("Downloaded from S3: " + strconv.FormatInt(n, 10) + " bytes.")

	return buf.Bytes(), n, nil
}

func S3FileDownloader(filename string) (string, error) {

	// The S3 client the S3 Downloader will use
	s3Svc := s3.New(sess)

	u, err := url.Parse(filename)
	if err != nil {
		slog.Error(err)
		return "", err
	}

	_, fname := filepath.Split(filename)

	item := filepath.Join(config.TEMP_INPUT_DIR, fname)

	file, err := os.Create(item)
	if err != nil {
		return "", err
	}

	defer file.Close()

	// Create a downloader with the s3 client and custom options
	downloader := s3manager.NewDownloaderWithClient(s3Svc, func(d *s3manager.Downloader) {
		d.PartSize = 5 * 1024 * 1024 // 5MB per part
	})

	n, err := downloader.Download(file, &s3.GetObjectInput {
		Bucket: aws.String(u.Host),
		Key: aws.String(u.Path),
	})
	if err != nil {
		slog.Error(err)
		return "", err
	}

	log.Info("Downloaded from S3: " + strconv.FormatInt(n, 10) + " bytes.")

	return item, nil
}

func S3FileUploader(filename, destination string) (error) {
	// Hello, there!

	return nil
}