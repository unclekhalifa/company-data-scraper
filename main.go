package main

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/khalifa-is/datademon"
	"log"
	"path/filepath"
	"time"
)

var dynamoClient *dynamodb.Client

func main() {
	defer timeFxn(time.Now(), "main fxn")

	cfg, err := getAwsConfig()
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	dynamoClient = dynamodb.NewFromConfig(cfg)

	configJson, err := getJsonConfig()
	if err != nil {
		log.Fatal(err)
	}

	// -2021-08-01-part1_6.zip

	chDataLink := configJson["chDataLink"].(string)
	zipFilePath := filepath.Join("data", configJson["zipFileName"].(string))

	_, err = datademon.DownloadZipFile(chDataLink, zipFilePath)
	if err != nil {
		log.Fatal(err)
	}

	files, err := datademon.Unzip(zipFilePath, "data")
	if err != nil {
		log.Fatal(err)
	}

	records, err := datademon.ReadCsvFile(files[0])
	if err != nil {
		log.Fatal(err)
	}

	datademon.ParseCsv(records, true, parseCsvCallback)

	cleanupDataDirectory()
}
