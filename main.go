package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/khalifa-is/datademon"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var dynamoClient *dynamodb.Client
var wg sync.WaitGroup

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

	year, month, _ := time.Now().Date()
	monthStr := ""
	if int(month) < 10 {
		monthStr = "0" + strconv.Itoa(int(month))
	} else {
		monthStr = strconv.Itoa(int(month))
	}

	baseDate := "-" + strconv.Itoa(year) + "-" + monthStr + "-01-"
	baseChLink := configJson["baseChLink"].(string)
	for i := 1; i < 7; i++ {
		partName := "part" + strconv.Itoa(i) + "_6.zip"
		chDataLink := baseChLink + baseDate + partName
		zipFilePath := filepath.Join("data", "company-data"+partName)

		wg.Add(1)
		go processChData(chDataLink, zipFilePath)
	}
	wg.Wait()

	cleanupDataDirectory()
}

func processChData(link string, zip string) {
	defer wg.Done()
	fmt.Println("Started processing: ", link)
	_, err := datademon.DownloadZipFile(link, zip)
	if err != nil {
		log.Fatal(err)
	}

	files, err := datademon.Unzip(zip, "data")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Started reading: ", files[0])
	records, err := datademon.ReadCsvFile(files[0])
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Number of records: ", len(records))

	errors := datademon.ParseCsv(records, true, parseCsvCallback)
	if len(errors) > 0 {
		fmt.Println("Number of errors: ", len(errors))
		fmt.Println(errors)
	}
}
