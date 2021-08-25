package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/khalifa-is/datademon"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"
)

func getEnvVar(key string) string {
	viper.SetConfigFile(".env")

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Error while reading config file %s", err)

	}

	value, ok := viper.Get(key).(string)

	if !ok {
		log.Fatal("Invalid type assertion")
	}

	return value
}

func getAwsConfig() (cfg aws.Config, err error) {
	ctx := context.TODO()
	if os.Args[len(os.Args)-1] == "-d" {
		return config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(getEnvVar("AWS_PROFILE")))
	}
	return config.LoadDefaultConfig(ctx)
}

func buildCompany(record []string) Company {
	incDateTimestamp, err := time.Parse("02/01/2006", record[14])
	if err != nil {
		incDateTimestamp = time.Now() // Default to today's date
	}
	company := Company{
		CompanyName:                record[0],
		CompanyNumber:              record[1],
		CareOf:                     record[2],
		POBox:                      record[3],
		AddressLine1:               record[4],
		AddressLine2:               record[5],
		PostTown:                   record[6],
		County:                     record[7],
		Country:                    record[8],
		PostCode:                   record[9],
		CompanyCategory:            record[10],
		CompanyStatus:              record[11],
		CountryOfOrigin:            record[12],
		DissolutionDate:            record[13],
		IncorporationDate:          record[14],
		IncorporationDateTimestamp: int(incDateTimestamp.Unix()),
		AccountingRefDay:           record[15],
		AccountingRefMonth:         record[16],
		NextDueDate:                record[17],
		LastMadeUpDate:             record[18],
		AccountsCategory:           record[19],
		ReturnsNextDueDate:         record[20],
		ReturnsLastMadeUpDate:      record[21],
		NumMortCharges:             record[22],
		NumMortOutstanding:         record[23],
		NumMortPartSatisfied:       record[24],
		NumMortSatisfied:           record[25],
		SICCode1:                   record[26],
		SICCode2:                   record[27],
		SICCode3:                   record[28],
		SICCode4:                   record[29],
		NumGenPartners:             record[30],
		NumLimPartners:             record[31],
		URI:                        record[32],
		ChangeOfNameDate:           record[33],
		CompanyNamePrevious:        record[34],
	}
	return company
}

func cleanupDataDirectory() {
	dir, err := ioutil.ReadDir("data")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range dir {
		err := os.RemoveAll(path.Join([]string{"data", d.Name()}...))
		if err != nil {
			log.Fatalf("Clean up failed: %v", err)
		}
	}
}

func getJsonConfig() (map[string]interface{}, error) {
	jsonFile, err := os.Open(getEnvVar("CONFIG_JSON"))
	if err != nil {
		return nil, err
	}

	defer func(jsonFile *os.File) {
		err = jsonFile.Close()
	}(jsonFile)

	if err != nil {
		return nil, err
	}

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var configJson map[string]interface{}
	err = json.Unmarshal(byteValue, &configJson)

	if err != nil {
		return nil, err
	}

	return configJson, nil
}

func downloadZipFile(url string, filePath string) (bool, error) {
	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
	}(resp.Body)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		return false, fmt.Errorf("could not download file: %x", resp.StatusCode)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return false, err
	}

	defer func(out *os.File) {
		err = out.Close()
	}(out)
	if err != nil {
		return false, err
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return false, err
	}

	return true, nil
}

func parseCsv(csv string, svc *dynamodb.Client) (errors []map[string]interface{}) {
	records := datademon.ReadCsvFile(csv)
	for i, record := range records {
		if record[0] == "CompanyName" {
			continue
		}

		company := buildCompany(record)

		av, err := attributevalue.MarshalMap(company)
		if err != nil {
			errors = append(errors, map[string]interface{}{
				"error":   err,
				"company": company,
			})
		}

		companyInput := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("Companies"),
		}

		_, err = svc.PutItem(context.TODO(), companyInput)
		if err != nil {
			errors = append(errors, map[string]interface{}{
				"error":   err,
				"company": company,
			})
		}
		if i == 5 {
			break
		}
	}
	return errors
}

func timeFxn(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func main() {
	defer timeFxn(time.Now(), "main fxn")

	cfg, err := getAwsConfig()
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	dynamoClient := dynamodb.NewFromConfig(cfg)

	configJson, err := getJsonConfig()
	if err != nil {
		log.Fatal(err)
	}

	// -2021-08-01-part1_6.zip

	chDataLink := configJson["chDataLink"].(string)
	zipFilePath := filepath.Join("data", configJson["zipFileName"].(string))

	_, err = downloadZipFile(chDataLink, zipFilePath)
	if err != nil {
		log.Fatal(err)
	}

	files, err := datademon.Unzip(zipFilePath, "data")
	if err != nil {
		log.Fatal(err)
	}

	parseCsv(files[0], dynamoClient)

	cleanupDataDirectory()
}
