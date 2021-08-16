package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func unzip(src string, dest string) ([]string, error) {
	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {
		fPath := filepath.Join(dest, f.Name)

		if !strings.HasPrefix(fPath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fPath)
		}

		filenames = append(filenames, fPath)

		if f.FileInfo().IsDir() {
			os.MkdirAll(fPath, os.ModePerm)
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fPath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}

func readCsvFile(file string) [][]string {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal("Unable to read input file "+file, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+file, err)
	}

	return records
}

func main() {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Opened JSON file")
	defer jsonFile.Close() // So we can close it later

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config map[string]interface{}
	json.Unmarshal([]byte(byteValue), &config)

	var chDataLink = config["chDataLink"].(string)

	resp, err := http.Get(chDataLink)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	fmt.Println("status", resp.Status)
	if resp.StatusCode != 200 {
		return
	}

	out, err := os.Create(filepath.Join("data", "company-data.zip"))
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	files, err := unzip(filepath.Join("data", "company-data.zip"), "data")
	if err != nil {
		log.Fatal(err)
	}

	records := readCsvFile(files[0])
	fmt.Println(records[0])

	dir, err := ioutil.ReadDir("data")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"data", d.Name()}...))
	}
}
