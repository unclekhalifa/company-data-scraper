package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/khalifa-is/datademon"
)

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

	files, err := datademon.Unzip(filepath.Join("data", "company-data.zip"), "data")
	if err != nil {
		log.Fatal(err)
	}

	records := datademon.ReadCsvFile(files[0])
	fmt.Println(records[0])

	dir, err := ioutil.ReadDir("data")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"data", d.Name()}...))
	}
}
