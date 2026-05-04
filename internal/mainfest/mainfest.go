package mainfest

import (
	"bufio"
	"encoding/json"
	"log"
	"lsmash/config"
	"os"
)

type Mainfest struct {
	filePath string
	file     *os.File
}

type MainfestRecord struct {
	Type    string   `json:"type"`
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Level   int64    `json:"level"`
}

func (m *Mainfest) Add(record MainfestRecord) {
	b, err := json.Marshal(record)
	if err != nil {
		log.Fatal("Error marshaling record:", err)
	}
	b = append(b, '\n')
	if _, err := m.file.Write(b); err != nil {
		log.Fatal("Error writing to Mainfest file:", err)
	}
	err = m.file.Sync()
	if err != nil {
		return
	}
}

func (m *Mainfest) Reply() []MainfestRecord {
	m.file.Seek(0, 0)
	var records []MainfestRecord
	scanner := bufio.NewScanner(m.file)
	for scanner.Scan() {
		var record MainfestRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			log.Println("Error decoding record:", err)
			continue
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return records
}

func NewMainfest() (*Mainfest, error) {
	cfg := config.DefaultConfig()
	fullPath := cfg.WorkingDir + "/mainfest.json"

	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	mainfest := &Mainfest{
		filePath: fullPath,
		file:     file,
	}
	return mainfest, nil
}
