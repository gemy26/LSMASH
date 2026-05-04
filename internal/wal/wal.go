package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"lsmash/config"
	"os"
	"path/filepath"
)

type OpType bool

const (
	OpPut    OpType = true
	OpDelete OpType = false
)

type Wal struct {
	ReaderWriter ReaderWriter
}
type WalRecord struct {
	Key   int64
	Value int64
	OP    OpType //PUT = 1, Delete = 0
}

type ReaderWriter interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Sync() error
}

// CRC 32
// KEY 64
// VALUE 64
// OP 1
func (w *Wal) Append(record *WalRecord) error {
	if err := binary.Write(w.ReaderWriter, binary.LittleEndian, record.Key); err != nil {
		return err
	}
	if err := binary.Write(w.ReaderWriter, binary.LittleEndian, record.Value); err != nil {
		return err
	}
	if err := binary.Write(w.ReaderWriter, binary.LittleEndian, record.OP); err != nil {
		return err
	}
	CRC, err := calculateCRC(record)
	if err != nil {
		return err
	}
	if err := binary.Write(w.ReaderWriter, binary.LittleEndian, CRC); err != nil {
		return err
	}
	return w.ReaderWriter.Sync()
}
func Reply() ([]WalRecord, error) {
	walFilename := "wal.log"
	cfg := config.DefaultConfig()
	fullPath := filepath.Join(cfg.WorkingDir, walFilename)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	var records []WalRecord
	for {
		var rec WalRecord
		if err := binary.Read(file, binary.LittleEndian, &rec.Key); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &rec.Value); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &rec.OP); err != nil {
			return nil, err
		}
		var storedCRC uint32
		if err := binary.Read(file, binary.LittleEndian, &storedCRC); err != nil {
			return nil, err
		}
		recCRC, err := calculateCRC(&rec)
		if err != nil {
			return nil, fmt.Errorf("failed CRC calc: %w", err)
		}
		if storedCRC != recCRC {
			return nil, fmt.Errorf("CRC mismatch: WAL corrupted")
		}
		records = append(records, rec)
	}
	return records, nil
}

func calculateCRC(record *WalRecord) (uint32, error) {
	crc32q := crc32.MakeTable(0xD5828281)
	buf := bytes.Buffer{}

	if err := binary.Write(&buf, binary.LittleEndian, record.Key); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, record.Value); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, record.OP); err != nil {
		return 0, err
	}
	data := buf.Bytes()
	return crc32.Checksum(data, crc32q), nil
}

func CreateNewWal() (*Wal, error) {
	walFilename := "wal.log"
	cfg := config.DefaultConfig()
	fullPath := filepath.Join(cfg.WorkingDir, walFilename)
	file, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	log.Printf("new wal.log file created, location: %s", fullPath)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}

	return &Wal{
		ReaderWriter: file,
	}, nil
}

func (w *Wal) DeleteWAL() (bool, error) {
	if err := w.ReaderWriter.Close(); err != nil {
		return false, err
	}
	filename := "wal.log"
	cfg := config.DefaultConfig()
	fullPath := filepath.Join(cfg.WorkingDir, filename)
	err := os.Remove(fullPath)
	if err != nil {
		return false, err
	}
	return true, err
}
