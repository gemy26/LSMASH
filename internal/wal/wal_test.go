package wal

import (
	"lsmash/config"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReply(t *testing.T) {
	wal, err := CreateNewWal()
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.DeleteWAL()

	for i := 0; i < 10; i++ {
		if err := wal.Append(&WalRecord{
			Key:   int64(i),
			Value: int64(i * 10),
			OP:    OpPut,
		}); err != nil {
			t.Fatalf("Append failed at record %d: %v", i, err)
		}
	}

	if err := wal.Append(&WalRecord{Key: 10, Value: 100, OP: OpDelete}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := wal.Append(&WalRecord{Key: 11, Value: 110, OP: OpDelete}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	data, err := wal.Reply()
	if err != nil {
		t.Fatalf("Reply failed: %v", err)
	}
	if len(data) != 12 {
		t.Errorf("expected 12 records, got %d", len(data))
	}
	if data[2].Key != 2 {
		t.Errorf("expected Key=2, got %d", data[2].Key)
	}
}

func TestDeleteWAL(t *testing.T) {
	wal, err := CreateNewWal()
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	if _, err := wal.DeleteWAL(); err != nil {
		t.Fatalf("DeleteWAL failed: %v", err)
	}

	cfg := config.DefaultConfig()
	walFilename := "wal.log"
	fullPath := filepath.Join(cfg.WorkingDir, walFilename)
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Error("WAL file should not exist after deletion")
	}

	if err := wal.Append(&WalRecord{Key: 1, Value: 10, OP: OpPut}); err == nil {
		t.Error("Append should fail after WAL is deleted")
	}

	_, err = wal.Reply()
	if err == nil {
		t.Error("Reply should fail after WAL is deleted")
	}
}
