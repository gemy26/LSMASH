package mainfest

import (
	"os"
	"testing"

	"lsmash/config"
)

func cleanup(t *testing.T) {
	cfg := config.DefaultConfig()
	os.Remove(cfg.WorkingDir + "/mainfest.json")
}

func TestManifest_AddAndReplay(t *testing.T) {
	cleanup(t)
	t.Cleanup(func() { cleanup(t) })

	m, err := NewMainfest()
	if err != nil {
		t.Fatal(err)
	}

	m.Add(MainfestRecord{Level: 0, Type: "Flush", Added: []string{"l0_0.lsm"}, Removed: nil})
	m.Add(MainfestRecord{Level: 1, Type: "Compaction", Added: []string{"l1_0.lsm"}, Removed: []string{"l0_0.lsm"}})

	rcs := m.Reply()
	if len(rcs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(rcs))
	}
	if rcs[0].Type != "Flush" || rcs[0].Added[0] != "l0_0.lsm" {
		t.Errorf("record 0 mismatch: %+v", rcs[0])
	}
	if rcs[1].Type != "Compaction" || rcs[1].Removed[0] != "l0_0.lsm" {
		t.Errorf("record 1 mismatch: %+v", rcs[1])
	}
}

func TestManifest_PersistsAcrossReopens(t *testing.T) {
	cleanup(t)
	t.Cleanup(func() { cleanup(t) })

	m1, _ := NewMainfest()
	m1.Add(MainfestRecord{Level: 0, Type: "Flush", Added: []string{"l0_0.lsm"}, Removed: nil})
	m1.file.Close()

	m2, err := NewMainfest()
	if err != nil {
		t.Fatal(err)
	}
	rcs := m2.Reply()
	if len(rcs) != 1 || rcs[0].Added[0] != "l0_0.lsm" {
		t.Errorf("expected l0_0.lsm after reopen, got %+v", rcs)
	}
}
