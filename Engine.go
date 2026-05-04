package lsmash

import (
	"log"
	config "lsmash/config"
	"lsmash/internal/mainfest"
	"lsmash/internal/memtable"
	"lsmash/internal/sstable"
	"lsmash/internal/wal"
)

type Engine struct {
	memtable  *memTable.MemTable
	immutable []*memTable.MemTable
	sstable   [][]*sstable.SSTable
	config    config.Config
	wal       *wal.Wal
	mainfest  *mainfest.Mainfest
}

func (e *Engine) insertIntoMemtable(key int64, value int64) error {
	log.Printf("Insert called: key=%d value=%d", key, value)
	log.Printf("memtable current size: %d", e.memtable.Size)
	if e.memtable.Size+1 > e.config.MemTableSizeLimit {
		log.Printf("Memtable limit exceeded, current size: %d, config size: %d", e.memtable.Size, e.config.MemTableSizeLimit)
		e.immutable = append([]*memTable.MemTable{e.memtable}, e.immutable...)
		log.Printf("push memtable to immutabe, current size: %d", len(e.immutable))
		e.memtable = memTable.NewMemTable()
		//Triger Flush
		if len(e.immutable) == 5 {
			//TODO: Change that static number to config value
			log.Println("Flushing immutable memtables to SSTables")
			for i, v := range e.immutable {
				log.Printf("Flushing memtable #%d", i)
				table, err := sstable.FlushToSSTable(v)
				//TODO: Add Mainfest Records for Flush
				if err != nil {
					log.Printf("Flush failed: %v", err)
					return err
				}
				e.sstable[0] = append([]*sstable.SSTable{table}, e.sstable[0]...)
			}
			_, err := e.wal.DeleteWAL()
			if err != nil {
				return err
			}
			e.immutable = make([]*memTable.MemTable, 0)
		}
	}
	//Triger Compaction
	e.forceCompaction()

	e.memtable.SkipList.Insert(key, value)
	e.memtable.Size += 1
	return nil
}

func (e *Engine) Insert(key int64, value int64) error {
	if err := e.wal.Append(&wal.WalRecord{key, value, wal.OpPut}); err != nil {
		log.Fatalf("Insert failed: key=%d value=%d err=%v", key, value, err)
		return err
	}
	err := e.insertIntoMemtable(key, value)
	if err != nil {
		log.Fatalf("Insert failed: key=%d value=%d err=%v", key, value, err)
		return err
	}
	return nil
}

func (e *Engine) Get(key int64) int64 {
	log.Printf("Get called: key=%d ", key)
	if val, ok := e.memtable.SkipList.Search(key); ok {
		log.Printf("key found in Skip list with val: %v", val)
		return val
	}

	iterators := make([]*memTable.Iterator, 0, len(e.immutable))
	for _, imm := range e.immutable {
		iterators = append(iterators, imm.SkipList.Iterator())
	}
	merged := memTable.Merge(iterators)
	for _, e := range merged {
		if e.Key == key && !e.Tombstoned {
			log.Printf("key found in Immutable memtable with value: %v", e)
			return e.Val
		}
	}
	log.Printf("key not found in Immutable memtable, now search in sstable: %v", key)
	for i, level := range e.sstable {
		for j := len(level) - 1; j >= 0; j-- {
			if val, err := level[j].Get(key); err == true {
				log.Printf("key found in SSTable level: %d, ssteble: %d,  with val: %v", i, j, val)
				return val
			}
		}
	}
	return -1
}

func (e *Engine) Delete(key int64) error {
	if err := e.wal.Append(&wal.WalRecord{key, 0, wal.OpDelete}); err != nil {
		log.Fatalf("Insert failed: key=%d err=%v", key, err)
		return err
	}
	e.memtable.SkipList.Delete(key)
	return nil
}

func CreateEngine(config config.Config) (*Engine, error) {
	w, err := wal.CreateNewWal()
	if err != nil {
		return nil, err
	}

	mf, err := mainfest.NewMainfest()
	if err != nil {
		return nil, err
	}

	newEngine := &Engine{
		config:    config,
		memtable:  memTable.NewMemTable(),
		immutable: make([]*memTable.MemTable, 0),
		sstable:   make([][]*sstable.SSTable, 3),
		wal:       w,
		mainfest:  mf,
	}
	records, err := wal.Reply()
	if err != nil {
		return &Engine{}, err
	}

	for _, record := range records {
		if record.OP { //OpPut
			err := newEngine.insertIntoMemtable(record.Key, record.Value)
			if err != nil {
				return &Engine{}, err
			}
		} else {
			err := newEngine.Delete(record.Key)
			if err != nil {
				return &Engine{}, err
			}
		}
	}

	//TODO: Reply Mainfest

	return newEngine, nil
}

func (e *Engine) forceCompaction() {
	// Check if the L0 is full -> start compaction L0 and L1 and so on for other levels
	// Update Mainfest
	for level := 0; level+1 < len(e.sstable); level++ {
		if len(e.sstable[level]) == 5 { //TODO: Change fixed number into matrix of level and max Size
			iterators := make([]*sstable.Iterator, len(e.sstable[level])+len(e.sstable[level+1]))
			idx := 0
			for i := 0; i < 2; i++ {
				for j := 0; j < len(e.sstable[level+i]); j++ {
					iterators[idx] = sstable.NewIterator(e.sstable[level+i][j])
					idx++
				}
			}
			mergeIt := sstable.NewMergeIterator(iterators)
			newSSTables, err := sstable.Compaction(mergeIt, int8(level+1))
			if err != nil {
				log.Fatalf("Compaction failed: %v", err)
			}

			oldFiles := make([][]string, 2)
			for i := 0; i < 2; i++ {
				tables := e.sstable[level+i]
				oldFiles[i] = make([]string, len(tables))
				for j, t := range tables {
					oldFiles[i][j] = t.FileName
					t.Delete() //TODO: delete after mainfest
				}
			}
			newFiles := make([]string, len(newSSTables))
			for j, t := range newSSTables {
				newFiles[j] = t.FileName
			}

			e.mainfest.Add(e.mainfest.CreateMinfestRecords(oldFiles[0], nil, "Compaction", int8(level)))
			e.mainfest.Add(e.mainfest.CreateMinfestRecords(oldFiles[1], newFiles, "Compaction", int8(level+1)))

			e.sstable[level+1] = newSSTables
			e.sstable[level] = []*sstable.SSTable{}
		}
	}
}
