package bitcask

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SEGMENT_SIZE         = 64 * 1024 * 1024 //64 MB
	SEGMENT_PREFIX       = "bitcask_segment_"
	COMPACTION_INTERVAL  = 30 * time.Second //trigger compaction every 30 seconds
	COMPACTION_THRESHOLD = 5                //compact when there are 5+ segments
	DEFAULT_DIR          = "."              //default directory to store segment files
)

type Segment struct {
	//only for active segment
	File            *os.File
	FilePath        string
	IsFull          bool
	CurrentFileSize int64
}

type SegmentRecord struct {
	Timestamp uint32
	KeySize   uint16
	ValueSize uint32
	Key       []byte
	Value     []byte
}

type ValueLocation struct {
	SegmentPath string
	Offset      int64
}

type BitcaskKVStore struct {
	//hash map storing keys to pointers where the value resides.
	//For simplicity, we consider each value to be of string type. It's a fair assumption
	//as every object can be converted to string
	KeysToValPointers map[string]ValueLocation
	//list to keep track of all closed segments
	SegmentTracker []Segment
	//reference object to active segment
	ActiveSegment Segment
	//mutex to protect concurrent access
	mu sync.RWMutex
	//flag to stop compaction goroutine
	stopCompaction chan bool
	//flag to track if compaction is running
	isCompacting bool
	//all valid segments file handle pool
	//used for quick access to segment files without needing to open them every time, which is expensive
	segmentFileHandles map[string]*os.File
}

func (kvStore *BitcaskKVStore) extractKeysFromSegmentFile(segmentFile *os.File) error {
	// Get the file size
	fileStat, err := segmentFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}
	fileSize := fileStat.Size()

	// Start reading from the beginning of the file
	offset := int64(0)
	for offset < fileSize {
		// Read the header (10 bytes)
		header := make([]byte, 10)
		_, err := segmentFile.ReadAt(header, offset)
		if err != nil {
			return fmt.Errorf("failed to read header at offset %d: %w", offset, err)
		}

		// Parse the header
		keySize := binary.LittleEndian.Uint16(header[4:6])
		valueSize := binary.LittleEndian.Uint32(header[6:10])

		// Read the key
		key := make([]byte, keySize)
		_, err = segmentFile.ReadAt(key, offset+10)
		if err != nil {
			return fmt.Errorf("failed to read key at offset %d: %w", offset+10, err)
		}

		if valueSize == 0 {
			//tombstone record, delete the key from map if it exists
			delete(kvStore.KeysToValPointers, string(key))
		} else {
			// Store the key and its location
			kvStore.KeysToValPointers[string(key)] = ValueLocation{
				SegmentPath: segmentFile.Name(),
				Offset:      offset,
			}
		}

		// Move to the next record (header + key + value)
		offset += 10 + int64(keySize) + int64(valueSize)
	}

	return nil
}

// listSortedSegmentFiles returns segment file names in ascending timestamp order.
// It looks for files with the prefix defined by SEGMENT_PREFIX and parses
// the trailing timestamp. Files that don't match the pattern are ignored.
func listSortedSegmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// collect candidate filenames
	names := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, SEGMENT_PREFIX) {
			names = append(names, name)
		}
	}

	// sort in-place by parsing timestamp suffix; fallback to lexical order on parse error
	sort.Slice(names, func(i, j int) bool {
		tsi := strings.TrimPrefix(names[i], SEGMENT_PREFIX)
		tsj := strings.TrimPrefix(names[j], SEGMENT_PREFIX)
		vi, erri := strconv.ParseInt(tsi, 10, 64)
		vj, errj := strconv.ParseInt(tsj, 10, 64)
		if erri == nil && errj == nil {
			return vi < vj
		}
		if erri == nil {
			return true
		}
		if errj == nil {
			return false
		}
		return names[i] < names[j]
	})

	return names, nil
}

func (kvStore *BitcaskKVStore) Init() error {
	//initialize the Bitcask KV Store
	kvStore.KeysToValPointers = make(map[string]ValueLocation)
	kvStore.SegmentTracker = []Segment{}
	kvStore.segmentFileHandles = make(map[string]*os.File)

	// search for existing segments in the specfied directory
	// and load them in creation order
	// currently, the default directory is the current working directory
	segFiles, err := listSortedSegmentFiles(DEFAULT_DIR)
	if err != nil {
		fmt.Println("Error initializing Bitcask KV Store:", err)
		return err
	}

	for _, name := range segFiles {
		f, err := os.OpenFile(name, os.O_RDWR, 0644)
		if err != nil {
			fmt.Println("Error initializing Bitcask KV Store:", err)
			return err
		}

		if err := kvStore.extractKeysFromSegmentFile(f); err != nil {
			f.Close()
			return fmt.Errorf("error loading keys from segment file %s: %w", name, err)
		}

		stat, err := f.Stat()
		if err != nil {
			f.Close()
			fmt.Println("Error initializing Bitcask KV Store:", err)
			return err
		}

		kvStore.SegmentTracker = append(kvStore.SegmentTracker, Segment{
			File:            f,
			FilePath:        name,
			IsFull:          true,
			CurrentFileSize: stat.Size(),
		})

		kvStore.segmentFileHandles[name] = f
	}

	//open a new active segment
	timestamp := time.Now().UnixMilli()
	SegmentName := fmt.Sprintf("bitcask_segment_%d", timestamp)
	NewSegment, err := os.OpenFile(SegmentName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:", err)
		return err
	}

	kvStore.ActiveSegment.File = NewSegment
	kvStore.ActiveSegment.IsFull = false
	kvStore.ActiveSegment.FilePath = SegmentName
	FileStats, err := NewSegment.Stat()
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:", err)
		return err
	}
	kvStore.ActiveSegment.CurrentFileSize = FileStats.Size()

	//add active segment to file handle pool
	kvStore.segmentFileHandles[SegmentName] = NewSegment

	// Start background compaction
	kvStore.StartCompactionBackground()

	return err
}

func (kvStore *BitcaskKVStore) Get(key string) (string, error) {
	kvStore.mu.RLock()
	RecordOffset, ok := kvStore.KeysToValPointers[key]
	kvStore.mu.RUnlock()

	if !ok {
		fmt.Println("Error in fetching key from Bitcask KV Store")
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store", key)
	}

	//try to get file descriptor from pool, fallback to opening if not found
	kvStore.mu.RLock()
	readDescriptor, exists := kvStore.segmentFileHandles[RecordOffset.SegmentPath]
	kvStore.mu.RUnlock()

	var shouldClose bool
	if !exists {
		//fallback: open the file if not in pool
		var err error
		readDescriptor, err = os.OpenFile(RecordOffset.SegmentPath, os.O_RDONLY, 0644)
		if err != nil {
			return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w", key, err)
		}
		shouldClose = true
	}

	if shouldClose {
		defer readDescriptor.Close()
	}

	//read the header -> timestamp + keysize + valuesize
	header := make([]byte, 10)
	if _, err := readDescriptor.ReadAt(header, RecordOffset.Offset); err != nil {
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w", key, err)
	}

	//Parse fields from header
	keySize := binary.LittleEndian.Uint16(header[4:6])
	valueSize := binary.LittleEndian.Uint32(header[6:10])

	//Read the value only (skip key)
	value := make([]byte, valueSize)
	valueOffset := RecordOffset.Offset + 10 + int64(keySize)

	if _, err := readDescriptor.ReadAt(value, valueOffset); err != nil {
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w", key, err)
	}

	return string(value), nil
}

func getSegmentRecord(key string, value string) SegmentRecord {
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	return SegmentRecord{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint16(len(keyBytes)),
		ValueSize: uint32(len(valueBytes)),
		Key:       keyBytes,
		Value:     valueBytes,
	}
}

func createWriteRecord(key string, value string) []byte {
	//create buffer for header
	NewSegmentRecord := getSegmentRecord(key, value)
	header := make([]byte, 10)
	binary.LittleEndian.PutUint32(header[0:4], NewSegmentRecord.Timestamp)
	binary.LittleEndian.PutUint16(header[4:6], NewSegmentRecord.KeySize)
	binary.LittleEndian.PutUint32(header[6:10], NewSegmentRecord.ValueSize)

	// Construct the full record
	record := make([]byte, 0, 10+int(NewSegmentRecord.KeySize)+int(NewSegmentRecord.ValueSize))
	record = append(record, header...)
	record = append(record, NewSegmentRecord.Key...)
	record = append(record, NewSegmentRecord.Value...)

	return record
}

func (kvStore *BitcaskKVStore) setKeyInNewSegment(key string, value string) error {
	timestamp := time.Now().UnixMilli()
	SegmentName := fmt.Sprintf("bitcask_segment_%d", timestamp)
	NewSegment, err := os.OpenFile(SegmentName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil || NewSegment == nil {
		fmt.Println("Error inserting key in Bitcask KV store:", err)
		return err
	}

	defer func() {
		if err != nil {
			closerr := NewSegment.Close()
			if closerr != nil {
				fmt.Errorf("[Rollback] failed to close file handle, Error: %w", err.Error())
			}
			rollbackerr := os.Remove(SegmentName)
			if rollbackerr != nil {
				fmt.Errorf("[Rollback] Rollback failed, unable to delete the newly created segment. Error: %s", err.Error())
			}
		}
	}()

	//this is the offset where the record starts so note it
	SegmentStat, err := NewSegment.Stat()
	if err != nil || SegmentStat == nil {
		fmt.Println("Error inserting key in Bitcask KV store:", err)
		return err
	}

	currentOffset := SegmentStat.Size()

	record := createWriteRecord(key, value)

	_, err = NewSegment.Write(record)
	if err != nil {
		fmt.Println("Error inserting key in Bitcask KV store:", err)
		return err
	}

	//store the offset and current segmentpath in hash map
	kvStore.KeysToValPointers[key] = ValueLocation{
		SegmentPath: SegmentName,
		Offset:      currentOffset,
	}

	defer func() {
		if err != nil {
			delete(kvStore.KeysToValPointers, key)
		}
	}()

	SegmentStat, err = NewSegment.Stat()
	if err != nil || SegmentStat == nil {
		fmt.Println("Error inserting key in Bitcask KV store:", err)
		return err
	}

	if kvStore.ActiveSegment.File != nil {
		err = kvStore.ActiveSegment.File.Close()
		if err != nil {
			fmt.Println("Error inserting key in Bitcask KV store:", err)
			return err
		}
	}

	kvStore.SegmentTracker = append(kvStore.SegmentTracker, kvStore.ActiveSegment)

	currSize := SegmentStat.Size()

	kvStore.ActiveSegment = Segment{
		File:            NewSegment,
		FilePath:        SegmentName,
		IsFull:          false,
		CurrentFileSize: currSize,
	}

	//add new segment to file handle pool
	kvStore.segmentFileHandles[SegmentName] = NewSegment

	fmt.Printf("Successful operation..")
	return nil
}

func (kvStore *BitcaskKVStore) Set(key string, value string) error {
	kvStore.mu.Lock()
	defer kvStore.mu.Unlock()

	RecordSize := len(key) + len(value) + 10 //fixed header bytes

	if RecordSize > SEGMENT_SIZE {
		fmt.Printf("Total size of key + value can't exceed segment size")
		return fmt.Errorf("Total size of key + value can't exceed segment size. Segment size is %d", SEGMENT_SIZE)
	}

	if RecordSize+int(kvStore.ActiveSegment.CurrentFileSize) > SEGMENT_SIZE {
		//close the current segment and open a new one
		//Written like a transaction

		err := kvStore.setKeyInNewSegment(key, value)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully set key %s in Bitcask KV store..", key)
		return nil
	}

	activeSegment := kvStore.ActiveSegment

	currentOffset := activeSegment.CurrentFileSize
	//print(currentOffset)

	record := createWriteRecord(key, value)

	_, err := activeSegment.File.Write(record)
	if err != nil {
		fmt.Println("Error inserting key in Bitcask KV store:", err)
		return err
	}

	kvStore.KeysToValPointers[key] = ValueLocation{
		SegmentPath: activeSegment.FilePath,
		Offset:      currentOffset,
	}

	kvStore.ActiveSegment.CurrentFileSize += int64(RecordSize)

	fmt.Printf("Successful Set operation..")
	return nil
}

func (kvStore *BitcaskKVStore) Delete(key string) error {
	kvStore.mu.Lock()
	defer kvStore.mu.Unlock()

	if _, ok := kvStore.KeysToValPointers[key]; !ok {
		return fmt.Errorf("Key %s not found in Bitcask KV store..", key)
	}

	tombstone := createWriteRecord(key, "")
	tombstoneSize := len(tombstone)
	if tombstoneSize+int(kvStore.ActiveSegment.CurrentFileSize) > SEGMENT_SIZE {
		//close the current segment and open a new one
		err := kvStore.setKeyInNewSegment(key, "")
		if err != nil {
			fmt.Println("Error deleting key in Bitcask KV store:", err)
			return err
		}

		delete(kvStore.KeysToValPointers, key)

		fmt.Printf("Successfully deleted key %s from Bitcask KV store..", key)
		return nil
	}

	_, err := kvStore.ActiveSegment.File.Write(tombstone)
	if err != nil {
		fmt.Println("Error deleting key in Bitcask KV store:", err)
		return err
	}
	kvStore.ActiveSegment.CurrentFileSize += int64(tombstoneSize)
	delete(kvStore.KeysToValPointers, key)

	fmt.Printf("Successfully deleted key %s from Bitcask KV store..", key)
	return nil
}

// StartCompactionBackground starts the async compaction goroutine
func (kvStore *BitcaskKVStore) StartCompactionBackground() {
	kvStore.stopCompaction = make(chan bool)
	go kvStore.compactionLoop()
	fmt.Println("Compaction background process started")
}

// StopCompactionBackground stops the async compaction goroutine
func (kvStore *BitcaskKVStore) StopCompactionBackground() {
	if kvStore.stopCompaction != nil {
		kvStore.stopCompaction <- true
		close(kvStore.stopCompaction)
	}
	fmt.Println("Compaction background process stopped")
}

// compactionLoop runs periodically to trigger compaction
func (kvStore *BitcaskKVStore) compactionLoop() {
	ticker := time.NewTicker(COMPACTION_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			kvStore.mu.RLock()
			shouldCompact := len(kvStore.SegmentTracker) >= COMPACTION_THRESHOLD && !kvStore.isCompacting
			kvStore.mu.RUnlock()

			if shouldCompact {
				kvStore.Compact()
			}

		case <-kvStore.stopCompaction:
			fmt.Println("Compaction loop exiting")
			return
		}
	}
}

// Compact merges multiple segments and removes tombstones
func (kvStore *BitcaskKVStore) Compact() error {

	//check whether compaction is already running or not,
	//if yes return immediately to avoid multiple concurrent compactions
	kvStore.mu.Lock()
	if kvStore.isCompacting {
		kvStore.mu.Unlock()
		return fmt.Errorf("compaction already in progress")
	}
	kvStore.isCompacting = true
	kvStore.mu.Unlock()

	// Ensure we clear the flag even if we panic
	// so that we don't end up in a state where compaction can never run again
	defer func() {
		kvStore.mu.Lock()
		kvStore.isCompacting = false
		kvStore.mu.Unlock()
	}()

	// Copy closed segments with read lock (doesn't block readers!)
	kvStore.mu.RLock()
	segmentsToCompact := make([]Segment, len(kvStore.SegmentTracker))
	copy(segmentsToCompact, kvStore.SegmentTracker)

	closedSegmentPaths := make(map[string]bool)
	for _, seg := range segmentsToCompact {
		closedSegmentPaths[seg.FilePath] = true
	}
	kvStore.mu.RUnlock()

	if len(segmentsToCompact) == 0 {
		return nil
	}

	fmt.Printf("Starting compaction of %d segments...\n", len(segmentsToCompact))

	// Identify keys that need to be compacted (those that point to the segments being compacted)
	kvStore.mu.RLock()
	keysToCompact := make(map[string]ValueLocation)
	for key, loc := range kvStore.KeysToValPointers {
		if closedSegmentPaths[loc.SegmentPath] {
			keysToCompact[key] = loc
		}
	}
	openFileHandles := kvStore.segmentFileHandles
	kvStore.mu.RUnlock()

	if len(keysToCompact) == 0 {
		fmt.Println("No keys to compact, skipping compaction")
		return nil
	}

	// Create compacted segment files as needed (roll when size limit reached)
	compactedBase := SEGMENT_PREFIX

	createNewCompacted := func() (*os.File, string, error) {
		timestamp := time.Now().UnixMilli()
		name := fmt.Sprintf("%s%d", compactedBase, timestamp)
		f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		return f, name, err
	}

	curFile, curPath, err := createNewCompacted()
	if err != nil {
		return fmt.Errorf("failed to create compacted segment: %w", err)
	}

	createdSegments := make([]Segment, 0)
	createdHandles := make(map[string]*os.File)
	createdHandles[curPath] = curFile

	// read keys to be compacted and write to new segment(s), track new offsets in a map
	curOffset := int64(0)
	newIndex := make(map[string]ValueLocation)

	for key, oldLoc := range keysToCompact {
		file := openFileHandles[oldLoc.SegmentPath]
		if file == nil {
			// fallback: open file if not present in pool
			file, err = os.OpenFile(oldLoc.SegmentPath, os.O_RDONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open source segment %s: %w", oldLoc.SegmentPath, err)
			}
			defer file.Close()
		}

		header := make([]byte, 10)
		if _, err := file.ReadAt(header, oldLoc.Offset); err != nil {
			return fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w", key, err)
		}

		// Parse fields from header
		keySize := binary.LittleEndian.Uint16(header[4:6])
		valueSize := binary.LittleEndian.Uint32(header[6:10])

		// Read the value only (skip key)
		value := make([]byte, valueSize)
		valueOffset := oldLoc.Offset + 10 + int64(keySize)

		if _, err := file.ReadAt(value, valueOffset); err != nil {
			return fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w", key, err)
		}

		record := createWriteRecord(key, string(value))

		// roll file if writing this record would exceed segment size
		if curOffset+int64(len(record)) > SEGMENT_SIZE {
			// finalize current file metadata
			createdSegments = append(createdSegments, Segment{
				File:            curFile,
				FilePath:        curPath,
				IsFull:          true,
				CurrentFileSize: curOffset,
			})

			curFile, curPath, err = createNewCompacted()
			if err != nil {
				return fmt.Errorf("failed to create compacted segment: %w", err)
			}
			createdHandles[curPath] = curFile
			curOffset = 0
		}

		n, err := curFile.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write record for key %s to compacted segment: %w", key, err)
		}

		newIndex[key] = ValueLocation{
			SegmentPath: curPath,
			Offset:      curOffset,
		}
		curOffset += int64(n)
	}

	// append final current file metadata
	createdSegments = append(createdSegments, Segment{
		File:            curFile,
		FilePath:        curPath,
		IsFull:          true,
		CurrentFileSize: curOffset,
	})

	kvStore.mu.Lock()

	// update pointers for compacted keys if they still point to the old closed segments
	for key, newLoc := range newIndex {
		currentLoc, exists := kvStore.KeysToValPointers[key]
		if exists && closedSegmentPaths[currentLoc.SegmentPath] {
			kvStore.KeysToValPointers[key] = newLoc
		}
	}

	// prepare lists of old handles to close and files to remove (do actual close/remove outside lock)
	toClose := make([]*os.File, 0, len(segmentsToCompact))
	toRemovePaths := make([]string, 0, len(segmentsToCompact))
	for _, seg := range segmentsToCompact {
		if fh, exists := kvStore.segmentFileHandles[seg.FilePath]; exists {
			toClose = append(toClose, fh)
			delete(kvStore.segmentFileHandles, seg.FilePath)
		}
		toRemovePaths = append(toRemovePaths, seg.FilePath)
	}

	// update segment tracker to remove old segments and add new compacted segments
	kvStore.SegmentTracker = createdSegments

	// register the handles in the store
	for _, cs := range createdSegments {
		kvStore.segmentFileHandles[cs.FilePath] = cs.File
	}

	kvStore.mu.Unlock()

	// close and remove old segment files outside the lock
	for i, fh := range toClose {
		path := toRemovePaths[i]
		_ = fh.Close()
		_ = os.Remove(path)
	}

	return nil
}
