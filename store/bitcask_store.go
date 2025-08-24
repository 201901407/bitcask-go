package bitcask

import (
	"fmt"
	"os"
	"time"
	"encoding/binary"
	"strings"
)

const (
	SEGMENT_SIZE = 64 * 1024 * 1024 //64 MB
	SEGMENT_PREFIX = "bitcask_segment_"
)

type Segment struct {
	//only for active segment
	File *os.File
	FilePath string
	IsFull bool
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
	Offset int64
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
}

func(kvStore *BitcaskKVStore) extractKeysFromSegmentFile(segmentFile *os.File) error {
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
			delete(kvStore.KeysToValPointers,string(key))
		} else {
			// Store the key and its location
			kvStore.KeysToValPointers[string(key)] = ValueLocation{
				SegmentPath: segmentFile.Name(),
				Offset: offset,
			}
		}

        // Move to the next record (header + key + value)
        offset += 10 + int64(keySize) + int64(valueSize)
    }

    return nil
}

func(kvStore *BitcaskKVStore) Init() error {
	//initialize the Bitcask KV Store
	kvStore.KeysToValPointers = make(map[string]ValueLocation)
	kvStore.SegmentTracker = []Segment{}

	//search for existing segments and load them
	//list all files in current directory
	allFiles, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:",err)
		return err
	}

	//filter files with segment prefix
	for _, file := range allFiles {
		if !file.IsDir() && strings.HasPrefix(file.Name(), SEGMENT_PREFIX) {
			//load this segment
			SegmentFile, err := os.OpenFile(file.Name(), os.O_RDWR, 0644)
			if err != nil {
				fmt.Println("Error initlializing Bitcask KV Store:",err)
				return err
			}

			//construct the in-memory hash map from this segment
			err = kvStore.extractKeysFromSegmentFile(SegmentFile)
			if err != nil {
				return fmt.Errorf("error loading keys from segment file %s: %w", file.Name(), err)
			}

			SegmentStat, err := SegmentFile.Stat()
			if err != nil {
				fmt.Println("Error initlializing Bitcask KV Store:",err)
				return err
			}

			kvStore.SegmentTracker = append(kvStore.SegmentTracker, Segment{
				File: SegmentFile,
				FilePath: file.Name(),
				IsFull: true,
				CurrentFileSize: SegmentStat.Size(),
			})

			SegmentFile.Close()
		}
	}

	//open a new active segment
	timestamp := time.Now().UnixMilli()
	SegmentName := fmt.Sprintf("bitcask_segment_%d",timestamp)
	NewSegment, err := os.OpenFile(SegmentName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:",err)
		return err
	}

	kvStore.ActiveSegment.File = NewSegment
	kvStore.ActiveSegment.IsFull = false
	kvStore.ActiveSegment.FilePath = SegmentName
	FileStats, err := NewSegment.Stat()
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:",err)
		return err
	}
	kvStore.ActiveSegment.CurrentFileSize = FileStats.Size()

	return err
}


func(kvStore *BitcaskKVStore) Get(key string) (string,error) {
	RecordOffset, ok := kvStore.KeysToValPointers[key]

	if !ok {
		fmt.Println("Error in fetching key from Bitcask KV Store")
		return "",fmt.Errorf("error in fetching key %s from Bitcask KV Store",key)
	}

	//open separate file descriptor for reading values from disk
	readDescriptor, err := os.OpenFile(RecordOffset.SegmentPath, os.O_RDONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w",key, err)
	}

	//read the header -> timestamp + keysize + valuesize
	header := make([]byte, 10)
	if _, err := readDescriptor.ReadAt(header, RecordOffset.Offset); err != nil {
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w",key, err)
	}

	//Parse fields from header
	keySize := binary.LittleEndian.Uint16(header[4:6])
	valueSize := binary.LittleEndian.Uint32(header[6:10])

	//Read the value only (skip key)
	value := make([]byte, valueSize)
	valueOffset := RecordOffset.Offset + 10 + int64(keySize)

	if _, err := readDescriptor.ReadAt(value, valueOffset); err != nil {
		return "", fmt.Errorf("error in fetching key %s from Bitcask KV Store: %w",key, err)
	}

	return string(value), nil
}

func getSegmentRecord(key string, value string) (SegmentRecord) {
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

func createWriteRecord(key string, value string) ([]byte) {
	//create buffer for header
	NewSegmentRecord := getSegmentRecord(key,value)
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

func(kvStore *BitcaskKVStore) setKeyInNewSegment(key string,value string) error {
	timestamp := time.Now().UnixMilli()
	SegmentName := fmt.Sprintf("bitcask_segment_%d",timestamp)
	NewSegment, err := os.OpenFile(SegmentName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil || NewSegment == nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}

	defer func() {
		if err != nil {
			closerr := NewSegment.Close()
			if closerr != nil {
				fmt.Errorf("[Rollback] failed to close file handle, Error: %w",err.Error())
			}
			rollbackerr := os.Remove(SegmentName)
			if rollbackerr != nil {
				fmt.Errorf("[Rollback] Rollback failed, unable to delete the newly created segment. Error: %s",err.Error())
			}
		}
	} ()

	//this is the offset where the record starts so note it
	SegmentStat, err := NewSegment.Stat()
	if err != nil || SegmentStat == nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}

	currentOffset := SegmentStat.Size()


	record := createWriteRecord(key,value)

	_, err = NewSegment.Write(record)
	if err != nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}

	//store the offset and current segmentpath in hash map
	kvStore.KeysToValPointers[key] = ValueLocation{
		SegmentPath: SegmentName,
		Offset: currentOffset,
	}

	defer func() {
		if err != nil {
			delete(kvStore.KeysToValPointers,key)
		}
	} ()

	SegmentStat, err = NewSegment.Stat()
	if err != nil || SegmentStat == nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}

	err = kvStore.ActiveSegment.File.Close()
	if err != nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}
	kvStore.SegmentTracker = append(kvStore.SegmentTracker, kvStore.ActiveSegment)

	currSize := SegmentStat.Size()

	kvStore.ActiveSegment = Segment{
		File: NewSegment,
		FilePath: SegmentName,
		IsFull: false,
		CurrentFileSize: currSize,
	}

	fmt.Printf("Successful operation..")
	return nil
}

func(kvStore *BitcaskKVStore) Set(key string,value string) error {
	RecordSize := len(key) + len(value) + 10 //fixed header bytes
	
	if RecordSize > SEGMENT_SIZE {
		fmt.Printf("Total size of key + value can't exceed segment size")
		return fmt.Errorf("Total size of key + value can't exceed segment size. Segment size is %d",SEGMENT_SIZE)
	}

	if RecordSize + int(kvStore.ActiveSegment.CurrentFileSize) > SEGMENT_SIZE {
		//close the current segment and open a new one
		//Written like a transaction

		err := kvStore.setKeyInNewSegment(key,value)
		if err != nil {
			return err
		}

		fmt.Printf("Successfully set key %s in Bitcask KV store..",key)
		return nil
	}

	activeSegment := kvStore.ActiveSegment

	currentOffset := activeSegment.CurrentFileSize
	//print(currentOffset)

	record := createWriteRecord(key,value)

	_, err := activeSegment.File.Write(record)
	if err != nil {
		fmt.Println("Error inserting key in Bitcask KV store:",err)
		return err
	}

	kvStore.KeysToValPointers[key] = ValueLocation{
		SegmentPath: activeSegment.FilePath,
		Offset: currentOffset,
	}

	kvStore.ActiveSegment.CurrentFileSize += int64(RecordSize)

	fmt.Printf("Successful Set operation..")
	return nil
}

func(kvStore *BitcaskKVStore) Delete(key string) error {
	if _, ok := kvStore.KeysToValPointers[key]; !ok {
		return fmt.Errorf("Key %s not found in Bitcask KV store..",key)
	}

	tombstone := createWriteRecord(key,"")
	tombstoneSize := len(tombstone)
	if tombstoneSize + int(kvStore.ActiveSegment.CurrentFileSize) > SEGMENT_SIZE {
		//close the current segment and open a new one
		err := kvStore.setKeyInNewSegment(key,"")
		if err != nil {
			fmt.Println("Error deleting key in Bitcask KV store:",err)
			return err
		}

		delete(kvStore.KeysToValPointers,key)

		fmt.Printf("Successfully deleted key %s from Bitcask KV store..",key)
		return nil
	}

	_, err := kvStore.ActiveSegment.File.Write(tombstone)
	if err != nil {
		fmt.Println("Error deleting key in Bitcask KV store:",err)
		return err
	}
	kvStore.ActiveSegment.CurrentFileSize += int64(tombstoneSize)
	delete(kvStore.KeysToValPointers,key)

	fmt.Printf("Successfully deleted key %s from Bitcask KV store..",key)
	return nil
}