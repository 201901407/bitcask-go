package bitcask

import (
	"fmt"
	"os"
	"time"
)

const SEGMENT_SIZE = 20 * 1024 //20 KB

type Segment struct {
	File *os.File
	IsFull bool
}

type BitcaskKVStore struct {
	//hash map storing keys to pointers where the value resides.
	//For simplicity, we consider each value to be of string type. It's a fair assumption
	//as every object can be converted to string
	KeysToValPointers map[string]*string
	//list to keep track of all segments (active and closed)
	SegmentTracker []Segment
	//reference object to active segment
	ActiveSegment Segment
}

func(kvStore BitcaskKVStore) init() error {
	//initialize the Bitcask KV Store
	kvStore.KeysToValPointers = make(map[string]*string)
	kvStore.SegmentTracker = []Segment{}

	timestamp := time.Now().UnixMilli()
	SegmentName := fmt.Sprintf("bitcask_segment_%d",timestamp)
	NewSegment, err := os.OpenFile(SegmentName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Error initlializing Bitcask KV Store:",err)
		return err
	}

	kvStore.ActiveSegment.File = NewSegment
	kvStore.ActiveSegment.IsFull = false

	return nil
}


func(kvStore BitcaskKVStore) Get(key string) (string,error) {
	ValuePointer, ok := kvStore.KeysToValPointers[key]

	if !ok || ValuePointer == nil{
		fmt.Println("Error in fetching key from Bitcask KV Store")
		return "",fmt.Errorf("error in fetching key %s from Bitcask KV Store",key)
	}

	ValueString := *ValuePointer
	return ValueString, nil
}

func(kvStore BitcaskKVStore) Set(key string,value string) error {
	return nil
}