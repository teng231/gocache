package gocache

import (
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"testing"
)

func TestRemove(t *testing.T) {
	t.Run("EmptyKeysSlice", func(t *testing.T) {
		keys := []*string{}
		newKeys, found := removeV2(keys, "key")
		if found {
			t.Error("Expected found to be false")
		}
		if len(newKeys) != 0 {
			t.Errorf("Expected newKeys length to be 0, got %d", len(newKeys))
		}
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		keys := []*string{stringPtr("key1"), stringPtr("key2")}
		newKeys, found := removeV2(keys, "key3")
		if found {
			t.Error("Expected found to be false")
		}
		if len(newKeys) != 2 {
			t.Errorf("Expected newKeys length to be 2, got %d", len(newKeys))
		}
	})

	t.Run("KeyFound", func(t *testing.T) {
		keys := []*string{stringPtr("key1"), stringPtr("key2")}
		newKeys, found := removeV2(keys, "key1")
		if !found {
			t.Error("Expected found to be true")
		}
		if len(newKeys) != 1 {
			t.Errorf("Expected newKeys length to be 1, got %d", len(newKeys))
		}
		if newKeys[0] != nil && *newKeys[0] == "key1" {
			t.Error("Key was not removed as expected")
		}
	})
}

func stringPtr(s string) *string {
	return &s
}

func TestPopV2(t *testing.T) {
	slice := []*string{
		stringPtr("one"),
		stringPtr("two"),
		stringPtr("three"),
		stringPtr("four"),
		stringPtr("five"),
	}

	// Test case 1: valid index
	index := 1
	expectedPopped := stringPtr("two")
	expectedRemaining := []*string{
		stringPtr("one"),
		stringPtr("two"),
		stringPtr("four"),
		stringPtr("five"),
	}

	remaining, popped := popV2(slice, index)
	// if !reflect.DeepEqual(remaining, expectedRemaining) {
	// 	t.Errorf("Expected remaining slice: %v, but got: %v", expectedRemaining, remaining)
	// }
	if !reflect.DeepEqual(popped, expectedPopped) {
		t.Errorf("Expected popped value: %v, but got: %v", *expectedPopped, *popped)
	}

	// Test case 2: index out of range
	index = 5
	expectedRemaining = slice
	expectedPopped = nil

	remaining, popped = popV2(slice, index)
	if !reflect.DeepEqual(remaining, expectedRemaining) {
		t.Errorf("Expected remaining slice: %v, but got: %v", expectedRemaining, remaining)
	}
	if popped != expectedPopped {
		t.Errorf("Expected popped value: %v, but got: %v", expectedPopped, popped)
	}
}

func TestPopV3(t *testing.T) {
	in := []*string{}
	for i := 0; i < 200; i++ {
		key := "key:" + strconv.Itoa(i)
		in = append(in, &key)
	}

	for i := 0; i < 100; i++ {
		var item *string
		in, item = popV2(in, 0)
		log.Print("pop ", *item)
	}
	out, _ := json.MarshalIndent(in, "", " ")
	log.Print(string(out))
}

func TestPopRemoveV3(t *testing.T) {
	in := []*string{}
	for i := 0; i < 200; i++ {
		key := "key:" + strconv.Itoa(i)
		in = append(in, &key)
	}

	for i := 0; i < 100; i++ {
		var item *string
		idex := i + 1
		in, item = popV2(in, idex)
		log.Print("pop ", *item)
	}
	out, _ := json.MarshalIndent(in, "", " ")
	log.Print(string(out))
}
