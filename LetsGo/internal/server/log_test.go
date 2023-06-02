package server

import (
	"reflect"
	"testing"
)

func TestLog_Append_CallOnce(t *testing.T) {
	log := NewLog()
	offset, err := log.Append(Record{
		Value: []byte("Hello, world!"),
	})

	if offset != 0 {
		t.Errorf("offset should be 0 after appending first record, but got = %d", offset)
	}

	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}
}

func TestLog_Append_CallTwice(t *testing.T) {
	log := NewLog()

	// 1st Append
	_, err := log.Append(Record{
		Value: []byte("Hello, world!"),
	})

	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}

	// 2nd Append
	offset, err := log.Append(Record{
		Value: []byte("Hello, world!"),
	})

	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}

	if offset != 1 {
		t.Errorf("offset should be 0 after appending first record, but got = %d", offset)
	}
}

func TestLog_Read(t *testing.T) {
	log := NewLog()

	_, err := log.Append(Record{Value: []byte("Hello, world 1")})
	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}

	record := Record{
		Value: []byte("Hello, world 2"),
	}
	_, err = log.Append(record)
	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}

	_, err = log.Append(Record{Value: []byte("Hello, world 3")})
	if err != nil {
		t.Errorf("never return err: got = %v", err)
	}

	gotRecord, err := log.Read(1)
	if err != nil {
		t.Errorf("should not return error: want = nil, got = %v", err)
	}

	wantRecord := Record{
		Value:  record.Value,
		Offset: 1,
	}
	if !reflect.DeepEqual(gotRecord, wantRecord) {
		t.Errorf("should return record appended: got = %v, want = %v", gotRecord, record)
	}
}

func TestLog_Read_outOfRange(t *testing.T) {
	log := NewLog()
	record, err := log.Read(0)

	if err != ErrOffsetNotFound {
		t.Errorf("should return ErroOffsetNotFound, but got = %v", err)
	}

	if !reflect.DeepEqual(record, Record{}) {
		t.Errorf("should return nil, but got = %v", record)
	}
}
