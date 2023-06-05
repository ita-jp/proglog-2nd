package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// 既存のエントリを超えて読み出す場合、インデックスはエラーを返す
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// インデックスは、既存のファイルからその状態を構築する
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}

func TestIndex_returnEOFWhenIndexIsEmpty(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	out, pos, err := idx.Read(0)
	require.Equal(t, uint32(0), out)
	require.Equal(t, uint64(0), pos)
	require.Equal(t, io.EOF, err)
}

func TestIndex_returnPositionWhenIndexHasEntriesAndArgumentIsMinusOne(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	err = idx.Write(uint32(0), uint64(2))
	require.NoError(t, err)

	out, pos, err := idx.Read(-1)
	require.Equal(t, uint32(0), out)
	require.Equal(t, uint64(2), pos)
	require.NoError(t, err)
}

func TestIndex_returnPositionWhenIndexHasEntriesAndArgumentIsZero(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	err = idx.Write(uint32(0), uint64(2))
	require.NoError(t, err)

	out, pos, err := idx.Read(0)
	require.Equal(t, uint32(0), out)
	require.Equal(t, uint64(2), pos)
	require.NoError(t, err)
}

func TestIndex_returnEOFWhenGivenOffsetIsOutOfRange(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	err = idx.Write(uint32(0), uint64(2))
	require.NoError(t, err)

	out, pos, err := idx.Read(1)
	require.Equal(t, uint32(0), out)
	require.Equal(t, uint64(0), pos)
	require.Error(t, io.EOF, err)
}

func TestIndex_returnPositionWhenIndexHasTwoEntries(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Errorf("got error = %v", err)
		}
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	err = idx.Write(uint32(0), uint64(2))
	require.NoError(t, err)
	err = idx.Write(uint32(1), uint64(999))
	require.NoError(t, err)

	out, pos, err := idx.Read(1)
	require.Equal(t, uint32(1), out)
	require.Equal(t, uint64(999), pos)
	require.NoError(t, err)
}
