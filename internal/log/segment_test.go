package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/nikp359/proglog/api/v1"
	req "github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	require := req.New(t)

	dir, err := ioutil.TempDir("", "segment-test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Log(err.Error())
		}
	}()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(err)
	require.Equal(uint64(16), s.nextOffset, s.nextOffset)
	require.False(s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		errMessage := fmt.Sprintf("record %v", i)

		off, err := s.Append(want)
		require.NoError(err)
		require.Equal(16+i, off, errMessage)

		got, err := s.Read(off)
		require.NoError(err, errMessage)
		require.Equal(want, got, errMessage)
	}

	_, err = s.Append(want)
	require.Equal(io.EOF, err)

	// maxed index
	require.True(s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(err)
	// maxed store
	require.True(s.IsMaxed())

	err = s.Remove()
	require.NoError(err)
	s, err = newSegment(dir, 16, c)
	require.NoError(err)
	require.False(s.IsMaxed())
}
