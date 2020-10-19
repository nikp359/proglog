package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("Hello world dude")
	width = uint64(len(write)) + lenWidth
)

func TestStore(t *testing.T) {
	assert := require.New(t)

	f, err := ioutil.TempFile("", "store_test")
	assert.NoError(err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	assert.NoError(err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	for i := uint64(1); i < 10; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)

		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	var pos uint64

	for i := uint64(1); i < 10; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)

		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	off := int64(0)
	for i := uint64(1); i < 10; i++ {
		b := make([]byte, lenWidth)

		num, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, num)
		off += int64(num)

		size := enc.Uint64(b)
		b = make([]byte, size)
		num, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), num)
		off += int64(num)
	}
}
