package log

import (
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMaxStoreBytes = 1024
	defaultMaxIndexBytes = 1024
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = defaultMaxStoreBytes
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = defaultMaxIndexBytes
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	baseOffset := getBaseOffset(files)

	for i := 0; i < len(baseOffset); i++ {
		if err := l.newSegment(baseOffset[i]); err != nil {
			return nil, err
		}
		// baseOffset contains dup for index and store so we skip the dup
		i++ //TODO remove baseOffset dup
	}

	if l.segments == nil {
		if err := l.newSegment(c.Segment.InitialOffset); err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func getBaseOffset(files []os.FileInfo) []uint64 {
	var baseOffset []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffset = append(baseOffset, off)
	}

	sort.Slice(baseOffset, func(i, j int) bool {
		return baseOffset[i] < baseOffset[j]
	})

	return baseOffset
}
