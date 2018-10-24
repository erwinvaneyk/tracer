package tracer

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TraceEntry struct {
	Timestamp time.Duration `json:"timestamp"` // timestamp relative to the start of the trace in nanoseconds.
	Payload   interface{}   `json:"payload"`
}

type Trace []TraceEntry

type EntryParser func(d []byte) (TraceEntry, error)

func (t Trace) Len() int           { return len(t) }
func (t Trace) Less(i, j int) bool { return t[i].Timestamp < t[j].Timestamp }
func (t Trace) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

func Start(ctx context.Context, trace Trace) <-chan TraceEntry {
	sort.Sort(trace)
	c := make(chan TraceEntry)
	startTime := time.Now()
	go func() {
		for _, entry := range trace {
			wait := startTime.Add(entry.Timestamp).Sub(time.Now())
			time.Sleep(wait)
			select {
			case c <- entry:
				// ok
			case <-ctx.Done():
				break
			}
			if ctx.Err() != nil {
				break
			}
		}
		close(c)
	}()
	return c
}

var JSONEntryParser EntryParser = func(d []byte) (TraceEntry, error) {
	var target TraceEntry
	err := json.Unmarshal(d, &target)
	return target, err
}

var CSVEntryParser EntryParser = func(d []byte) (TraceEntry, error) {
	s := string(d)
	ps := strings.SplitN(s, ";", 2)
	ts, err := strconv.ParseInt(ps[0], 10, 64)
	if err != nil {
		return TraceEntry{}, err
	}
	var payload interface{}
	if len(ps) > 1 {
		payload = ps[1]
	}
	return TraceEntry{
		Timestamp: time.Duration(ts),
		Payload:   payload,
	}, nil
}

func Load(src io.Reader, entryParser EntryParser) (Trace, error) {
	var trace Trace
	sc := bufio.NewScanner(src)
	for sc.Scan() {
		entry, err := entryParser(sc.Bytes())
		if err != nil {
			return nil, err
		}
		trace = append(trace, entry)
	}
	return trace, nil
}
