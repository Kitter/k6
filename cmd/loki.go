/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2020 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// TODO move this to it's own package
// benchmark it

type lokiHook struct {
	addr             string
	additionalParams [][2]string
	ch               chan *logrus.Entry
	limit            int
	levels           []logrus.Level
	pushPeriod       time.Duration
}

func lokiFromConfigLine(line string) (*lokiHook, error) {
	h := &lokiHook{
		addr:       "http://127.0.0.1:3100/loki/api/v1/push",
		limit:      100,
		levels:     logrus.AllLevels,
		pushPeriod: time.Second * 1, // TODO configurable,
	}
	if line == "loki" {
		return h, nil
	}

	parts := strings.SplitN(line, "=", 2)
	if parts[0] != "loki" {
		return nil, fmt.Errorf("loki configuration should be in the form `loki=url-to-push` but is `%s`", logOutput)
	}
	args := strings.Split(parts[1], ",")
	h.addr = args[0]
	// TODO use something better ... maybe
	// https://godoc.org/github.com/kubernetes/helm/pkg/strvals
	// atleast until https://github.com/loadimpact/k6/issues/926?
	if len(args) == 1 {
		return h, nil
	}

	for _, arg := range args[1:] {
		paramParts := strings.SplitN(arg, "=", 2)

		if len(paramParts) != 2 {
			return nil, fmt.Errorf("loki arguments should be in the form `address,key1=value1,key2=value2`, got %s", arg)
		}

		key := paramParts[0]
		value := paramParts[1]
		switch key {
		case "additionalParams":
			values := strings.Split(value, ";") // ; because , is already used

			h.additionalParams = make([][2]string, len(values))
			for i, value := range values {
				additionalParamParts := strings.SplitN(value, "=", 2)
				if len(additionalParamParts) != 2 {
					return nil, fmt.Errorf("additionalparam should be in the form key1=value1;key2=value2, got %s", value)
				}
				h.additionalParams[i] = [2]string{additionalParamParts[0], additionalParamParts[1]}
			}
		case "limit":
			var err error
			h.limit, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("couldn't parse the loki limit as a number %w", err)
			}
		case "level":
			// TODO figure out if `tracing`,`fatal` and `panic` should be included
			h.levels = []logrus.Level{}
			switch value {
			case "debug":
				h.levels = append(h.levels, logrus.DebugLevel)
				fallthrough
			case "info":
				h.levels = append(h.levels, logrus.InfoLevel)
				fallthrough
			case "warning":
				h.levels = append(h.levels, logrus.WarnLevel)
				fallthrough
			case "error":
				h.levels = append(h.levels, logrus.ErrorLevel)
			default:
				return nil, fmt.Errorf("unknown log level %s", value)
			}
		default:
			return nil, fmt.Errorf("unknown loki config key %s", key)
		}
	}

	return h, nil
}

func (h *lokiHook) start() error {
	h.ch = make(chan *logrus.Entry, 1000)
	go h.loop()

	return nil
}

// fill one of two equally sized slices with entries and then push it while filling the other one
// TODO clean old entries after push?
func (h *lokiHook) loop() {
	var (
		msgs       = make([]tmpMsg, h.limit)
		msgsToPush = make([]tmpMsg, h.limit)
		dropped    int
		count      int
		ticker     = time.NewTicker(h.pushPeriod)
		pushCh     = make(chan chan struct{})
	)

	defer close(pushCh)

	go func() {
		oldLogs := make([]tmpMsg, 0, h.limit*2)
		for ch := range pushCh {
			msgsToPush, msgs = msgs, msgsToPush
			oldCount, oldDropped := count, dropped
			count, dropped = 0, 0
			close(ch) // signal that more buffering can continue

			copy(oldLogs[len(oldLogs):len(oldLogs)+oldCount], msgsToPush[:oldCount])
			oldLogs = oldLogs[:len(oldLogs)+oldCount]
			n, err := h.push(oldLogs, oldDropped)
			_ = err // TODO print it on terminal ?!?
			if n > len(oldLogs) {
				oldLogs = oldLogs[:0]
				continue
			}
			oldLogs = oldLogs[:copy(oldLogs, oldLogs[n:])]
		}
	}()

	for {
		select {
		case entry, ok := <-h.ch:
			if !ok {
				return
			}
			if count == h.limit {
				dropped++
				continue
			}
			labels := make(map[string]string, len(entry.Data)+1)
			for k, v := range entry.Data {
				// TODO filter the additional ones
				labels[k] = fmt.Sprint(v) // TODO optimize ?
			}
			for _, params := range h.additionalParams {
				labels[params[0]] = params[1]
			}
			labels["level"] = entry.Level.String()
			// TODO have the cutoff here ?
			msgs[count] = tmpMsg{
				labels: labels,
				msg:    entry.Message,
				t:      entry.Time.UnixNano(),
			}
			count++
		case <-ticker.C:
			ch := make(chan struct{})
			pushCh <- ch
			<-ch
		}
	}
}

func (h *lokiHook) push(msgs []tmpMsg, dropped int) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	// TODO using time.Before was giving a lot of out of order, but even now, there are some, if the
	// limit is big enough ...
	sort.Slice(msgs, func(i, j int) bool {
		return msgs[i].t < msgs[j].t
	})

	// We can technically cutoff during the addMsg phase, which will be even better if we sort after
	// it as well ... maybe
	cutOff := time.Now().Add(-time.Millisecond * 500).UnixNano() // probably better to be configurable

	cutOffPoint := sort.Search(len(msgs), func(i int) bool {
		return !(msgs[i].t < cutOff)
	})

	if cutOffPoint > len(msgs) {
		cutOffPoint = len(msgs)
	}

	strms := new(lokiPushMessage)

	for _, msg := range msgs[:cutOffPoint] {
		strms.add(msg)
	}
	if dropped != 0 {
		labels := make(map[string]string, 2+len(h.additionalParams))
		labels["level"] = logrus.WarnLevel.String()
		labels["dropped"] = strconv.Itoa(dropped)
		for _, params := range h.additionalParams {
			labels[params[0]] = params[1]
		}

		msg := tmpMsg{
			labels: labels,
			msg: fmt.Sprintf("k6 dropped some packages because they were above the limit of %d/%s",
				h.limit, h.pushPeriod),
			t: msgs[cutOffPoint-1].t,
		}
		strms.add(msg)
	}

	var b bytes.Buffer
	_, err := strms.WriteTo(&b)
	if err != nil {
		return cutOffPoint, err
	}
	// TODO use a custom client
	res, err := http.Post(h.addr, "application/json", &b) //nolint:noctx
	if res != nil {
		_, _ = io.Copy(ioutil.Discard, res.Body)
		_ = res.Body.Close()
	}
	return cutOffPoint, err
}

func mapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if v2, ok := b[k]; !ok || v2 != v {
			return false
		}
	}
	return true
}

func (strms *lokiPushMessage) add(entry tmpMsg) {
	var foundStrm *stream
	for _, strm := range strms.Streams {
		if mapEqual(strm.Stream, entry.labels) {
			foundStrm = strm
			break
		}
	}

	if foundStrm == nil {
		foundStrm = &stream{Stream: entry.labels}
		strms.Streams = append(strms.Streams, foundStrm)
	}

	foundStrm.Values = append(foundStrm.Values, logEntry{t: entry.t, msg: entry.msg})
}

// this is temporary message format
type tmpMsg struct {
	labels map[string]string
	t      int64
	msg    string
}

func (h *lokiHook) Fire(entry *logrus.Entry) error {
	h.ch <- entry
	return nil
}

func (h *lokiHook) Levels() []logrus.Level {
	return h.levels
}

/*
{
  "streams": [
    {
      "stream": {
        "label": "value"
      },
      "values": [
          [ "<unix epoch in nanoseconds>", "<log line>" ],
          [ "<unix epoch in nanoseconds>", "<log line>" ]
      ]
    }
  ]
}
*/
type lokiPushMessage struct {
	Streams []*stream `json:"streams"`
}

func (strms *lokiPushMessage) WriteTo(w io.Writer) (n int64, err error) {
	var k int
	write := func(b []byte) {
		if err != nil {
			return
		}
		k, err = w.Write(b)
		n += int64(k)
	}
	// 10+ 9 for the amount of nanoseconds between 2001 and 2286 also it overflows in the year 2262 ;)
	var nanoseconds [19]byte
	write([]byte(`{"streams":[`))
	for i, str := range strms.Streams {
		if i != 0 {
			write([]byte(`,`))
		}
		write([]byte(`{"stream":{`))
		var f bool
		for k, v := range str.Stream {
			if f {
				write([]byte(`,`))
			}
			f = true
			write([]byte(`"`))
			write([]byte(k))
			write([]byte(`":"`))
			write([]byte(v))
			write([]byte(`"`))
		}
		write([]byte(`},"values":[`))
		for j, v := range str.Values {
			if j != 0 {
				write([]byte(`,`))
			}
			write([]byte(`["`))
			strconv.AppendInt(nanoseconds[:0], v.t, 10)
			write(nanoseconds[:])
			write([]byte(`","`))
			write([]byte(v.msg))
			write([]byte(`"]`))
		}
		write([]byte(`]}`))
	}

	write([]byte(`]}`))

	return n, err
}

type stream struct {
	Stream map[string]string `json:"stream"`
	Values []logEntry        `json:"values"`
}

type logEntry struct {
	t   int64  // nanoseconds
	msg string // maybe intern those as they are likely to be the same for an interval
}

// rewrite this either with easyjson or with a custom marshalling
func (l logEntry) MarshalJSON() ([]byte, error) {
	// 2 for '[]', 1 for ',', 4 for '"' and 10 + 9 for the amount of nanoseconds between 2001 and
	// 2286 also it overflows in the year 2262 ;)
	b := make([]byte, 2, len(l.msg)+26)
	b[0] = '['
	b[1] = '"'
	b = strconv.AppendInt(b, l.t, 10)
	b = append(b, '"', ',', '"')
	b = append(b, l.msg...)
	b = append(b, '"', ']')
	return b, nil
}
