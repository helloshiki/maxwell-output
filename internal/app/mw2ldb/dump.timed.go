package mw2ldb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	levelErrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
	"os"
	"path"
	"strings"
	"time"
)

const (
	RowPrefix = "row:" // row:hsn-user_daily_payback-2225
	LogPrefix = "log:" // log:hsn-user_daily_payback-20190912000052-64734
	KeyPrefix = "key:" // key:offset
)

const (
	TimestampFmt = "20060102150405"
	TimestampMin = "19700101000000"
	TimestampMax = "20501231235959"
)

type timedDump struct {
	logger *zap.Logger
	size   int
	cache  []*sarama.ConsumerMessage

	*config
	dir     string
	ldbName string
	ldb     *leveldb.DB

	tables map[string]struct{}
}

func (d *timedDump) loadLogTables() error {
	key := KeyPrefix + "logtables"
	value, err := d.ldb.Get([]byte(key), nil)
	if err != nil {
		if err == levelErrors.ErrNotFound {
			return nil
		}
	}

	var tables []string
	if err := json.Unmarshal(value, &tables); err != nil {
		return err
	}

	for _, s := range tables {
		d.tables[s] = struct{}{}
	}

	return nil
}

func (d *timedDump) Initialize() error {
	d.cache = make([]*sarama.ConsumerMessage, 0, d.size)

	file := path.Join(d.dir, d.ldbName)

	ldb, err := leveldb.OpenFile(file, nil)
	if _, corrupted := err.(*levelErrors.ErrCorrupted); corrupted {
		ldb, err = leveldb.RecoverFile(file, nil) // 如果有冲突，则需要修复文件
	}

	if err != nil {
		return err
	}

	d.ldb = ldb

	d.tables = make(map[string]struct{})
	if err := d.loadLogTables(); err != nil {
		return err
	}

	return nil
}

func (d *timedDump) LastOffset() (int64, error) {
	key := KeyPrefix + "offset"
	value, err := d.ldb.Get([]byte(key), &opt.ReadOptions{DontFillCache: true})
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = nil
		}

		return -1, err
	}

	offset := binary.BigEndian.Uint64(value)
	return int64(offset), nil
}

func (d *timedDump) Dump(msg *sarama.ConsumerMessage) error {
	if msg == nil {
		return d.flush()
	}

	d.cache = append(d.cache, msg)
	if len(d.cache) >= d.size {
		return d.flush()
	}

	return nil
}

func (d *timedDump) flush() error {
	if len(d.cache) == 0 {
		return nil
	}

	batch := new(leveldb.Batch)
	for _, msg := range d.cache {
		d.update(batch, msg)
		d.appendLog(batch, msg)
	}

	lastMsg := d.cache[len(d.cache)-1]

	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(lastMsg.Offset))
	batch.Put([]byte(KeyPrefix+"offset"), buf)

	if err := d.ldb.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}

	d.logger.Info(`save`, zap.Int(`count`, len(d.cache)), zap.Int64("offset", lastMsg.Offset))

	d.cache = make([]*sarama.ConsumerMessage, 0, d.size)
	return nil
}

func (d *timedDump) appendLog(batch *leveldb.Batch, msg *sarama.ConsumerMessage) {
	var r struct {
		Database string `json:"database"`
		Table    string `json:"table"`
		Ts       int64  `json:"ts"`
	}

	if err := json.Unmarshal(msg.Value, &r); err != nil {
		panic(err)
	}

	database, table := []byte(r.Database), []byte(r.Table)
	buf := &bytes.Buffer{}
	makeDBTable(buf, database, table)

	c := d.TableConfig(buf.String())
	if c.DisableRow {
		return
	}

	t := time.Unix(r.Ts, 0)

	buf.Reset()
	key := makeLogKey(buf, database, table, t.Format(TimestampFmt), msg.Offset)
	batch.Put(key.Bytes(), msg.Value)

	buf.Reset()
	makeDBTable(buf, database, table)
	if _, ok := d.tables[buf.String()]; ok {
		return
	}

	d.tables[buf.String()] = struct{}{}
	tables := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tables = append(tables, k)
	}

	bs, _ := json.Marshal(tables)
	batch.Put([]byte(KeyPrefix+"logtables"), bs)
}

func (d *timedDump) update(batch *leveldb.Batch, msg *sarama.ConsumerMessage) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		panic(err)
	}

	switch typ := string(raw["type"]); typ {
	case `"insert"`, `"update"`, `"bootstrap-insert"`, `"delete"`:
		var m map[string]json.RawMessage
		if err := json.Unmarshal(msg.Key, &m); err != nil {
			panic(err)
		}

		if len(m) != 3 {
			d.logger.Error(`ignore pk`, zap.Any("key", json.RawMessage(msg.Key)))
			return
		}

		var pkID []byte
		var database, table string

		json.Unmarshal(m["database"], &database)
		json.Unmarshal(m["table"], &table)

		for k, v := range m {
			if strings.HasPrefix(k, "pk.") {
				if bytes.HasPrefix(v, []byte("\"")) {
					pkID = v[1 : len(v)-1]
				} else {
					pkID = v
				}
				break
			}
		}

		if len(pkID) == 0 {
			panic(string(msg.Key))
		}

		c := d.DBConfig(string(database))
		if c.Disable {
			return
		}

		buf := &bytes.Buffer{}
		makeRowKey(buf, []byte(database), []byte(table), pkID)
		key := buf.Bytes()

		if typ == `"delete"` {
			batch.Delete([]byte(key))
			return
		}

		if d.config.WithoutMeta {
			batch.Put(key, raw["data"])
			return
		}

		var r struct {
			Data json.RawMessage            `json:"data"`
			Meta map[string]json.RawMessage `json:"meta"`
		}

		r.Data = raw["data"]
		r.Meta = make(map[string]json.RawMessage, len(m)-1)
		for k, data := range raw {
			if k != `data` {
				r.Meta[k] = data
			}
		}

		bs, _ := json.Marshal(r)
		batch.Put(key, bs)
	case `"bootstrap-start"`, `"bootstrap-complete"`, `"table-create"`:
	default:
		panic(string(msg.Value))
	}
}

func makeDBTable(buf *bytes.Buffer, database, table []byte) *bytes.Buffer {
	buf.Write(database)
	buf.Write([]byte("."))
	buf.Write(table)
	return buf
}

func makeRowKey(buf *bytes.Buffer, database, table, pkID []byte) *bytes.Buffer {
	buf.Write([]byte(RowPrefix))
	makeDBTable(buf, database, table)
	buf.Write([]byte("-"))
	if len(pkID) > 0 {
		buf.WriteString(fmt.Sprintf("%012s", pkID))
	}
	return buf
}

func makeLogKey(buf *bytes.Buffer, database, table []byte, timestamp string, offset int64) *bytes.Buffer {
	buf.Write([]byte(LogPrefix))
	makeDBTable(buf, database, table)
	buf.Write([]byte("-"))
	if len(timestamp) > 0 {
		buf.WriteString(timestamp)
		buf.WriteString(fmt.Sprintf("-%012d", offset))
	}
	return buf
}

func (d *timedDump) ClearLog() error {
	now := time.Now()
	startBuf, endBuf := &bytes.Buffer{}, &bytes.Buffer{}
	for table := range d.tables {
		c := d.TableConfig(table)
		lastTime := now.Add(-c.Retention).Format(TimestampFmt)

		startBuf.Reset()
		endBuf.Reset()

		makeLogKey(startBuf, nil, []byte(table), TimestampMin, 0)
		startTime := startBuf.Bytes()

		makeLogKey(endBuf, nil, []byte(table), lastTime, 0)
		endTime := endBuf.Bytes()

		batch := new(leveldb.Batch)
		iter := d.ldb.NewIterator(&util.Range{Start: startTime, Limit: endTime}, &opt.ReadOptions{DontFillCache: true})

		for iter.Next() {
			batch.Delete(iter.Key())
		}

		iter.Release()
		if err := iter.Error(); err != nil {
			return err
		}

		if batch.Len() == 0 {
			continue
		}

		d.logger.Warn(`del`, zap.String("table", table), zap.Int(`count`, batch.Len()), zap.String(`timestamp`, lastTime))
		if err := d.ldb.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
			return err
		}
	}

	tables := make([]string, 0, len(d.tables))
OUTER:
	for table := range d.tables {
		c := d.TableConfig(table)
		lastTime := now.Add(-c.Retention).Format(TimestampFmt)

		startBuf.Reset()
		endBuf.Reset()

		makeLogKey(startBuf, nil, []byte(table), TimestampMin, 0)
		makeLogKey(endBuf, nil, []byte(table), lastTime, 0)
		startTime, endTime := startBuf.Bytes(), endBuf.Bytes()

		iter := d.ldb.NewIterator(&util.Range{Start: startTime, Limit: endTime},
			&opt.ReadOptions{DontFillCache: true})
		for iter.Next() {
			iter.Release()
			tables = append(tables, table)
			continue OUTER
		}
	}

	if len(tables) == len(d.tables) {
		return nil
	}

	d.tables = make(map[string]struct{}, len(tables))
	for _, s := range tables {
		d.tables[s] = struct{}{}
	}

	bs, _ := json.Marshal(tables)
	key := KeyPrefix + "logtables"
	return d.ldb.Put([]byte(key), bs, &opt.WriteOptions{Sync: true})
}

func (d *timedDump) QueryLog(opts LogOpt) (interface{}, error) {
	if len(opts.BeginTime) == 0 {
		opts.BeginTime = TimestampMin
	}

	if len(opts.EndTime) == 0 {
		opts.EndTime = TimestampMax
	}

	if len(opts.BeginTime) != len(TimestampMin) || len(opts.EndTime) != len(TimestampMin) {
		return nil, errors.New(`bad beginTime/endTime`)
	}

	startBuf, endBuf := &bytes.Buffer{}, &bytes.Buffer{}
	makeLogKey(startBuf, []byte(opts.Database), []byte(opts.Table), opts.BeginTime, 0)
	makeLogKey(endBuf, []byte(opts.Database), []byte(opts.Table), opts.EndTime, 0)
	startTime, endTime := startBuf.Bytes(), endBuf.Bytes()

	type R struct {
		K string                     `json:"k"`
		V map[string]json.RawMessage `json:"v"`
	}

	if opts.Limit == 0 {
		opts.Limit = 100
	}

	var resArr []interface{}
	iter := d.ldb.NewIterator(&util.Range{
		Start: startTime,
		Limit: endTime,
	}, &opt.ReadOptions{DontFillCache: true})

	next := iter.Next
	if opts.Reverse {
		next = iter.Prev
		iter.Last()
		iter.Next()
	}

	skip, take := 0, 0
	for next() {
		skip++
		if skip <= opts.Offset {
			continue
		}

		take++
		key, value := string(iter.Key()), json.RawMessage(iter.Value())

		var r map[string]json.RawMessage
		_ = json.Unmarshal(value, &r)
		resArr = append(resArr, R{key, r})

		if take >= opts.Limit {
			break
		}
	}

	iter.Release()

	return resArr, iter.Error()
}

func (d *timedDump) QueryRow(opts RowOpt) ([]interface{}, error) {
	type R struct {
		K string                     `json:"k"`
		V map[string]json.RawMessage `json:"v"`
	}

	if opts.Limit == 0 {
		opts.Limit = 1
	}

	buf := &bytes.Buffer{}
	if len(opts.PkID) > 0 {
		if opts.Limit == 1 {
			makeRowKey(buf, []byte(opts.Database), []byte(opts.Table), []byte(opts.PkID))
			key := buf.Bytes()

			v, err := d.ldb.Get(key, &opt.ReadOptions{DontFillCache: true})
			if err != nil {
				return nil, err
			}

			var r map[string]json.RawMessage
			_ = json.Unmarshal(v, &r)
			return []interface{}{R{string(key), r}}, nil
		}
	}

	buf.Reset()
	makeRowKey(buf, []byte(opts.Database), []byte(opts.Table), nil)
	key := buf.Bytes()

	iter := d.ldb.NewIterator(util.BytesPrefix(key), nil)

	if len(opts.PkID) > 0 {
		buf.Reset()
		makeRowKey(buf, []byte(opts.Database), []byte(opts.Table), []byte(opts.PkID))
		iter.Seek(buf.Bytes())
		iter.Prev()
	}

	skip, take := 0, 0
	var resArr []interface{}
	for iter.Next() {
		skip++
		if skip <= opts.Offset {
			continue
		}

		take++
		key, value := string(iter.Key()), json.RawMessage(iter.Value())

		var r map[string]json.RawMessage
		_ = json.Unmarshal(value, &r)
		resArr = append(resArr, R{key, r})

		if take >= opts.Limit {
			break
		}
	}

	iter.Release()
	return resArr, iter.Error()
}

func (d *timedDump) ResetOffset(offset int) error {
	key := []byte(KeyPrefix + "offset")
	if offset <= 0 {
		return d.ldb.Delete(key, &opt.WriteOptions{Sync: true})
	}

	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	if err := d.ldb.Put(key, buf, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}

	d.logger.Warn(`exit normally`)

	os.Exit(0)
	return nil
}
