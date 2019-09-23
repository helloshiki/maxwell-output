package mw2es

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/olivere/elastic"
	"go.uber.org/zap"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	TimestampFmt = "20060102150405"
)

type timedDump struct {
	logger *zap.Logger
	size   int
	cache  []*sarama.ConsumerMessage

	*config
}

func (d *timedDump) Initialize() error {
	d.cache = make([]*sarama.ConsumerMessage, 0, d.size)
	return nil
}

func (d *timedDump) LastOffset() (int64, error) {
	client, err := elastic.NewClient()
	if err != nil {
		return -1, err
	}

	ok, err := client.IndexExists(d.Prefix).Do(context.Background())
	if err != nil {
		return -1, err
	}

	if !ok {
		return -1, nil
	}

	resp, err := client.Get().Index(d.Prefix).Id("offset").Do(context.Background())
	if err != nil {
		if elastic.IsNotFound(err) {
			return -1, nil
		}
		return -1, err
	}

	var r struct {
		Data string `json:"data"`
	}

	if err := json.Unmarshal(*resp.Source, &r); err != nil {
		return -1, err
	}

	n, err := strconv.ParseUint(r.Data, 10, 64)
	return int64(n), err
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

	// {"database":"hsn","table":"user_info","pk.id":177} {"database":"hsn","table":"user_info","type":"update","ts":1568760500,"xid":9268369,"commit":true,"position":"master.000009:9275773","data":{"id":177,"parend_id":167,"direaction":"/83/167/177","mobile":null,"password":"6fdcb73f9875fbabc053219631550170bb69507dde4c2b75d4201a9c6ea366ce","trade_password":null,"salt":"vCnGWQ3HYeksE4n0CXwf","true_name":null,"nick_name":"HSN_2644517","deposit_usdt_address":"19YDXxdSzX8vgYRhzHqE25VLsNocxW2k8U","deposit_hsn_address":"0xabedbeb4fd032fd9fabb304902ec8ff88e9191e5","balance_usdt":0E-8,"balance_usdt_available":0E-8,"balance_usdt_freeze":0E-8,"balance_hsn":0E-8,"balance_hsn_available":0E-8,"balance_hsn_freeze":0E-8,"points":0E-8,"invite_code":"9794246","id_card":null,"team_name":null,"is_active":"no","partner_level":0,"inviteReward_hsn":0E-8,"inviteReward_usdt":0E-8,"version":0,"create_date":"2019-09-18 06:48:20","update_date":"2019-09-18 06:48:20","privilege_user":"no","mail":"Lola.blair62@gmail.com","protector_master_node":null,"gambling_status":null},"old":{"direaction":null,"deposit_usdt_address":null,"deposit_hsn_address":null}}
	cacheSize := len(d.cache)
	lastMsg := d.cache[cacheSize-1]

	start := time.Now()
	defer func() {
		d.logger.Info(`save`, zap.Int(`count`, cacheSize),
			zap.Int64("offset", lastMsg.Offset), zap.Duration("cost", time.Since(start)))
	}()

	client, err := elastic.NewClient()
	if err != nil {
		return err
	}

	bulk := client.Bulk()
	for _, msg := range d.cache {
		d.update(bulk, msg)
		d.appendLog(bulk, msg)
	}

	resp, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}

	if resp.Errors {
		for _, item := range resp.Failed() {
			d.logger.Error(`bulk`, zap.String("id", item.Id), zap.String("e", item.Error.Reason))
		}
		return fmt.Errorf("bulk fail")
	}

	d.cache = make([]*sarama.ConsumerMessage, 0, d.size)
	{
		bs, _ := json.Marshal(lastMsg.Offset)
		resp, err := client.Index().
			Index(d.Prefix).
			Type("_doc").
			Id(`offset`).
			BodyJson(map[string]interface{}{"data": string(bs)}).
			Do(context.Background())
		if err != nil {
			return err
		}

		if resp.Status >= 300 {
			panic(`logical error`)
		}
	}

	return nil
}

func (d *timedDump) appendLog(bulk *elastic.BulkService, msg *sarama.ConsumerMessage) {
	var r struct {
		Database string `json:"database"`
		Table    string `json:"table"`
		Ts       int64  `json:"ts"`
	}

	if err := json.Unmarshal(msg.Value, &r); err != nil {
		panic(err)
	}

	c := d.TableConfig(makeDBTable(r.Database, r.Table))
	if c.DisableRow {
		return
	}

	t := time.Unix(r.Ts, 0)

	key := makeLogKey(r.Database, r.Table, t.Format(TimestampFmt), msg.Offset)

	var value map[string]interface{}
	if err := json.Unmarshal(msg.Value, &value); err != nil {
		panic(err)
	}

	if msg.Key[0] != '{' {
		d.logger.Warn(`skip`, zap.String("key", string(msg.Key)), zap.String("value", string(msg.Value)))
		return
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(msg.Key, &m); err != nil {
		panic(err)
	}
	value["key"] = makeID(m)

	bs, _ := json.Marshal(value["data"])
	value["data"] = string(bs)
	if v, ok := value["old"]; ok {
		bs, _ = json.Marshal(v)
		value["old"] = string(bs)
	}

	req := elastic.NewBulkIndexRequest().Index(d.Prefix + "_log").Type("_doc").Id(key).Doc(value)
	bulk.Add(req)
}

func indexName(prefix, database, table string) string {
	return fmt.Sprintf("%s-%s-%s", prefix, database, table)
}

func makeID(m map[string]json.RawMessage) string {
	var database, table string

	_ = json.Unmarshal(m["database"], &database)
	_ = json.Unmarshal(m["table"], &table)

	type T struct {
		k string
		v json.RawMessage
	}

	varr := make([]T, 0, len(m)-2)
	for k, v := range m {
		switch k {
		case "database", "table":
		default:
			if v[0] != '"' {
				varr = append(varr, T{k, v})
				continue
			}
			varr = append(varr, T{k, v[1 : len(v)-1]})
		}
	}

	if len(varr) > 1 {
		sort.Slice(varr, func(i, j int) bool {
			return varr[i].k < varr[j].k
		})
	}

	var builder strings.Builder
	builder.WriteString(database)
	builder.WriteByte('#')
	builder.WriteString(table)
	builder.WriteByte('#')
	for i, item := range varr {
		builder.Write(item.v)
		if i < len(varr)-1 {
			builder.WriteByte('#')
		}
	}
	return builder.String()
}

func flatten(raw map[string]json.RawMessage, field string) error {
	value, ok := raw[field]
	if !ok {
		return nil
	}

	var m map[string]json.RawMessage
	err := json.Unmarshal(value, &m)
	if err != nil {
		return err
	}

	delete(raw, field)
	for k, v := range m {
		raw[fmt.Sprintf("%s.%s", field, k)] = v
	}

	return nil
}

func (d *timedDump) update(bulk *elastic.BulkService, msg *sarama.ConsumerMessage) {
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

		var database, table string
		_ = json.Unmarshal(m["table"], &table)
		_ = json.Unmarshal(m["database"], &database)

		c := d.DBConfig(database)
		if c.Disable {
			return
		}

		index := indexName(d.Prefix, database, table)
		key := makeID(m)
		if typ == `"delete"` {
			req := elastic.NewBulkDeleteRequest().Index(index).Type("_doc").Id(key)
			bulk.Add(req)
			return
		}

		var doc struct {
			Data json.RawMessage            `json:"data"`
			Old json.RawMessage            `json:"old,omitempty"`
			Meta map[string]json.RawMessage `json:"meta,omitempty"`
		}

		doc.Data = raw["data"]
		if v, ok := raw["old"]; ok {
			doc.Old = v
		}

		if d.config.WithoutMeta {
			req := elastic.NewBulkIndexRequest().Index(index).Type("_doc").Id(key).Doc(doc)
			bulk.Add(req)
			return
		}

		doc.Meta = make(map[string]json.RawMessage, len(m)-1)
		for k, data := range raw {
			if k != `data` && k != `old` {
				doc.Meta[k] = data
			}
		}

		req := elastic.NewBulkIndexRequest().Index(index).Type("_doc").Id(key).Doc(doc)
		bulk.Add(req)
	case `"bootstrap-start"`, `"bootstrap-complete"`, `"table-create"`:
	default:
		panic(string(msg.Value))
	}
}

func makeDBTable(database, table string) string {
	return strings.ToLower(fmt.Sprintf("%s.%s", database, table))
}

func makeLogKey(database, table, timestamp string, offset int64) string {
	var builder strings.Builder 
	builder.WriteString(database)
	builder.WriteByte('#')
	builder.WriteString(table)
	builder.WriteByte('#')
	if len(timestamp) > 0 {
		builder.WriteString(timestamp)
		builder.WriteString(fmt.Sprintf("#%d", offset))
	}
	return builder.String()
}

func (d *timedDump) ClearLog() error {
	return nil
}
