package mw2ldb

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/syndtr/goleveldb/leveldb"
)

func dispatch(instance string, msg *sarama.ConsumerMessage) {
	appendLog(instance, msg)
	update(instance, msg)
}

/*
type ConsumerMessage struct {
	Headers        []*RecordHeader // only set if kafka is version 0.11+
	Timestamp      time.Time       // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp

	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}
*/
/*
 {
	"database": "hsn",
	"table": "user_info",
	"type": "update",
	"ts": 1568188765,
	"xid": 5224975,
	"commit": true,
	"position": "master.000006:36133212",
	"data": {
		"id": 134,
		"parend_id": 85,
		"direaction": "/82/85/134",
		"mobile": null,
		"password": "cc68e1f0e0a68ea38a4d9eac30013a58f50c5f07a8102187f8920292972202ce",
		"trade_password": null,
		"salt": "lkTabZVH88K78ZYRMRUK",
		"true_name": null,
		"nick_name": "HSN_6526660",
		"deposit_usdt_address": "14f3obDV7kxqQbVxtSz2GWTugWVGKT7LDv",
		"deposit_hsn_address": "0xdf191990cd899470bae63bf09e42fa282bddda1c",
		"balance_usdt": 0E-8,
		"balance_usdt_available": 0E-8,
		"balance_usdt_freeze": 0E-8,
		"balance_hsn": 0E-8,
		"balance_hsn_available": 0E-8,
		"balance_hsn_freeze": 0E-8,
		"points": 0E-8,
		"invite_code": "5406836",
		"id_card": null,
		"team_name": null,
		"is_active": "no",
		"partner_level": 0,
		"inviteReward_hsn": 0E-8,
		"inviteReward_usdt": 0E-8,
		"version": 0,
		"create_date": "2019-09-11 15:59:26",
		"update_date": "2019-09-11 15:59:26",
		"privilege_user": "no",
		"mail": "mcrypto@icloud.com",
		"protector_master_node": null,
		"gambling_status": null
	},
	"old": {
		"direaction": null
	}
}
*/

const WithoutMeta = true

func update(instance string, msg *sarama.ConsumerMessage) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(msg.Value, &m); err != nil {
		panic(err)
	}

	batch := new(leveldb.Batch)

	typ := string(m["type"])
	switch typ {
	case `"insert"`, `"update"`, `"bootstrap-insert"`:
		var bs []byte
		if WithoutMeta {
			bs, _ = m["data"]
		} else {
			var r struct {
				Data json.RawMessage            `json:"data"`
				Meta map[string]json.RawMessage `json:"meta"`
			}

			r.Data = m["data"]
			r.Meta = make(map[string]json.RawMessage, len(m)-1)
			for k, data := range m {
				if k != "data" {
					r.Meta[k] = data
				}
			}

			bs, _ = json.Marshal(r)
		}

		batch.Put(msg.Key, bs)
	case `"delete"`:
		batch.Delete(msg.Key)
	case `"bootstrap-start"`, `"bootstrap-complete"`, `"table-create"`:
	default:
		panic(string(msg.Value))
	}
	//
	//err := d.ldb.Write(batch, &opt.WriteOptions{Sync:true})
	//if err != nil {
	//	panic(err)
	//}
}

func update0(instance string, msg *sarama.ConsumerMessage) {
	// { "database": "db1", "table": "users", "pk.id": 123 }
	// [ "db1", "users", { "id": 123 } ]
	// Headers, Timestamp, BlockTimestamp, Key, Value, Topic, Partition, Offset

	var r struct {
		Type string `json:"Type"`
	}

	if err := json.Unmarshal(msg.Value, &r); err != nil {
		panic(err)
	}

	switch r.Type {
	case "insert":
		/*
			{
				"database": "hsn",
				"table": "addresses",
				"type": "insert",
				"ts": 1568118836,
				"xid": 4718685,
				"xoffset": 9997,
				"position": "master.000006:20022821",
				"data": {
					"id": 39227,
					"typ": "ETH",
					"address": "0x2409a2e9750228ffee1a2e782dc4f6ce05ba14bf",
					"userid": 0,
					"created": "2019-09-10 12:33:56",
					"modified": "2019-09-10 12:33:56"
				}
			}
		*/
	case "update":
		/*
			{
				"database": "hsn",
				"table": "user_recharge_payback_freeze_log",
				"type": "update",
				"ts": 1568174400,
				"xid": 5135435,
				"xoffset": 64,
				"position": "master.000006:32895537",
				"data": {
					"id": 36476,
					"uid": 127,
					"amount": 26.33800000,
					"coin_name": "HSN",
					"description": "payback",
					"status": null,
					"create_date": "2019-09-09 20:57:19",
					"update_date": "2019-09-11 12:00:00",
					"end_date": "2019-12-18 20:57:19"
				},
				"old": {
					"amount": 13.16900000,
					"update_date": "2019-09-10 12:00:00"
				}
			}
		*/
	case "delete":
		/*
			{
				"database": "hsn",
				"table": "addresses",
				"type": "delete",
				"ts": 1568117843,
				"xid": 4712015,
				"xoffset": 9582,
				"position": "master.000006:19175255",
				"data": {
					"id": 29227,
					"typ": "ETH",
					"address": "0x2409a2e9750228ffee1a2e782dc4f6ce05ba14bf",
					"userid": 0,
					"created": "2019-09-10 12:15:03",
					"modified": "2019-09-10 12:15:03"
				}
			}
		*/
	case "bootstrap-insert":
		/*
			{
				"database": "hsn",
				"table": "user_captcha",
				"type": "bootstrap-insert",
				"ts": 1567736302,
				"data": {
					"uuid": "9500167605",
					"code": "6be7y",
					"expire_time": "2019-09-05 20:19:38"
				}
			}
		*/
		//fmt.Println(string(msg.Value))
	case "bootstrap-start":
		/*
			{
				"database": "hsn",
				"table": "user_info",
				"type": "bootstrap-start",
				"ts": 1567736306,
				"data": {}
			}
		*/
	case "bootstrap-complete":
		/*
			{
				"database": "hsn",
				"table": "user_info",
				"type": "bootstrap-complete",
				"ts": 1567736309,
				"data": {}
			}
		*/
	case "table-create":
		/*
			{
				"type": "table-create",
				"database": "hsn",
				"table": "addresses",
				"def": {
					"database": "hsn",
					"charset": "utf8mb4",
					"table": "addresses",
					"primary-key": ["id"],
					"columns": [{
						"type": "int",
						"name": "id",
						"signed": true
					}, {
						"type": "char",
						"name": "typ",
						"charset": "utf8mb4"
					}, {
						"type": "char",
						"name": "address",
						"charset": "utf8mb4"
					}, {
						"type": "int",
						"name": "userid",
						"signed": true
					}, {
						"type": "timestamp",
						"name": "created",
						"column-length": 0
					}, {
						"type": "timestamp",
						"name": "modified",
						"column-length": 0
					}]
				},
				"ts": 1568115381000,
				"sql": "create table if not exists hsn.addresses (\n  `id`          int          NOT NULL PRIMARY KEY AUTO_INCREMENT,\n  `typ`         char(8)      NOT NULL DEFAULT '',         -- 地址类型, ETH/ETH-TOKEN/BTC/USDT...\n  `address`     char(64)     NOT NULL UNIQUE DEFAULT \"\",  -- 地址\n  `userid`      int          NOT NULL DEFAULT 0,          -- 绑定用户ID, 0为未绑定\n  `created`     timestamp    DEFAULT CURRENT_TIMESTAMP,\n  `modified`    timestamp    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
				"position": "master.000006:14599207"
			}
		*/
	default:
		fmt.Println(string(msg.Value))
	}
}

func appendLog(instance string, msg *sarama.ConsumerMessage) {

}

