package mw2ldb

import (
	"encoding/json"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"os"
)

func (c *Server) startHttpServer() {
	addr := c.ListenAddr
	if len(addr) == 0 {
		return
	}

	logger := c.logger.With(zap.String("sub", "http"))
	m := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		logger.Sugar().Infof(`path: %s, args: %s`, path, ctx.QueryArgs().String())
		switch path {
		case "/log":
			args := ctx.QueryArgs()

			var opt LogOpt
			opt.Database, opt.Table = string(args.Peek(`db`)), string(args.Peek(`tb`))
			if len(opt.Database) == 0 || len(opt.Table) == 0 {
				ctx.Error("bad database/table\n", 500)
				return
			}

			opt.BeginTime, opt.EndTime = string(args.Peek(`start`)), string(args.Peek(`end`))
			if (len(opt.BeginTime) > 0 && len(opt.BeginTime) != len(TimestampMin)) ||
				(len(opt.EndTime) > 0 && len(opt.EndTime) != len(TimestampMin)) {
				ctx.Error("bad start/end\n", 500)
				return
			}

			opt.Reverse = args.GetBool(`reverse`)

			var err error
			opt.Offset, err = args.GetUint(`offset`)
			if err != nil && err != fasthttp.ErrNoArgValue {
				ctx.Error("bad offset\n", 500)
				return
			}

			opt.Limit, err = args.GetUint(`limit`)
			if err != nil && err != fasthttp.ErrNoArgValue {
				ctx.Error("bad limit\n", 500)
				return
			}

			if opt.Limit < 0 {
				opt.Limit = 0
			}

			if opt.Offset < 0 {
				opt.Offset = 0
			}

			resArr, err := c.dumper.QueryLog(opt)
			if err != nil {
				ctx.Error(err.Error()+"\n", 500)
				return
			}

			bs, _ := json.Marshal(resArr)
			_, _ = ctx.WriteString(string(bs))
			_, _ = ctx.WriteString("\n")
		case "/row":
			args := ctx.QueryArgs()

			var opt RowOpt
			opt.Database, opt.Table = string(args.Peek(`db`)), string(args.Peek(`tb`))
			if len(opt.Database) == 0 || len(opt.Table) == 0 {
				ctx.Error("bad database/table\n", 500)
				return
			}

			opt.PkID = string(args.Peek(`pk`))

			var err error
			opt.Offset, err = args.GetUint(`offset`)
			if err != nil && err != fasthttp.ErrNoArgValue {
				ctx.Error("bad offset\n", 500)
				return
			}

			opt.Limit, err = args.GetUint(`limit`)
			if err != nil && err != fasthttp.ErrNoArgValue {
				ctx.Error("bad limit\n", 500)
				return
			}

			if opt.Limit < 0 {
				opt.Limit = 0
			}

			if opt.Offset < 0 {
				opt.Offset = 0
			}

			resArr, err := c.dumper.QueryRow(opt)
			if err != nil {
				ctx.Error(err.Error()+"\n", 500)
				return
			}


			bs, _ := json.Marshal(resArr)
			_, _ = ctx.WriteString(string(bs))
			_, _ = ctx.WriteString("\n")
		case "/del":

			_, _ = ctx.WriteString("done")
		default:
			ctx.Error("bad path: "+path, 500)
		}
	}

	logger.Sugar().Infof(`listen %s`, addr)
	if err := fasthttp.ListenAndServe(addr, m); err != nil {
		logger.Fatal(`listen fail`, zap.String("e", err.Error()))
		os.Exit(1)
	}
}
