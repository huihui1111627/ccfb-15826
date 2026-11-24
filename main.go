// Command runtimefilter runs the runtime-filter verification suite and
// serves the effect-analysis dashboard.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	"runtimefilter/gen"
	"runtimefilter/rf"
	"runtimefilter/verify"
	"runtimefilter/web"
)

func main() {
	var (
		seed     = flag.Int64("seed", 20261124, "固定随机种子")
		addr     = flag.String("http", "127.0.0.1:8080", "面板监听地址")
		noServe  = flag.Bool("no-serve", false, "只跑校验不启动网页")
		outJSON  = flag.String("json", "", "把报告写入指定 JSON 文件")
		maxBytes = flag.Int("max-filter-bytes", 0, "覆盖过滤器尺寸上限（字节），0 用默认值")
		only     = flag.Int("only", 0, "只运行指定编号的场景")
	)
	flag.Parse()

	cfg := rf.DefaultConfig()
	if *maxBytes > 0 {
		cfg.MaxBytes = *maxBytes
	}
	opt := verify.Options{
		Cfg: cfg,
		ScenarioCfg: func(s *gen.Scenario, base rf.Config) rf.Config {
			c := base
			// Scenario 18 explicitly exercises the degradation policy:
			// shrink its budget below the exact-set footprint.
			for _, t := range s.Tags {
				if t == "degrade" {
					c.MaxBytes = 4096
				}
			}
			return c
		},
	}

	var rep verify.SuiteReport
	if *only > 0 {
		for _, s := range gen.NewSet(*seed) {
			if s.ID == *only {
				r := verify.RunOne(s, opt)
				rep = verify.SuiteReport{Seed: *seed, Total: 1}
				rep.Scenarios = []verify.ScenarioReport{r}
				if r.Passed {
					rep.Passed = 1
				} else {
					rep.Failed = 1
				}
			}
		}
	} else {
		rep = verify.RunAll(*seed, opt)
	}

	printSummary(rep)
	if *outJSON != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*outJSON, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write json:", err)
			os.Exit(1)
		}
		fmt.Println("report written to", *outJSON)
	}
	if rep.Failed > 0 {
		if *noServe {
			os.Exit(1)
		}
	}
	if *noServe {
		if rep.Failed > 0 {
			os.Exit(1)
		}
		return
	}

	srv := &web.Server{Report: rep}
	fmt.Printf("dashboard: http://%s/  (Ctrl-C 退出)\n", *addr)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		fmt.Fprintln(os.Stderr, "http:", err)
		os.Exit(1)
	}
}

func printSummary(rep verify.SuiteReport) {
	fmt.Printf("seed=%d  场景 %d  通过 %d  失败 %d\n", rep.Seed, rep.Total, rep.Passed, rep.Failed)
	for _, s := range rep.Scenarios {
		mark := "PASS"
		if !s.Passed {
			mark = "FAIL"
		}
		line := fmt.Sprintf("  [%s] %02d %-46s rows=%-7d noRF=%8.3fms RF=%8.3fms NL=%8.3fms",
			mark, s.ID, s.Name, s.RowCount, s.NoRF.ElapsedMS, s.RF.ElapsedMS, s.NL.ElapsedMS)
		if len(s.Filters) > 0 {
			f := s.Filters[0]
			line += fmt.Sprintf(" | %s size=%s reject=%d/%d skip=%d块",
				f.Mode, humanBytes(f.SizeBytes), f.RowsRejected, f.RowsTested, f.BlocksSkipped)
		}
		fmt.Println(line)
		if !s.Passed {
			fmt.Println("        ->", s.Error)
		}
	}
}

func humanBytes(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
