package cmds

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/resonateio/resonate-on-scylladb/internal/base"
	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/resonateio/resonate-on-scylladb/internal/dbms"
	"github.com/resonateio/resonate-on-scylladb/internal/loop"
	"github.com/resonateio/resonate-on-scylladb/internal/netw"
	"github.com/spf13/cobra"
)

func ServeCmd() *cobra.Command {
	var (
		addr           string
		scyllaHost     string
		scyllaPort     int
		bucketWidthMs  int64
		bucketLookback int
		shards         int
		workerTTL      int
		debug          bool
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("scylladb-host") {
				if h := os.Getenv("SCYLLADB_HOST"); h != "" {
					scyllaHost = h
				}
			}
			if !cmd.Flags().Changed("scylladb-port") {
				if p := os.Getenv("SCYLLADB_PORT"); p != "" {
					n, err := strconv.Atoi(p)
					if err != nil {
						return fmt.Errorf("invalid SCYLLADB_PORT %q: %w", p, err)
					}
					scyllaPort = n
				}
			}
			if !cmd.Flags().Changed("bucket-width-ms") {
				if v := os.Getenv("BUCKET_WIDTH_MS"); v != "" {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid BUCKET_WIDTH_MS %q: %w", v, err)
					}
					bucketWidthMs = n
				}
			}
			if bucketWidthMs <= 0 {
				return fmt.Errorf("bucket-width-ms must be > 0, got %d", bucketWidthMs)
			}
			if !cmd.Flags().Changed("bucket-lookback") {
				if v := os.Getenv("BUCKET_LOOKBACK"); v != "" {
					n, err := strconv.Atoi(v)
					if err != nil {
						return fmt.Errorf("invalid BUCKET_LOOKBACK %q: %w", v, err)
					}
					bucketLookback = n
				}
			}
			if bucketLookback < 0 {
				return fmt.Errorf("bucket-lookback must be >= 0, got %d", bucketLookback)
			}
			if !cmd.Flags().Changed("shards") {
				if v := os.Getenv("SHARDS"); v != "" {
					n, err := strconv.Atoi(v)
					if err != nil {
						return fmt.Errorf("invalid SHARDS %q: %w", v, err)
					}
					shards = n
				}
			}
			if shards <= 0 {
				shards = 1
			}
			if !cmd.Flags().Changed("worker-ttl") {
				if v := os.Getenv("WORKER_TTL"); v != "" {
					n, err := strconv.Atoi(v)
					if err != nil {
						return fmt.Errorf("invalid WORKER_TTL %q: %w", v, err)
					}
					workerTTL = n
				}
			}
			if workerTTL <= 0 {
				workerTTL = 30_000
			}

			cfg := dbms.Config{
				Hosts:        scyllaHosts(scyllaHost, scyllaPort),
				Port:         scyllaPort,
				Username:     os.Getenv("SCYLLADB_USERNAME"),
				Password:     os.Getenv("SCYLLADB_PASSWORD"),
				TLS:          os.Getenv("SCYLLADB_TLS") == "1" || strings.EqualFold(os.Getenv("SCYLLADB_TLS"), "true"),
				TLSInsecure:  os.Getenv("SCYLLADB_TLS_INSECURE") == "1" || strings.EqualFold(os.Getenv("SCYLLADB_TLS_INSECURE"), "true"),
				Keyspace:     os.Getenv("SCYLLADB_KEYSPACE"),
				CreateSchema: debug,
				Replication:  os.Getenv("SCYLLADB_REPLICATION"),
			}

			session, err := dbms.Connect(cfg)
			if err != nil {
				return fmt.Errorf("schema: %w", err)
			}
			defer session.Close()

			workerID, err := gocql.RandomUUID()
			if err != nil {
				return fmt.Errorf("generate worker ID: %w", err)
			}

			h := &core.Handler{
				Session:        session,
				Host:           strings.Join(cfg.Hosts, ","),
				BucketWidth:    bucketWidthMs,
				BucketLookback: bucketLookback,
				Shards:         shards,
				Debug:          debug,
			}

			push := &netw.HttpPush{}
			poll := &netw.HttpPoll{}
			dispatcher := base.NewDispatcher()
			dispatcher.Register(push, "http", "https")
			dispatcher.Register(poll, "poll")
			h.Dispatcher = dispatcher
			h.Backgrounds = []base.Background{
				loop.NewTimeoutProcessor(h, workerID, shards, workerTTL),
				// loop.NewRepair(h),
			}
			// Transports are always running; not paused by debug.start.
			push.Init()
			poll.Init()
			for _, b := range h.Backgrounds {
				b.Init()
			}
			if debug {
				log.Printf("debug mode: keyspace recreated, debug endpoints enabled")
			}

			srv := &http.Server{
				Addr:              addr,
				Handler:           netw.NewServer(h, poll),
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      10 * time.Second,
				IdleTimeout:       120 * time.Second,
			}

			go func() {
				log.Printf("listening on %s", addr)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("listen: %v", err)
				}
			}()

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
			<-quit
			log.Println("shutting down")

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := srv.Shutdown(ctx); err != nil {
				log.Printf("server shutdown: %v", err)
			}
			for _, b := range h.Backgrounds {
				b.Stop()
			}
			push.Stop()
			poll.Stop()
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", ":8001", "listen address")
	cmd.Flags().StringVar(&scyllaHost, "scylladb-host", "localhost", "ScyllaDB host (single-host fallback; use SCYLLADB_HOSTS for multi-host)")
	cmd.Flags().IntVar(&scyllaPort, "scylladb-port", 9042, "ScyllaDB CQL native port")
	cmd.Flags().Int64Var(&bucketWidthMs, "bucket-width-ms", 3_600_000, "timeout-bucket width in milliseconds")
	cmd.Flags().IntVar(&bucketLookback, "bucket-lookback", 1, "past buckets scanned by TickAt in addition to the current bucket")
	cmd.Flags().IntVar(&shards, "shards", 1, "number of timeout-table shards; must be identical across all server instances (changing requires schema recreation)")
	cmd.Flags().IntVar(&workerTTL, "worker-ttl", 30_000, "milliseconds before a worker row expires if not renewed; should be at least 2× the tick interval")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug mode: drop+recreate keyspace at startup, accept debug.* requests, honor resonate:debug_time header; debug.start installs recorder and stops background loops, debug.stop restores them")
	return cmd
}

// scyllaHosts returns the seed list. Prefers SCYLLADB_HOSTS (comma-separated,
// each entry may include :port). Falls back to flag/env single host plus port.
func scyllaHosts(fallbackHost string, fallbackPort int) []string {
	if v := os.Getenv("SCYLLADB_HOSTS"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	return []string{fmt.Sprintf("%s:%d", fallbackHost, fallbackPort)}
}
