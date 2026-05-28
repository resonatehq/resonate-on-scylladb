package cmds

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			configPath, err := cmd.Flags().GetString("config")
			if err != nil {
				return err
			}

			cfg, warnings, err := loadServeConfig(cmd, configPath)
			if err != nil {
				return err
			}
			for _, w := range warnings {
				log.Print(w)
			}

			dbCfg := dbms.Config{
				Hosts:        cfg.ScyllaDB.Hosts,
				Port:         cfg.ScyllaDB.Port,
				Username:     cfg.ScyllaDB.Username,
				Password:     cfg.ScyllaDB.Password,
				TLS:          cfg.ScyllaDB.TLS.Enabled,
				TLSInsecure:  cfg.ScyllaDB.TLS.Insecure,
				Keyspace:     cfg.ScyllaDB.Keyspace,
				Replication:  cfg.ScyllaDB.Replication,
				CreateSchema: cfg.Server.Debug,
			}

			session, err := dbms.Connect(dbCfg)
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
				Host:           strings.Join(cfg.ScyllaDB.Hosts, ","),
				BucketWidth:    cfg.Timeouts.BucketWidth.Milliseconds(),
				BucketLookback: cfg.Timeouts.BucketLookback,
				Shards:         cfg.Timeouts.Shards,
				Debug:          cfg.Server.Debug,
			}

			push := &netw.HttpPush{}
			poll := &netw.HttpPoll{}
			dispatcher := base.NewDispatcher()
			dispatcher.Register(push, "http", "https")
			dispatcher.Register(poll, "poll")
			h.Dispatcher = dispatcher
			h.Backgrounds = []base.Background{
				loop.NewTimeoutProcessor(h, workerID, cfg.Timeouts.Shards, cfg.Worker.TTL, cfg.Worker.TickInterval),
			}
			push.Init()
			poll.Init()
			for _, b := range h.Backgrounds {
				b.Init()
			}
			if cfg.Server.Debug {
				log.Printf("debug mode: keyspace recreated, debug endpoints enabled")
			}

			srv := &http.Server{
				Addr:              cfg.Server.Addr,
				Handler:           netw.NewServer(h, poll),
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      10 * time.Second,
				IdleTimeout:       120 * time.Second,
			}

			go func() {
				log.Printf("listening on %s", cfg.Server.Addr)
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
	registerServeFlags(cmd)
	return cmd
}
