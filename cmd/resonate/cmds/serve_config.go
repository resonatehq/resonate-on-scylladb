package cmds

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type serveConfig struct {
	Server struct {
		Addr  string `mapstructure:"addr"`
		Debug bool   `mapstructure:"debug"`
	} `mapstructure:"server"`

	ScyllaDB struct {
		Hosts       []string `mapstructure:"hosts"`
		Port        int      `mapstructure:"port"`
		Username    string   `mapstructure:"username"`
		Password    string   `mapstructure:"password"`
		Keyspace    string   `mapstructure:"keyspace"`
		Replication string   `mapstructure:"replication"`
		TLS         struct {
			Enabled  bool `mapstructure:"enabled"`
			Insecure bool `mapstructure:"insecure"`
		} `mapstructure:"tls"`
	} `mapstructure:"scylladb"`

	Timeouts struct {
		BucketWidth    time.Duration `mapstructure:"bucket-width"`
		BucketLookback int           `mapstructure:"bucket-lookback"`
		Shards         int           `mapstructure:"shards"`
	} `mapstructure:"timeouts"`

	Worker struct {
		TTL          time.Duration `mapstructure:"ttl"`
		TickInterval time.Duration `mapstructure:"tick-interval"`
	} `mapstructure:"worker"`
}

func loadServeConfig(cmd *cobra.Command, configPath string) (serveConfig, []string, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	setServeDefaults(v)

	if err := bindServeFlags(v, cmd); err != nil {
		return serveConfig{}, nil, err
	}

	// pflag.StringArray.String() returns "[a,b,c]" (bracket-enclosed), which
	// confuses Viper's string-to-slice hook. Extract the actual []string and
	// push it into the override layer so mapstructure receives a proper slice.
	if f := cmd.Flags().Lookup("scylladb-host"); f != nil && f.Changed {
		if hosts, err := cmd.Flags().GetStringArray("scylladb-host"); err == nil {
			v.Set("scylladb.hosts", hosts)
		}
	}

	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return serveConfig{}, nil, fmt.Errorf("read config file: %w", err)
		}
	} else {
		v.SetConfigName("resonate")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return serveConfig{}, nil, fmt.Errorf("read config file: %w", err)
			}
		}
	}

	var cfg serveConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return serveConfig{}, nil, fmt.Errorf("unmarshal config: %w", err)
	}

	var warnings []string
	if err := validateServeConfig(&cfg, &warnings); err != nil {
		return serveConfig{}, nil, err
	}

	return cfg, warnings, nil
}

func setServeDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8001")
	v.SetDefault("server.debug", false)
	v.SetDefault("scylladb.hosts", []string{"localhost"})
	v.SetDefault("scylladb.port", 0)
	v.SetDefault("scylladb.username", "")
	v.SetDefault("scylladb.password", "")
	v.SetDefault("scylladb.keyspace", "")
	v.SetDefault("scylladb.replication", "")
	v.SetDefault("scylladb.tls.enabled", false)
	v.SetDefault("scylladb.tls.insecure", false)
	v.SetDefault("timeouts.bucket-width", time.Hour)
	v.SetDefault("timeouts.bucket-lookback", 1)
	v.SetDefault("timeouts.shards", 1)
	v.SetDefault("worker.ttl", 15*time.Second)
	v.SetDefault("worker.tick-interval", time.Second)
}

func bindServeFlags(v *viper.Viper, cmd *cobra.Command) error {
	bindings := [][2]string{
		{"server.addr", "addr"},
		{"server.debug", "debug"},
		{"scylladb.hosts", "scylladb-host"},
		{"scylladb.port", "scylladb-port"},
		{"scylladb.username", "scylladb-username"},
		{"scylladb.password", "scylladb-password"},
		{"scylladb.tls.enabled", "scylladb-tls"},
		{"scylladb.tls.insecure", "scylladb-tls-insecure"},
		{"scylladb.keyspace", "scylladb-keyspace"},
		{"scylladb.replication", "scylladb-replication"},
		{"timeouts.bucket-width", "bucket-width"},
		{"timeouts.bucket-lookback", "bucket-lookback"},
		{"timeouts.shards", "shards"},
		{"worker.ttl", "worker-ttl"},
		{"worker.tick-interval", "worker-tick-interval"},
	}
	for _, b := range bindings {
		if err := v.BindPFlag(b[0], cmd.Flags().Lookup(b[1])); err != nil {
			return fmt.Errorf("bind flag %q to key %q: %w", b[1], b[0], err)
		}
	}
	return nil
}

func validateServeConfig(cfg *serveConfig, warnings *[]string) error {
	if cfg.Timeouts.BucketWidth <= 0 {
		return fmt.Errorf("timeouts.bucket-width must be > 0, got %v", cfg.Timeouts.BucketWidth)
	}
	if cfg.Timeouts.BucketLookback < 0 {
		return fmt.Errorf("timeouts.bucket-lookback must be >= 0, got %d", cfg.Timeouts.BucketLookback)
	}
	if cfg.Timeouts.Shards <= 0 {
		return fmt.Errorf("timeouts.shards must be > 0, got %d", cfg.Timeouts.Shards)
	}
	if cfg.Worker.TTL <= 0 {
		return fmt.Errorf("worker.ttl must be > 0, got %v", cfg.Worker.TTL)
	}
	if cfg.Worker.TickInterval <= 0 {
		return fmt.Errorf("worker.tick-interval must be > 0, got %v", cfg.Worker.TickInterval)
	}
	if cfg.Worker.TickInterval >= cfg.Worker.TTL {
		*warnings = append(*warnings, fmt.Sprintf(
			"warning: tick-interval (%v) >= worker-ttl (%v); worker rows may expire before the next heartbeat",
			cfg.Worker.TickInterval, cfg.Worker.TTL,
		))
	}
	return nil
}

func registerServeFlags(cmd *cobra.Command) {
	cmd.Flags().String("config", "", "config file path; optional")
	cmd.Flags().String("addr", ":8001", "server listen address")
	cmd.Flags().StringArray("scylladb-host", []string{"localhost"}, "ScyllaDB seed host; repeat for multiple seeds")
	cmd.Flags().Int("scylladb-port", 0, "ScyllaDB CQL port for hosts without an explicit port; 0 uses gocql default")
	cmd.Flags().String("scylladb-username", "", "ScyllaDB username")
	cmd.Flags().String("scylladb-password", "", "ScyllaDB password")
	cmd.Flags().Bool("scylladb-tls", false, "ScyllaDB TLS")
	cmd.Flags().Bool("scylladb-tls-insecure", false, "ScyllaDB TLS certificate verification; disabled when set")
	cmd.Flags().String("scylladb-keyspace", "", "ScyllaDB keyspace; empty uses dbms default")
	cmd.Flags().String("scylladb-replication", "", "ScyllaDB replication clause for schema creation")
	cmd.Flags().Duration("bucket-width", time.Hour, "timeout bucket width (e.g. 1h, 30m); must be greater than 0")
	cmd.Flags().Int("bucket-lookback", 1, "past timeout buckets to scan in addition to the current bucket")
	cmd.Flags().Int("shards", 1, "timeout table shard count; must match across server instances")
	cmd.Flags().Duration("worker-ttl", 15*time.Second, "worker row TTL (e.g. 15s, 1m); must be greater than 0")
	cmd.Flags().Duration("worker-tick-interval", time.Second, "coordinator and table-loop tick interval (e.g. 6s, 1m); should be less than worker-ttl")
	cmd.Flags().Bool("debug", false, "debug mode; recreates schema and enables debug endpoints")
}
