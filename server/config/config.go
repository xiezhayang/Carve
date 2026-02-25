package config

import (
	"os"
	"strings"
)

type Config struct {
	csvDir  string
	port    string
	metrics []string
}

func Load() *Config {
	csvDir := os.Getenv("CARVE_CSV_DIR")
	if csvDir == "" {
		csvDir = "data"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	var metrics []string
	if s := strings.TrimSpace(os.Getenv("CARVE_METRICS")); s != "" {
		for _, m := range strings.Split(s, ",") {
			if t := strings.TrimSpace(m); t != "" {
				metrics = append(metrics, t)
			}
		}
	}
	return &Config{csvDir: csvDir, port: port, metrics: metrics}
}

func (c *Config) CSVDir() string           { return c.csvDir }
func (c *Config) Port() string             { return c.port }
func (c *Config) InitialMetrics() []string { return c.metrics }
