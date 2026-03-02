package config

import (
	"os"
	"strings"
)

type Config struct {
	csvDir       string
	port         string
	metrics      []string
	modelDir     string
	trainerImage string
	jobNamespace string
	carveURL     string
	alerterImage string
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
	modelDir := os.Getenv("CARVE_MODEL_DIR")
	if modelDir == "" {
		modelDir = "data/models"
	}
	trainerImage := os.Getenv("CARVE_TRAINER_IMAGE")
	if trainerImage == "" {
		trainerImage = "carve-trainer:latest"
	}
	jobNamespace := os.Getenv("CARVE_JOB_NAMESPACE")
	if jobNamespace == "" {
		jobNamespace = "default"
	}
	carveURL := os.Getenv("CARVE_URL")
	if carveURL == "" {
		carveURL = "http://carve:8080"
	}
	alerterImage := os.Getenv("CARVE_ALERTER_IMAGE")
	if alerterImage == "" {
		alerterImage = "carve-alerter:latest"
	}
	carveURL = strings.TrimSuffix(carveURL, "/")
	var metrics []string
	if s := strings.TrimSpace(os.Getenv("CARVE_METRICS")); s != "" {
		for _, m := range strings.Split(s, ",") {
			if t := strings.TrimSpace(m); t != "" {
				metrics = append(metrics, t)
			}
		}
	}
	return &Config{
		csvDir:       csvDir,
		port:         port,
		metrics:      metrics,
		modelDir:     modelDir,
		trainerImage: trainerImage,
		jobNamespace: jobNamespace,
		carveURL:     carveURL,
		alerterImage: alerterImage,
	}
}

func (c *Config) CSVDir() string           { return c.csvDir }
func (c *Config) Port() string             { return c.port }
func (c *Config) InitialMetrics() []string { return c.metrics }
func (c *Config) ModelDir() string         { return c.modelDir }
func (c *Config) TrainerImage() string     { return c.trainerImage }
func (c *Config) JobNamespace() string     { return c.jobNamespace }
func (c *Config) CarveURL() string         { return c.carveURL }
func (c *Config) AlerterImage() string     { return c.alerterImage }
