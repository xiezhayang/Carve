package main

import (
	"context"
	"log"
	"os"

	"github.com/xiezhayang/Carve/datamanager"
	"github.com/xiezhayang/Carve/server"
	"github.com/xiezhayang/Carve/server/alert"
	"github.com/xiezhayang/Carve/server/config"
	"github.com/xiezhayang/Carve/server/job"
)

func main() {
	cfg := config.Load()
	_ = os.MkdirAll(cfg.CSVDir(), 0755)
	_ = os.MkdirAll(cfg.ModelDir(), 0755)
	state := datamanager.NewState(*cfg)
	if err := state.LoadTargets(); err != nil {
		log.Printf("[carve] LoadTargets: %v", err)
	}
	log.Printf("[carve] LoadTargets done, %d targets", len(state.AllTargets()))
	writer := func(path string, rows []datamanager.Row) (int, error) {
		return datamanager.AppendRows(path, rows)
	}
	var jobRunner func(string, string) error
	var jobDeleter func(context.Context, string) error
	var alerterDeployer func(context.Context, string, string) (string, error)
	var alerterDeleter func(context.Context, string) error
	if runner, err := job.NewRunnerInCluster(cfg.CarveURL(), cfg.TrainerImage(), cfg.JobNamespace()); err != nil {
		log.Printf("[carve] JobRunner disabled (not in cluster or no RBAC): %v", err)
		jobRunner = nil
	} else {
		jobRunner = func(csvFilename, modelName string) error {
			return runner.Run(context.Background(), csvFilename, modelName)
		}
		jobDeleter = func(ctx context.Context, jobName string) error {
			return runner.DeleteJob(ctx, jobName)
		}
	}
	if alerterRunner, err := alert.NewRunnerInCluster(cfg.CarveURL(), cfg.AlerterImage(), cfg.JobNamespace()); err != nil {
		log.Printf("[carve] AlerterDeployer disabled (not in cluster or no RBAC): %v", err)
	} else {
		alerterDeployer = func(ctx context.Context, target, modelName string) (string, error) {
			return alerterRunner.Deploy(ctx, target, modelName)
		}
		alerterDeleter = func(ctx context.Context, deploymentName string) error {
			return alerterRunner.Delete(ctx, deploymentName)
		}
	}
	srv := server.New(cfg, state, writer, jobRunner, jobDeleter, alerterDeployer, alerterDeleter)
	_ = srv.Run()
}
