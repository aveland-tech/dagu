// Copyright (C) 2024 The Daguflow/Dagu Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package cmd

import (
	"log"
	"os"
	"path/filepath"

	"github.com/daguflow/dagu/internal/agent"
	"github.com/daguflow/dagu/internal/config"
	"github.com/daguflow/dagu/internal/dag"
	"github.com/daguflow/dagu/internal/logger"
	"github.com/spf13/cobra"
)

func retryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry --req=<request-id> /path/to/spec.yaml",
		Short: "Retry the DAG execution",
		Long:  `dagu retry --req=<request-id> /path/to/spec.yaml`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Load()
			if err != nil {
				log.Fatalf("Configuration load failed: %v", err)
			}
			initLogger := logger.NewLogger(logger.NewLoggerArgs{
				LogLevel:  cfg.LogLevel,
				LogFormat: cfg.LogFormat,
			})

			requestID, err := cmd.Flags().GetString("req")
			if err != nil {
				initLogger.Error("Request ID generation failed", "error", err)
				os.Exit(1)
			}

			// Read the specified DAG execution status from the history store.
			dataStore := newDataStores(cfg)
			historyStore := dataStore.HistoryStore()

			specFilePath := args[0]
			absoluteFilePath, err := filepath.Abs(specFilePath)
			if err != nil {
				initLogger.Error("Absolute path resolution failed",
					"error", err,
					"file", specFilePath)
				os.Exit(1)
			}

			status, err := historyStore.FindByRequestID(absoluteFilePath, requestID)
			if err != nil {
				initLogger.Error("Historical execution retrieval failed",
					"error", err,
					"requestID", requestID,
					"file", absoluteFilePath)
				os.Exit(1)
			}

			// Start the DAG with the same parameters with the execution that
			// is being retried.
			workflow, err := dag.Load(cfg.BaseConfig, absoluteFilePath, status.Status.Params)
			if err != nil {
				initLogger.Error("Workflow specification load failed",
					"error", err,
					"file", specFilePath,
					"params", status.Status.Params)
				os.Exit(1)
			}

			newRequestID, err := generateRequestID()
			if err != nil {
				initLogger.Error("Request ID generation failed", "error", err)
				os.Exit(1)
			}

			logFile, err := openLogFile("dry_", cfg.LogDir, workflow, newRequestID)
			if err != nil {
				initLogger.Error("Log file creation failed",
					"error", err,
					"workflow", workflow.Name)
				os.Exit(1)
			}
			defer logFile.Close()

			agentLogger := logger.NewLogger(logger.NewLoggerArgs{
				LogLevel:  cfg.LogLevel,
				LogFormat: cfg.LogFormat,
				LogFile:   logFile,
			})

			cli := newClient(cfg, dataStore, agentLogger)

			agentLogger.Info("Workflow retry initiated",
				"workflow", workflow.Name,
				"originalRequestID", requestID,
				"newRequestID", newRequestID,
				"logFile", logFile.Name())

			agt := agent.New(
				newRequestID,
				workflow,
				agentLogger,
				filepath.Dir(logFile.Name()),
				logFile.Name(),
				cli,
				dataStore,
				&agent.Options{RetryTarget: status.Status},
			)

			ctx := cmd.Context()
			listenSignals(ctx, agt)

			if err := agt.Run(ctx); err != nil {
				agentLogger.Error("Failed to start workflow", "error", err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringP("req", "r", "", "request-id")
	_ = cmd.MarkFlagRequired("req")
	return cmd
}
