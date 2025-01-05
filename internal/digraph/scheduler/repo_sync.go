package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/dagu-org/dagu/internal/digraph"
	"github.com/dagu-org/dagu/internal/logger"
)

func getCheckDir(remoteWorkflow digraph.RemoteWorkflow) string {
	return fmt.Sprintf("%s/%s/%s/%s", remoteWorkflow.CheckoutDir, remoteWorkflow.Owner, remoteWorkflow.Name, remoteWorkflow.Ref)
}

func CloneCheck(ctx context.Context, sc *Scheduler, remoteWorkflow digraph.RemoteWorkflow) error {
	checkDir := getCheckDir(remoteWorkflow)

	if !CheckExists(remoteWorkflow) {
		return errors.New("check does not exist as it does not contain a check.yaml file in the root of the repository")
	}

	if IsRepoUpToDate(ctx, checkDir, remoteWorkflow) {
		logger.Infof(ctx, "Repository %s is already cloned and up to date", remoteWorkflow.FullName())
		return nil
	}

	return CloneRepo(ctx, remoteWorkflow, checkDir)
}

func IsRepoUpToDate(ctx context.Context, checkDir string, remoteWorkflow digraph.RemoteWorkflow) bool {
	if _, err := os.Stat(checkDir); os.IsNotExist(err) {
		return false
	}

	repoURL := fmt.Sprintf("https://api.github.com/repos/%s/git/refs/heads/%s", remoteWorkflow.FullName(), remoteWorkflow.Ref)
	req, err := http.NewRequestWithContext(ctx, "GET", repoURL, nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}

	remoteCommitHash, ok := result["object"].(map[string]any)["sha"].(string)
	if !ok {
		return false
	}

	cmd := exec.CommandContext(ctx, "git", "-C", checkDir, "rev-parse", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	localCommitHash := strings.TrimSpace(string(output))
	return localCommitHash == remoteCommitHash
}

func CheckExists(remoteWorkflow digraph.RemoteWorkflow) bool {
	checkURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/check.yaml", remoteWorkflow.Owner, remoteWorkflow.Name, remoteWorkflow.Ref)
	parsedURL, err := url.Parse(checkURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return false
	}
	resp, err := http.Head(parsedURL.String())
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	defer resp.Body.Close()
	return true
}

func ValidateInput(input string) (string, error) {
	if regexp.MustCompile(`^[a-zA-Z0-9._/-]+$`).MatchString(input) {
		return input, nil
	}
	return "", fmt.Errorf("invalid input: %s", input)
}

func ValidateInputs(inputs ...*string) error {
	for _, input := range inputs {
		valid, err := ValidateInput(*input)
		if err != nil {
			return err
		}
		*input = valid
	}
	return nil
}

func CloneRepo(ctx context.Context, remoteWorkflow digraph.RemoteWorkflow, checkDir string) error {
	if err := ValidateInputs(&remoteWorkflow.Ref, &remoteWorkflow.Owner, &remoteWorkflow.Name); err != nil {
		return err
	}

	var err error
	if checkDir, err = filepath.Abs(checkDir); err != nil {
		return err
	}

	cloneURL := fmt.Sprintf("https://github.com/%s/%s.git", remoteWorkflow.Owner, remoteWorkflow.Name)

	if _, err := os.Stat(checkDir); err == nil {
		return exec.CommandContext(ctx, "git", "-C", checkDir, "pull", "origin", remoteWorkflow.Ref).Run()
	} else if os.IsNotExist(err) {
		return exec.CommandContext(ctx, "git", "clone", "--branch", remoteWorkflow.Ref, cloneURL, checkDir).Run()
	}

	return fmt.Errorf("failed to access directory: %w", err)
}

func SyncRemoteWorkflows(ctx context.Context, sc *Scheduler, graph *ExecutionGraph) error {
	if !graph.IsRemoteSynced() {
		for _, node := range graph.Nodes() {
			if node.data.Step.RemoteWorkflow != nil && node.data.Step.RemoteWorkflow.FullName() != "" {
				logger.Infof(ctx, "Fetching remote workflow for step '%s'", node.data.Step.RemoteWorkflow.FullName())
				logger.Infof(ctx, "Cloning repository to '%s'", node.data.Step.RemoteWorkflow.CheckoutDir)
				if err := CloneCheck(ctx, sc, *node.data.Step.RemoteWorkflow); err != nil {
					logger.Error(ctx, "Failed to clone remote workflow repository", "repository", node.data.Step.RemoteWorkflow.FullName(), "error", err)
					node.MarkError(err)
				}
			}
		}
		graph.RemoteSynced()
	}
	return nil
}

func CheckInitialErrors(sc *Scheduler, graph *ExecutionGraph) error {
	for _, node := range graph.Nodes() {
		if node.State().Status == NodeStatusError {
			sc.setLastError(node.State().Error)
			for _, n := range graph.Nodes() {
				if n.State().Status == NodeStatusNone {
					n.SetStatus(NodeStatusSkipped)
				}
			}
			return sc.lastError
		}
	}
	return nil
}
