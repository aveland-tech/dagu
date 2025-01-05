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

func getCapsuleDir(capsule digraph.Capsule) string {
	return fmt.Sprintf("%s/%s/%s/%s", capsule.CheckoutDir, capsule.Owner, capsule.Name, capsule.Ref)
}

func CloneCheck(ctx context.Context, capsule digraph.Capsule) error {
	checkDir := getCapsuleDir(capsule)

	if !CapsuleExists(capsule) {
		return errors.New("check does not exist as it does not contain a capsule.yaml file in the root of the repository")
	}

	if IsRepoUpToDate(ctx, checkDir, capsule) {
		logger.Infof(ctx, "Repository %s is already cloned and up to date", capsule.FullName())
		return nil
	}

	return CloneCapsule(ctx, capsule, checkDir)
}

func IsRepoUpToDate(ctx context.Context, checkDir string, capsule digraph.Capsule) bool {
	if _, err := os.Stat(checkDir); os.IsNotExist(err) {
		return false
	}

	repoURL := fmt.Sprintf("https://api.github.com/repos/%s/git/refs/heads/%s", capsule.FullName(), capsule.Ref)
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

func CapsuleExists(capsule digraph.Capsule) bool {
	capsuleURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/capsule.yaml", capsule.Owner, capsule.Name, capsule.Ref)
	parsedURL, err := url.Parse(capsuleURL)
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

func CloneCapsule(ctx context.Context, capsule digraph.Capsule, checkDir string) error {
	if err := ValidateInputs(&capsule.Ref, &capsule.Owner, &capsule.Name); err != nil {
		return err
	}

	var err error
	if checkDir, err = filepath.Abs(checkDir); err != nil {
		return err
	}

	cloneURL := fmt.Sprintf("https://github.com/%s/%s.git", capsule.Owner, capsule.Name)

	if _, err := os.Stat(checkDir); err == nil {
	  // nolint:gosec
		return exec.CommandContext(ctx, "git", "-C", checkDir, "pull", "origin", capsule.Ref).Run()
	} else if os.IsNotExist(err) {
		// nolint:gosec
		return exec.CommandContext(ctx, "git", "clone", "--branch", capsule.Ref, cloneURL, checkDir).Run()
	}

	return fmt.Errorf("failed to access directory: %w", err)
}

func SyncCapsules(ctx context.Context, sc *Scheduler, graph *ExecutionGraph) error {
	if !graph.IsRemoteSynced() {
		for _, node := range graph.Nodes() {
			if node.data.Step.Capsule != nil && node.data.Step.Capsule.FullName() != "" {
				logger.Infof(ctx, "Fetching remote workflow for step '%s'", node.data.Step.Capsule.FullName())
				logger.Infof(ctx, "Cloning repository to '%s'", node.data.Step.Capsule.CheckoutDir)
				if err := CloneCheck(ctx, *node.data.Step.Capsule); err != nil {
					logger.Error(ctx, "Failed to clone remote workflow repository", "repository", node.data.Step.Capsule.FullName(), "error", err)
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
