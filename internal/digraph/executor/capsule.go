package executor

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/dagu-org/dagu/internal/digraph"
	"github.com/dagu-org/dagu/internal/fileutil"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
)

//go:embed node/encapsulated_execution.js
var encapsulatedExecutionJS []byte

type capsule struct {
	cmd  *exec.Cmd
	lock sync.Mutex
}

func newCapsule(ctx context.Context, step digraph.Step) (Executor, error) {
	if len(step.Dir) > 0 && !fileutil.FileExists(step.Dir) {
		return nil, fmt.Errorf("directory %q does not exist", step.Dir)
	}

	stepContext := digraph.GetStepContext(ctx)

	params, err := mapToJSON(step.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to JSON: %w", err)
	}

	capsulesYAML := fmt.Sprintf("%s/%s", getCapsuleDir(*step.Capsule), "capsule.yaml")
	raw, err := readFile(capsulesYAML)
	if err != nil {
		return nil, err
	}

	// Decode the raw data into a config definition.
	def, err := decode(raw)
	if err != nil {
		return nil, err
	}

	// Write the embedded JavaScript to a temporary file
	tmpDir := os.TempDir()
	executionScript := filepath.Join(tmpDir, "encapsulated_execution.js")

	// nolint: gosec
	err = os.WriteFile(executionScript, encapsulatedExecutionJS, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to write embedded JavaScript to temporary file: %w", err)
	}

	userScript := filepath.Join(getCapsuleDir(*step.Capsule), def.Runs.ExecutionPoint)
	checkDef, err := json.Marshal(def)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal def to JSON: %w", err)
	}

	args := []string{executionScript, userScript, fmt.Sprintf("'%s'", string(checkDef)), fmt.Sprintf("'%s'", params)}

	step.Args = args
	step.Command = def.Runs.Using

	cmd, err := createCommand(ctx, step)
	if err != nil {
		return nil, fmt.Errorf("failed to create command: %w", err)
	}
	cmd.Env = append(cmd.Env, stepContext.AllEnvs()...)
	cmd.Dir = step.Dir

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
		Pgid:    0,
	}

	return &capsule{cmd: cmd}, nil
}

func (e *capsule) Run(_ context.Context) error {
	e.lock.Lock()
	err := e.cmd.Start()
	e.lock.Unlock()
	if err != nil {
		return err
	}
	return e.cmd.Wait()
}

func (e *capsule) SetStdout(out io.Writer) {
	e.cmd.Stdout = out
}

func (e *capsule) SetStderr(out io.Writer) {
	e.cmd.Stderr = out
}

func (e *capsule) Kill(sig os.Signal) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.cmd == nil || e.cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-e.cmd.Process.Pid, sig.(syscall.Signal))
}

func init() {
	Register(digraph.ExecutorTypeCapsule, newCapsule)
}

func getCapsuleDir(capsule digraph.Capsule) string {
	return fmt.Sprintf("%s/%s/%s/%s", capsule.CheckoutDir, capsule.Owner, capsule.Name, capsule.Ref)
}

// readFile reads the contents of the file into a map.
func readFile(file string) (cfg map[string]any, err error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %v", errReadFile, file, err)
	}

	return unmarshalData(data)
}

// unmarshalData unmarshals the data into a map.
func unmarshalData(data []byte) (map[string]any, error) {
	var cm map[string]any
	err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&cm)
	if errors.Is(err, io.EOF) {
		err = nil
	}

	return cm, err
}

var (
	errReadFile = errors.New("failed to read file")
)

// decode decodes the configuration map into a configDefinition.
func decode(cm map[string]any) (*digraph.CapsuleFileDef, error) {
	c := new(digraph.CapsuleFileDef)
	md, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
		TagName:     "",
	})
	err := md.Decode(cm)

	return c, err
}

// MapToJSON converts a []string in key=value format to a JSON string.
func mapToJSON(input []string) (string, error) {
	// Map to hold the key-value pairs
	result := make(map[string]string)

	// Iterate over the input to split into key-value pairs
	for _, item := range input {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			result[key] = value
		}
	}

	// Convert the map to JSON
	jsonData, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}
