package trace2hooks_test

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/trace2hooks"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper/testcfg"
)

func TestReceivePackHook(t *testing.T) {
	t.Parallel()

	ctx := log.InitContextCustomFields(testhelper.Context(t))
	cfg := testcfg.Build(t)

	_, sourceRepoPath := gittest.CreateRepository(t, ctx, cfg,
		gittest.CreateRepositoryConfig{SkipCreationViaService: true},
	)

	targetRepoProto, targetRepoPath := gittest.CreateRepository(t, ctx, cfg,
		gittest.CreateRepositoryConfig{SkipCreationViaService: true},
	)

	// Force connectivity checks in the target repository
	gittest.Exec(t, cfg, "-C", targetRepoPath, "config", "receive.connectivityCheck", "true")
	gittest.Exec(t, cfg, "-C", targetRepoPath, "config", "transfer.fsckObjects", "true")

	// Create a more complex commit history to make connectivity check more likely
	commit1 := gittest.WriteCommit(t, cfg, sourceRepoPath,
		gittest.WithMessage("first commit"),
		gittest.WithBranch("feature"),
		gittest.WithTreeEntries(gittest.TreeEntry{
			Mode:    "100644",
			Path:    "file1.txt",
			Content: "content 1",
		}),
	)

	commit2 := gittest.WriteCommit(t, cfg, sourceRepoPath,
		gittest.WithMessage("second commit"),
		gittest.WithParents(commit1),
		gittest.WithTreeEntries(gittest.TreeEntry{
			Mode:    "100644",
			Path:    "file2.txt",
			Content: "content 2",
		}),
	)

	commit3 := gittest.WriteCommit(t, cfg, sourceRepoPath,
		gittest.WithMessage("third commit"),
		gittest.WithParents(commit2),
		gittest.WithTreeEntries(gittest.TreeEntry{
			Mode:    "100644",
			Path:    "file3.txt",
			Content: "content 3",
		}),
	)

	var inputBuffer bytes.Buffer

	zeroOID := gittest.DefaultObjectHash.ZeroOID
	// Ref update line with proper pkt-line format - push the tip commit
	refUpdate := fmt.Sprintf("%s %s refs/heads/feature\x00report-status side-band-64k object-format=%s\n",
		zeroOID, commit3.String(), gittest.DefaultObjectHash.Format)
	pktLine := fmt.Sprintf("%04x%s", len(refUpdate)+4, refUpdate)
	inputBuffer.WriteString(pktLine)

	inputBuffer.WriteString("0000")

	var packData bytes.Buffer

	// Use rev-list to get all objects reachable from the tip commit
	var objList bytes.Buffer
	revListCmd := gittest.NewCommand(t, cfg, "-C", sourceRepoPath,
		"rev-list", "--objects", commit3.String())
	revListCmd.Stdout = &objList
	require.NoError(t, revListCmd.Run())

	// Extract just the object IDs (first column of each line)
	var objectIDs []string
	scanner := bufio.NewScanner(&objList)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) > 0 {
			objectIDs = append(objectIDs, parts[0])
		}
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, objectIDs, "Should have found objects to pack")

	// Create packfile with all objects
	packObjectsCmd := gittest.NewCommand(t, cfg, "-C", sourceRepoPath,
		"pack-objects", "--stdout")
	packObjectsCmd.Stdin = strings.NewReader(strings.Join(objectIDs, "\n") + "\n")
	packObjectsCmd.Stdout = &packData
	require.NoError(t, packObjectsCmd.Run())

	require.Greater(t, packData.Len(), 0, "Packfile should not be empty")

	inputBuffer.Write(packData.Bytes())

	gitCmdFactory := gittest.NewCommandFactory(t, cfg, gitcmd.WithTrace2Hooks([]trace2.Hook{
		trace2hooks.NewReceivePack(),
	}))

	cmd, err := gitCmdFactory.New(ctx, targetRepoProto, gitcmd.Command{
		Name: "receive-pack",
		Args: []string{targetRepoPath},
	}, gitcmd.WithStdin(&inputBuffer), gitcmd.WithDisabledHooks())
	require.NoError(t, err)

	// receive-pack may return non-zero exit code for some operations (like connectivity failures)
	// but we still want to capture the log entries
	_ = cmd.Wait()

	customFields := log.CustomFieldsFromContext(ctx)
	require.NotNil(t, customFields)

	logrusFields := customFields.Fields()
	require.Equal(t, "true", logrusFields["trace2.activated"])
	require.Equal(t, "receive-pack", logrusFields["trace2.hooks"])

	require.Contains(t, logrusFields, "receive-pack.connectivity-check-us")
	value, ok := logrusFields["receive-pack.connectivity-check-us"].(int)
	require.True(t, ok, "connectivity check value should be an integer")
	require.Greater(t, value, 0, "connectivity check should have positive elapsed time")

	childProcessCount := 0
	foundUnpackObjects := false
	foundIndexPack := false

	for key, val := range logrusFields {
		// Check for indexed child process timing fields (should be like "receive-pack.child-process.0", "receive-pack.child-process.1", etc.)
		if strings.HasPrefix(key, "receive-pack.child-process.") && !strings.HasSuffix(key, ".command") {
			// Extract the index to verify it's numeric
			suffix := strings.TrimPrefix(key, "receive-pack.child-process.")
			if _, err := fmt.Sscanf(suffix, "%d", new(int)); err == nil {
				intVal, ok := val.(int)
				require.True(t, ok, "child process timing should be an integer for key %s", key)
				require.Greater(t, intVal, 0, "child process should have positive elapsed time for key %s", key)
				childProcessCount++
			}
		}

		// Also check for the original non-indexed field name (during transition)
		if key == "receive-pack.child-process" {
			intVal, ok := val.(int)
			require.True(t, ok, "child process timing should be an integer")
			require.Greater(t, intVal, 0, "child process should have positive elapsed time")
			childProcessCount++
		}

		// Check for corresponding command name fields
		if strings.HasPrefix(key, "receive-pack.child-process.") && strings.HasSuffix(key, ".command") {
			cmdName, ok := val.(string)
			require.True(t, ok, "child process command should be a string")
			require.NotEmpty(t, cmdName, "child process command should not be empty")

			if strings.Contains(cmdName, "unpack-objects") {
				foundUnpackObjects = true
			} else if strings.Contains(cmdName, "index-pack") {
				foundIndexPack = true
			}
		}
	}

	require.Greater(t, childProcessCount, 0, "Expected to find at least one child process timing metric")
	require.True(t, foundUnpackObjects || foundIndexPack,
		"Expected to find either unpack-objects or index-pack child process command")
}
