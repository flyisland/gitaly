package ssh

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/bundleuri"
	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v16/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func (s *server) sshUploadPack(ctx context.Context, req *gitalypb.SSHUploadPackWithSidechannelRequest, stdin io.Reader, stdout, stderr io.Writer) (negotiation *stats.PackfileNegotiation, _ int, _ error) {
	repoProto := req.GetRepository()

	repo := s.localRepoFactory.Build(repoProto)
	repoPath, err := repo.Path(ctx)
	if err != nil {
		return nil, 0, err
	}

	gitcmd.WarnIfTooManyBitmaps(ctx, s.logger, s.locator, repoProto.GetStorageName(), repoPath)

	config, err := gitcmd.ConvertConfigOptions(req.GetGitConfigOptions())
	if err != nil {
		return nil, 0, err
	}

	var wg sync.WaitGroup
	pr, pw := io.Pipe()
	defer func() {
		pw.Close()
		wg.Wait()
	}()

	stdin = io.TeeReader(stdin, pw)

	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			pr.Close()
		}()

		stats, errIgnore := stats.ParsePackfileNegotiation(pr)
		negotiation = &stats
		if errIgnore != nil {
			s.logger.WithError(errIgnore).DebugContext(ctx, "failed parsing packfile negotiation")
			return
		}
		stats.UpdateMetrics(s.packfileNegotiationMetrics)
		stats.UpdateLogFields(ctx)
	}()

	if s.bundleURIManager != nil {
		config = append(config, s.bundleURIManager.UploadPackGitConfig(ctx, req.GetRepository())...)
	} else {
		config = append(config, bundleuri.CapabilitiesGitConfig(ctx, false)...)
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("detecting object hash: %w", err)
	}

	commandOpts := []gitcmd.CmdOpt{
		gitcmd.WithGitProtocol(s.logger, req),
		gitcmd.WithConfig(config...),
		gitcmd.WithPackObjectsHookEnv(objectHash, repoProto, "ssh"),
	}

	timeoutTicker := s.uploadPackRequestTimeoutTickerFactory()

	// upload-pack negotiation is terminated by either a flush, or the "done"
	// packet: https://github.com/git/git/blob/v2.20.0/Documentation/technical/pack-protocol.txt#L335
	//
	// "flush" tells the server it can terminate, while "done" tells it to start
	// generating a packfile. Add a timeout to the second case to mitigate
	// use-after-check attacks.
	if err := s.runUploadCommand(ctx, repo, stdin, stdout, stderr, timeoutTicker, pktline.PktDone(), gitcmd.Command{
		Name: "upload-pack",
		Args: []string{repoPath},
	}, commandOpts...); err != nil {
		status, _ := command.ExitStatus(err)
		return nil, status, fmt.Errorf("running upload-pack: %w", err)
	}

	return nil, 0, nil
}

func (s *server) SSHUploadPackWithSidechannel(ctx context.Context, req *gitalypb.SSHUploadPackWithSidechannelRequest) (*gitalypb.SSHUploadPackWithSidechannelResponse, error) {
	conn, err := sidechannel.OpenSidechannel(ctx)
	if err != nil {
		return nil, structerr.NewAborted("opennig sidechannel: %w", err)
	}
	defer conn.Close()

	sidebandWriter := pktline.NewSidebandWriter(conn)
	stdout := sidebandWriter.Writer(stream.BandStdout)
	stderr := sidebandWriter.Writer(stream.BandStderr)
	stats, _, err := s.sshUploadPack(ctx, req, conn, stdout, stderr)
	if err != nil {
		return nil, structerr.NewInternal("%w", err)
	}
	if err := conn.Close(); err != nil {
		return nil, structerr.NewInternal("close sidechannel: %w", err)
	}

	return &gitalypb.SSHUploadPackWithSidechannelResponse{
		PackfileNegotiationStatistics: stats.ToProto(),
	}, nil
}
