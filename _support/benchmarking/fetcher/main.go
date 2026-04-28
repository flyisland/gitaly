package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	gitalyclient "gitlab.com/gitlab-org/gitaly/v18/client"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
	"google.golang.org/grpc"
)

const clientCaps = "multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta deepen-since deepen-not agent=git/2.47.0"

// Repos can have millions of refs (additional_refs: 1000000 in config).
// Buffering all of them would waste memory — we only need a sample to pick want targets.
const maxRefsPerRepo = 1000

type repoConfig struct {
	Name          string   `json:"name"`
	IncludeInTest bool     `json:"include_in_test"`
	Testdata      testdata `json:"testdata"`
}

type testdata struct {
	Commits []string `json:"commits"`
}

var (
	gitalyAddr  = flag.String("addr", "", "Gitaly TCP address (host:port)")
	reposFile   = flag.String("repos", "", "Path to repositories.json")
	concurrency = flag.Int("concurrency", 10, "Number of concurrent fetch workers")
	duration    = flag.Duration("duration", 5*time.Minute, "Benchmark duration")
	mode        = flag.String("mode", "incremental", "Fetch mode: 'full' (clone) or 'incremental' (fetch with haves)")
)

func main() {
	flag.Parse()
	if *gitalyAddr == "" || *reposFile == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *mode != "full" && *mode != "incremental" {
		log.Fatalf("invalid -mode %q: must be 'full' or 'incremental'", *mode)
	}

	repos := loadRepos(*reposFile)
	if len(repos) == 0 {
		log.Fatal("no active repositories found in config")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration+30*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	registry := gitalyclient.NewSidechannelRegistry(logger)

	conn, err := gitalyclient.DialSidechannel(ctx, "tcp://"+*gitalyAddr, registry)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	wantOIDs := discoverRefs(ctx, conn, repos)
	for _, repo := range repos {
		if len(wantOIDs[repo.Name]) == 0 {
			log.Fatalf("no refs discovered for repo %q — cannot benchmark", repo.Name)
		}
	}

	log.Printf("starting benchmark: concurrency=%d duration=%s mode=%s repos=%d",
		*concurrency, *duration, *mode, len(repos))

	done := make(chan struct{})
	time.AfterFunc(*duration, func() { close(done) })

	var wg sync.WaitGroup
	for i := range *concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(ctx, done, conn, registry, repos, wantOIDs, id)
		}(i)
	}
	wg.Wait()
}

func runWorker(
	ctx context.Context,
	done <-chan struct{},
	conn *grpc.ClientConn,
	registry *gitalyclient.SidechannelRegistry,
	repos []repoConfig,
	wantOIDs map[string][]string,
	id int,
) {
	for {
		select {
		case <-done:
			return
		default:
		}

		repo := repos[rand.Intn(len(repos))]
		wants := wantOIDs[repo.Name]
		wantOID := wants[rand.Intn(len(wants))]

		var haveOID string
		if *mode == "incremental" && len(repo.Testdata.Commits) > 0 {
			haveOID = repo.Testdata.Commits[rand.Intn(len(repo.Testdata.Commits))]
		}

		if err := doFetch(ctx, conn, registry, repo.Name, wantOID, haveOID); err != nil {
			log.Printf("worker %d: fetch %s: %v", id, repo.Name, err)
		}
	}
}

// buildNegotiation constructs the pktline payload for a git-upload-pack exchange.
//
// Incremental fetch (with haves) uses the no-done capability: the server sends
// the packfile after ACKing a common commit without waiting for "done".
// Full clone (no haves) must send "done" explicitly since there are no haves to ACK.
//
// See: https://git-scm.com/docs/pack-protocol#_packfile_negotiation
func buildNegotiation(wantOID, haveOID string) *bytes.Buffer {
	var buf bytes.Buffer

	pktline.WriteString(&buf, fmt.Sprintf("want %s %s\n", wantOID, clientCaps))
	pktline.WriteFlush(&buf)

	if haveOID != "" {
		pktline.WriteString(&buf, fmt.Sprintf("have %s\n", haveOID))
		pktline.WriteFlush(&buf)
	} else {
		buf.Write(pktline.PktDone())
	}

	return &buf
}

func doFetch(
	ctx context.Context,
	conn *grpc.ClientConn,
	registry *gitalyclient.SidechannelRegistry,
	repoName, wantOID, haveOID string,
) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	negotiation := buildNegotiation(wantOID, haveOID)

	req := &gitalypb.SSHUploadPackWithSidechannelRequest{
		Repository: &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: repoName,
			GlRepository: repoName,
		},
	}

	// countingWriter discards the packfile data to avoid buffering potentially
	// large packfiles (hundreds of MB for full clones).
	var stdout discardWriter
	var stderr bytes.Buffer

	_, err := gitalyclient.UploadPackWithSidechannelWithResult(
		ctx, conn, registry,
		negotiation,
		&stdout,
		&stderr,
		req,
	)
	if err != nil {
		return fmt.Errorf("upload-pack: %w (stderr: %s)", err, stderr.String())
	}

	return nil
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func discoverRefs(ctx context.Context, conn *grpc.ClientConn, repos []repoConfig) map[string][]string {
	smartHTTP := gitalypb.NewSmartHTTPServiceClient(conn)
	result := make(map[string][]string)

	for _, repo := range repos {
		oids, err := discoverRepoRefs(ctx, smartHTTP, repo.Name)
		if err != nil {
			log.Printf("info-refs %s: %v", repo.Name, err)
			continue
		}

		log.Printf("discovered %d refs for %s (capped at %d)", len(oids), repo.Name, maxRefsPerRepo)
		result[repo.Name] = oids
	}

	return result
}

func discoverRepoRefs(ctx context.Context, smartHTTP gitalypb.SmartHTTPServiceClient, repoName string) ([]string, error) {
	// Use a child context so we can cancel the stream once we have enough refs,
	// without leaking server-side resources.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := smartHTTP.InfoRefsUploadPack(ctx, &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: repoName,
			GlRepository: repoName,
		},
	})
	if err != nil {
		return nil, err
	}

	var oids []string
	scanner := pktline.NewScanner(newInfoRefsReader(stream))
	for scanner.Scan() {
		if len(oids) >= maxRefsPerRepo {
			break
		}
		line := pktline.Data(scanner.Bytes())
		if len(line) >= 40 {
			oid := string(line[:40])
			if isHex(oid) {
				oids = append(oids, oid)
			}
		}
	}

	return oids, nil
}

// infoRefsReader adapts the streaming InfoRefsUploadPack response into an io.Reader
// so it can be fed directly to pktline.NewScanner without buffering the full response.
type infoRefsReader struct {
	stream gitalypb.SmartHTTPService_InfoRefsUploadPackClient
	buf    []byte
}

func newInfoRefsReader(stream gitalypb.SmartHTTPService_InfoRefsUploadPackClient) *infoRefsReader {
	return &infoRefsReader{stream: stream}
}

func (r *infoRefsReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		resp, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = resp.GetData()
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func loadRepos(path string) []repoConfig {
	f, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var repos []repoConfig
	if err := json.Unmarshal(f, &repos); err != nil {
		log.Fatal(err)
	}
	var active []repoConfig
	for _, r := range repos {
		if r.IncludeInTest {
			active = append(active, r)
		}
	}
	return active
}
