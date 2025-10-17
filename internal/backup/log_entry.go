package backup

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v18/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage/storagemgr/partition/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/helper"
	logging "gitlab.com/gitlab-org/gitaly/v18/internal/log"
)

const (
	// minRetryWait is the shortest duration to backoff before retrying, and is also the initial backoff duration.
	minRetryWait = 5 * time.Second
	// maxRetryWait is the longest duration to backoff before retrying.
	maxRetryWait = 5 * time.Minute
)

// logEntry is used to track the state of a backup request.
type logEntry struct {
	partitionInfo PartitionInfo
	lsn           storage.LSN
	success       bool
}

// newLogEntry constructs a new logEntry.
func newLogEntry(partitionInfo PartitionInfo, lsn storage.LSN) *logEntry {
	return &logEntry{
		partitionInfo: partitionInfo,
		lsn:           lsn,
	}
}

// partitionNotification is used to store the data received by NotifyNewEntries.
type partitionNotification struct {
	lowWaterMark  storage.LSN
	highWaterMark storage.LSN
	partitionInfo PartitionInfo
}

// newPartitionNotification constructs a new partitionNotification.
func newPartitionNotification(storageName string, partitionID storage.PartitionID, lowWaterMark, highWaterMark storage.LSN) *partitionNotification {
	return &partitionNotification{
		partitionInfo: PartitionInfo{
			StorageName: storageName,
			PartitionID: partitionID,
		},
		lowWaterMark:  lowWaterMark,
		highWaterMark: highWaterMark,
	}
}

// partitionState tracks the progress made on one partition.
type partitionState struct {
	// nextLSN is the next LSN to be backed up.
	nextLSN storage.LSN
	// highWaterMark is the highest LSN to be backed up.
	highWaterMark storage.LSN
	// hasJob indicates if a backup job is currently being processed for this partition.
	hasJob bool
}

// newPartitionState constructs a new partitionState.
func newPartitionState(nextLSN, highWaterMark storage.LSN) *partitionState {
	return &partitionState{
		nextLSN:       nextLSN,
		highWaterMark: highWaterMark,
	}
}

// PartitionInfo is the global identifier for a partition.
type PartitionInfo struct {
	StorageName string
	PartitionID storage.PartitionID
}

// LogEntryArchiver is used to backup applied log entries. It has a configurable number of
// worker goroutines that will perform backups. Each partition may only have one backup
// executing at a time, entries are always processed in-order. Backup failures will trigger
// an exponential backoff.
type LogEntryArchiver struct {
	// logger is the logger to use to write log messages.
	logger logging.Logger
	// store is where the log archives are kept.
	store LogEntryStore
	// node is used to access the LogManagers.
	node *storage.Node

	// notificationCh is the channel used to signal that a new notification has arrived.
	notificationCh chan struct{}
	// workCh is the channel used to signal that the archiver should try to process more jobs.
	workCh chan struct{}
	// closingCh is the channel used to signal that the LogEntryArchiver should exit.
	closingCh chan struct{}
	// closedCh is the channel used to wait for the archiver to completely stop.
	closedCh chan struct{}

	// notifications is the list of log notifications to ingest. notificationsMutex must be held when accessing it.
	notifications *list.List
	// notificationsMutex is used to synchronize access to notifications.
	notificationsMutex sync.Mutex

	// partitionStates tracks the current LSN and entry backlog of each partition in a storage.
	partitionStates map[PartitionInfo]*partitionState
	// activePartitions tracks with partitions need to be processed.
	activePartitions map[PartitionInfo]struct{}

	// activeJobs tracks how many entries are currently being backed up.
	activeJobs uint
	// workerCount sets the number of goroutines used to perform backups.
	workerCount uint

	// waitDur controls how long to wait before retrying when a backup attempt fails.
	waitDur time.Duration
	// tickerFunc allows the archiver to wait with an exponential backoff between retries.
	tickerFunc func(time.Duration) helper.Ticker

	// backupCounter provides metrics with a count of the number of WAL entries backed up by status.
	backupCounter *prometheus.CounterVec
	// backupLatency provides metrics on the latency of WAL backup operations.
	backupLatency prometheus.Histogram
}

// NewLogEntryArchiver constructs a new LogEntryArchiver.
func NewLogEntryArchiver(logger logging.Logger, archiveSink *Sink, workerCount uint, node *storage.Node) *LogEntryArchiver {
	return newLogEntryArchiver(logger, archiveSink, workerCount, node, helper.NewTimerTicker)
}

// newLogEntryArchiver constructs a new LogEntryArchiver with a configurable ticker function.
func newLogEntryArchiver(logger logging.Logger, archiveSink *Sink, workerCount uint, node *storage.Node, tickerFunc func(time.Duration) helper.Ticker) *LogEntryArchiver {
	if workerCount < 1 {
		workerCount = 1
	}

	archiver := &LogEntryArchiver{
		logger:           logger,
		store:            NewLogEntryStore(archiveSink),
		node:             node,
		notificationCh:   make(chan struct{}, 1),
		workCh:           make(chan struct{}, 1),
		closingCh:        make(chan struct{}),
		closedCh:         make(chan struct{}),
		notifications:    list.New(),
		partitionStates:  make(map[PartitionInfo]*partitionState),
		activePartitions: make(map[PartitionInfo]struct{}),
		workerCount:      workerCount,
		tickerFunc:       tickerFunc,
		waitDur:          minRetryWait,
		backupCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_wal_backup_count",
				Help: "Counter of the number of WAL entries backed up by status",
			},
			[]string{"status"},
		),
		backupLatency: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "gitaly_wal_backup_latency_seconds",
				Help: "Latency of WAL entry backups",
			},
		),
	}

	return archiver
}

// NotifyNewEntries passes the log entry information to the LogEntryArchiver for processing.
func (la *LogEntryArchiver) NotifyNewEntries(storageName string, partitionID storage.PartitionID, lowWaterMark, highWaterMark storage.LSN) {
	la.notificationsMutex.Lock()
	defer la.notificationsMutex.Unlock()

	la.notifications.PushBack(newPartitionNotification(storageName, partitionID, lowWaterMark, highWaterMark))

	select {
	case la.notificationCh <- struct{}{}:
	// Archiver has a pending notification already, no further action needed.
	default:
	}
}

// Run starts log entry archiving.
func (la *LogEntryArchiver) Run() {
	go func() {
		la.logger.Info("log entry archiver: started")
		defer func() {
			la.notificationsMutex.Lock()
			defer la.notificationsMutex.Unlock()
			la.logger.WithField("pending_entries", la.notifications.Len()).Info("log entry archiver: stopped")
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sendCh := make(chan *logEntry)
		recvCh := make(chan *logEntry)

		var wg sync.WaitGroup
		for i := uint(0); i < la.workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				la.processEntries(ctx, sendCh, recvCh)
			}()
		}

		la.main(ctx, sendCh, recvCh)

		close(sendCh)

		// Interrupt any running backups so we can exit quickly.
		cancel()

		// Wait for all workers to exit.
		wg.Wait()
	}()
}

// Close stops the LogEntryArchiver, causing Run to return.
func (la *LogEntryArchiver) Close() {
	close(la.closingCh)
	<-la.closedCh
}

// main is the main loop of the LogEntryArchiver. New notifications are ingested, jobs
// are sent to workers, and the result of jobs are received.
func (la *LogEntryArchiver) main(ctx context.Context, sendCh, recvCh chan *logEntry) {
	defer close(la.closedCh)

	for {
		// Triggering sendEntries via workCh may not process all entries if there
		// are more active partitions than workers or more than one entry to process
		// in a partition. We will need to call it repeatedly to work through the
		// backlog. If there are no available jobs or workers sendEntries is a no-op.
		la.sendEntries(sendCh)

		select {
		case <-la.workCh:
			la.sendEntries(sendCh)
		case <-la.notificationCh:
			la.ingestNotifications(ctx)
		case entry := <-recvCh:
			la.receiveEntry(ctx, entry)
		case <-la.closingCh:
			return
		}
	}
}

// sendEntries sends available log entries to worker goroutines for processing.
// It may consume up to as many entries as there are available workers.
func (la *LogEntryArchiver) sendEntries(sendCh chan *logEntry) {
	// We use a map to randomize partition processing order. Map access is not
	// truly random or completely fair, but it's close enough for our purposes.
	for partitionInfo := range la.activePartitions {
		// All workers are busy, go back to waiting.
		if la.activeJobs == la.workerCount {
			return
		}

		state := la.partitionStates[partitionInfo]
		if state.hasJob {
			continue
		}

		state.hasJob = true
		sendCh <- newLogEntry(partitionInfo, state.nextLSN)

		la.activeJobs++
	}
}

// ingestNotifications read all new notifications and updates partition states.
func (la *LogEntryArchiver) ingestNotifications(ctx context.Context) {
	for {
		notification := la.popNextNotification()
		if notification == nil {
			return
		}

		state, ok := la.partitionStates[notification.partitionInfo]
		if !ok {
			state = newPartitionState(notification.lowWaterMark, notification.highWaterMark)
			la.partitionStates[notification.partitionInfo] = state
		}

		// We have already backed up all entries sent by the LogManager, but the manager is
		// not aware of this. Acknowledge again with our last processed entry.
		if state.nextLSN > notification.highWaterMark {
			if err := la.callLogReader(ctx, notification.partitionInfo, func(lm storage.LogReader) error {
				return lm.AcknowledgePosition(log.ConsumerPosition, state.nextLSN-1)
			}); err != nil {
				la.logger.WithError(err).Error("log entry archiver: failed to get LogManager for already completed entry")
			}
			continue
		}

		// We expect our next LSN to be at or above the oldest LSN available for backup. If not,
		// we will be unable to backup the full sequence.
		if state.nextLSN < notification.lowWaterMark {
			la.logger.WithFields(
				logging.Fields{
					"storage":      notification.partitionInfo.StorageName,
					"partition_id": notification.partitionInfo.PartitionID,
					"expected_lsn": state.nextLSN,
					"actual_lsn":   notification.lowWaterMark,
				}).Error("log entry archiver: gap in log sequence")

			// The LogManager reports that it no longer has our expected
			// LSN available for consumption. Skip ahead to the oldest entry
			// still present.
			state.nextLSN = notification.lowWaterMark
		}

		state.highWaterMark = notification.highWaterMark

		// Mark partition as active.
		la.activePartitions[notification.partitionInfo] = struct{}{}

		la.notifyNewEntries()
	}
}

func (la *LogEntryArchiver) callLogReader(ctx context.Context, partitionInfo PartitionInfo, callback func(lm storage.LogReader) error) error {
	storageHandle, err := (*la.node).GetStorage(partitionInfo.StorageName)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	partition, err := storageHandle.GetPartition(ctx, partitionInfo.PartitionID)
	if err != nil {
		return fmt.Errorf("get partition: %w", err)
	}
	defer partition.Close()

	if err := callback(partition.GetLogReader()); err != nil {
		return fmt.Errorf("acknowledge consumer position: %w", err)
	}

	return nil
}

// receiveEntry handles the result of a backup job. If the backup failed, then it
// will block for la.waitDur to allow the conditions that caused the failure to resolve
// themselves. Continued failure results in an exponential backoff.
func (la *LogEntryArchiver) receiveEntry(ctx context.Context, entry *logEntry) {
	la.activeJobs--

	state := la.partitionStates[entry.partitionInfo]
	state.hasJob = false

	if !entry.success {
		// It is likely that a problem with one backup will impact others, e.g.
		// connectivity issues with object storage. Wait to avoid a thundering
		// herd of retries.
		la.backoff()

		return
	}

	state.nextLSN++

	// All entries in partition have been backed up, the partition is dormant.
	if state.nextLSN > state.highWaterMark {
		delete(la.activePartitions, entry.partitionInfo)
	}

	// Decrease backoff on success.
	la.waitDur /= 2
	if la.waitDur < minRetryWait {
		la.waitDur = minRetryWait
	}

	if err := la.callLogReader(ctx, entry.partitionInfo, func(lm storage.LogReader) error {
		return lm.AcknowledgePosition(log.ConsumerPosition, entry.lsn)
	}); err != nil {
		la.logger.WithError(err).WithFields(
			logging.Fields{
				"storage":      entry.partitionInfo.StorageName,
				"partition_id": entry.partitionInfo.PartitionID,
				"lsn":          entry.lsn,
			}).Error("log entry archiver: failed to get LogManager for newly completed entry")
	}
}

// processEntries is executed by worker goroutines. This performs the actual backups.
func (la *LogEntryArchiver) processEntries(ctx context.Context, inCh, outCh chan *logEntry) {
	for entry := range inCh {
		la.processEntry(ctx, entry)
		outCh <- entry
	}
}

// processEntry checks if an existing backup exists, and performs a backup if not present.
func (la *LogEntryArchiver) processEntry(ctx context.Context, entry *logEntry) {
	logger := la.logger.WithFields(logging.Fields{
		"storage":      entry.partitionInfo.StorageName,
		"partition_id": entry.partitionInfo.PartitionID,
		"lsn":          entry.lsn,
	})

	var entryPath string
	if err := la.callLogReader(context.Background(), entry.partitionInfo, func(lm storage.LogReader) error {
		entryPath = lm.GetEntryPath(entry.lsn)
		return nil
	}); err != nil {
		la.backupCounter.WithLabelValues("fail").Add(1)
		la.logger.WithError(err).Error("log entry archiver: failed to get LogManager for entry path")
		return
	}

	backupExists, err := la.store.Exists(ctx, entry.partitionInfo, entry.lsn)
	if err != nil {
		la.backupCounter.WithLabelValues("fail").Add(1)
		logger.WithError(err).Error("log entry archiver: checking for existing log entry backup")
		return
	}
	if backupExists {
		// Don't increment backupCounter, we didn't perform a backup.
		entry.success = true
		return
	}

	if err := la.backupLogEntry(ctx, entry.partitionInfo, entry.lsn, entryPath); err != nil {
		la.backupCounter.WithLabelValues("fail").Add(1)
		logger.WithError(err).Error("log entry archiver: failed to backup log entry")
		return
	}

	la.backupCounter.WithLabelValues("success").Add(1)

	entry.success = true
}

// backupLogEntry tar's the root directory of the log entry and writes it to the Sink.
func (la *LogEntryArchiver) backupLogEntry(ctx context.Context, partitionInfo PartitionInfo, lsn storage.LSN, entryPath string) (returnErr error) {
	timer := prometheus.NewTimer(la.backupLatency)
	defer timer.ObserveDuration()

	// Create a new context to abort the write on failure.
	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()

	w, err := la.store.GetWriter(writeCtx, partitionInfo, lsn)
	if err != nil {
		return fmt.Errorf("get backup writer: %w", err)
	}
	defer func() {
		if err := w.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close log entry backup writer: %w", err)
		}
	}()

	entryParent := filepath.Dir(entryPath)
	entryName := filepath.Base(entryPath)

	if err := archive.WriteTarball(writeCtx, la.logger, w, entryParent, entryName); err != nil {
		// End the context before calling Close to ensure we don't persist the failed
		// write to object storage.
		cancelWrite()
		return fmt.Errorf("backup log archive: %w", err)
	}

	return nil
}

// popNextNotification removes the next entry from the head of the list.
func (la *LogEntryArchiver) popNextNotification() *partitionNotification {
	la.notificationsMutex.Lock()
	defer la.notificationsMutex.Unlock()

	front := la.notifications.Front()
	if front == nil {
		return nil
	}

	return la.notifications.Remove(front).(*partitionNotification)
}

// notifyNewEntries alerts the LogEntryArchiver that new entries are available to backup.
func (la *LogEntryArchiver) notifyNewEntries() {
	select {
	case la.workCh <- struct{}{}:
		// There is already a pending notification, proceed.
	default:
	}
}

// backoff sleeps for waitDur and doubles the duration for the next backoff call.
func (la *LogEntryArchiver) backoff() {
	ticker := la.tickerFunc(la.waitDur)
	ticker.Reset()

	select {
	case <-la.closingCh:
		ticker.Stop()
	case <-ticker.C():
	}

	la.waitDur *= 2
	if la.waitDur > maxRetryWait {
		la.waitDur = maxRetryWait
	}
}

// Describe is used to describe Prometheus metrics.
func (la *LogEntryArchiver) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(la, descs)
}

// Collect is used to collect Prometheus metrics.
func (la *LogEntryArchiver) Collect(metrics chan<- prometheus.Metric) {
	la.backupCounter.Collect(metrics)
}

// LogEntryStore manages uploaded log entry archives in object storage.
type LogEntryStore struct {
	sink *Sink
}

// NewLogEntryStore returns a new LogEntryStore.
func NewLogEntryStore(sink *Sink) LogEntryStore {
	return LogEntryStore{
		sink: sink,
	}
}

// Exists returns true if a log entry for the specified partition and LSN exists in the store.
func (s LogEntryStore) Exists(ctx context.Context, info PartitionInfo, lsn storage.LSN) (bool, error) {
	exists, err := s.sink.Exists(ctx, archivePath(info, lsn))
	if err != nil {
		return false, fmt.Errorf("exists: %w", err)
	}
	return exists, nil
}

// GetReader returns a reader in order to read a log entry from the store.
func (s *LogEntryStore) GetReader(ctx context.Context, info PartitionInfo, lsn storage.LSN) (io.ReadCloser, error) {
	r, err := s.sink.GetReader(ctx, archivePath(info, lsn))
	if err != nil {
		return nil, fmt.Errorf("get reader: %w", err)
	}
	return r, nil
}

// GetWriter returns a writer in order to write a new log entry into the store.
func (s LogEntryStore) GetWriter(ctx context.Context, info PartitionInfo, lsn storage.LSN) (io.WriteCloser, error) {
	w, err := s.sink.GetWriter(ctx, archivePath(info, lsn))
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}
	return w, nil
}

// Query returns an iterator that finds all log entries in the store for the
// given partition starting at the LSN specified by from.
func (s LogEntryStore) Query(info PartitionInfo, from storage.LSN) *LogEntryIterator {
	it := s.sink.List(partitionDir(info))
	return &LogEntryIterator{it: it, from: from}
}

// LogEntryIterator iterates over archived log entries in object-storage.
type LogEntryIterator struct {
	it   *ListIterator
	from storage.LSN
	err  error
	lsn  storage.LSN
	path string
}

// Next iterates to the next item. Returns false if there are no more results.
func (it *LogEntryIterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}

	ok := it.it.Next(ctx)
	if !ok {
		return false
	}

	it.lsn, it.err = extractLSN(it.it.Path())
	if it.err != nil {
		return false
	}

	for it.lsn < it.from {
		ok = it.it.Next(ctx)
		if !ok {
			return false
		}

		it.lsn, it.err = extractLSN(it.it.Path())
		if it.err != nil {
			return false
		}
	}

	return true
}

// Err returns a iteration error if there were any.
func (it *LogEntryIterator) Err() error {
	return it.err
}

// Path of the current log entry.
func (it *LogEntryIterator) Path() string {
	return it.path
}

// LSN of the current log entry.
func (it *LogEntryIterator) LSN() storage.LSN {
	return it.lsn
}

func extractLSN(path string) (storage.LSN, error) {
	rawLSN := strings.TrimSuffix(filepath.Base(path), ".tar")
	return storage.ParseLSN(rawLSN)
}

func partitionDir(info PartitionInfo) string {
	return filepath.Join(info.StorageName, fmt.Sprintf("%d", info.PartitionID))
}

func archivePath(info PartitionInfo, lsn storage.LSN) string {
	return filepath.Join(partitionDir(info), lsn.String()+".tar")
}
