package bigcat

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bigcat/internal/storage"
)

// TestCrashWorkerHelper is the child subprocess entrypoint used by SIGKILL chaos tests.
// It does nothing during normal test runs unless BIGCAT_CRASH_WORKER=1 is set.
func TestCrashWorkerHelper(t *testing.T) {
	if os.Getenv("BIGCAT_CRASH_WORKER") != "1" {
		return
	}

	dir := os.Getenv("BIGCAT_CRASH_DIR")
	maxSize, err := strconv.ParseInt(os.Getenv("BIGCAT_CRASH_MAX_FILE_SIZE"), 10, 64)
	if err != nil || maxSize <= 0 {
		maxSize = 256
	}

	db, err := Open(dir, WithMaxFileSize(maxSize), WithSyncOnWrite(true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker Open failed: %v\n", err)
		os.Exit(2)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker Listen failed: %v\n", err)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stdout, "READY %s\n", ln.Addr().String())
	_ = os.Stdout.Sync()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleCrashWorkerConn(db, conn)
	}
}

func handleCrashWorkerConn(db *DB, conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "PUT":
			if len(fields) != 3 {
				_, _ = writer.WriteString("ERR bad_put\n")
				break
			}
			if err := db.Put([]byte(fields[1]), []byte(fields[2])); err != nil {
				_, _ = fmt.Fprintf(writer, "ERR %v\n", err)
			} else {
				_, _ = writer.WriteString("OK\n")
			}

		case "DEL":
			if len(fields) != 2 {
				_, _ = writer.WriteString("ERR bad_del\n")
				break
			}
			if err := db.Delete([]byte(fields[1])); err != nil {
				_, _ = fmt.Fprintf(writer, "ERR %v\n", err)
			} else {
				_, _ = writer.WriteString("OK\n")
			}

		case "BATCH":
			if len(fields) != 5 {
				_, _ = writer.WriteString("ERR bad_batch\n")
				break
			}
			b := NewWriteBatch()
			_ = b.Put([]byte(fields[1]), []byte(fields[2]))
			_ = b.Put([]byte(fields[3]), []byte(fields[4]))
			if err := db.WriteBatch(b); err != nil {
				_, _ = fmt.Fprintf(writer, "ERR %v\n", err)
			} else {
				_, _ = writer.WriteString("OK\n")
			}

		case "GET":
			if len(fields) != 2 {
				_, _ = writer.WriteString("ERR bad_get\n")
				break
			}
			val, err := db.Get([]byte(fields[1]))
			if errors.Is(err, ErrKeyNotFound) {
				_, _ = writer.WriteString("NOTFOUND\n")
			} else if err != nil {
				_, _ = fmt.Fprintf(writer, "ERR %v\n", err)
			} else {
				_, _ = fmt.Fprintf(writer, "VAL %s\n", string(val))
			}

		case "MERGE":
			if err := db.Merge(); err != nil {
				_, _ = fmt.Fprintf(writer, "ERR %v\n", err)
			} else {
				_, _ = writer.WriteString("OK\n")
			}

		default:
			_, _ = writer.WriteString("ERR unknown_cmd\n")
		}
		_ = writer.Flush()
	}
}

// TestCrashFailpointHelper is the subprocess entrypoint for deterministic failpoint crashes.
// It exits abruptly via os.Exit(99) (skipping Close and defers) when the target failpoint hits.
func TestCrashFailpointHelper(t *testing.T) {
	target := os.Getenv("BIGCAT_FAILPOINT")
	if target == "" {
		return
	}

	dir := os.Getenv("BIGCAT_CRASH_DIR")
	action := os.Getenv("BIGCAT_FAILPOINT_ACTION")
	maxSize, err := strconv.ParseInt(os.Getenv("BIGCAT_CRASH_MAX_FILE_SIZE"), 10, 64)
	if err != nil || maxSize <= 0 {
		maxSize = 180
	}

	mergeFailpoint = func(point string) {
		if point == target {
			os.Exit(99)
		}
	}
	defer func() { mergeFailpoint = nil }()

	db, err := Open(dir, WithMaxFileSize(maxSize), WithSyncOnWrite(true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failpoint helper Open(%s) failed: %v\n", action, err)
		os.Exit(2)
	}

	if action == "merge" {
		if err := db.Merge(); err != nil {
			fmt.Fprintf(os.Stderr, "failpoint helper Merge failed: %v\n", err)
			os.Exit(2)
		}
	}
	_ = db.Close()
}

type keyState struct {
	exists bool
	val    string
}

type inflightOp struct {
	active  bool
	isBatch bool
	idx1    int
	op1     keyState
	idx2    int
	op2     keyState
}

func startCrashWorker(t *testing.T, dir string, maxFileSize int64) (*exec.Cmd, string, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorkerHelper$")
	cmd.Env = append(os.Environ(),
		"BIGCAT_CRASH_WORKER=1",
		"BIGCAT_CRASH_DIR="+dir,
		fmt.Sprintf("BIGCAT_CRASH_MAX_FILE_SIZE=%d", maxFileSize),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe failed: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start crash worker: %v", err)
	}

	readyCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		r := bufio.NewReader(stdout)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "READY ") {
				readyCh <- strings.TrimPrefix(line, "READY ")
				return
			}
		}
	}()

	select {
	case addr := <-readyCh:
		return cmd, addr, &stderr
	case err := <-errCh:
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("crash worker exited before READY: %v, stderr=%s", err, stderr.String())
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("timed out waiting for crash worker READY, stderr=%s", stderr.String())
	}
	return nil, "", nil
}

func sendWorkerCmd(conn net.Conn, reader *bufio.Reader, cmdLine string) (string, error) {
	if _, err := conn.Write([]byte(cmdLine + "\n")); err != nil {
		return "", err
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

// TestCrash_RandomSIGKILLChaos spawns live child BigCat processes, hammers them concurrently
// with Put/Delete/WriteBatch/Get/Merge over TCP, abruptly kills them with SIGKILL at random
// intervals across 12 restart cycles, and asserts strict linearizable crash recovery.
func TestCrash_RandomSIGKILLChaos(t *testing.T) {
	dir := t.TempDir()
	const maxFileSize = 256
	const numRounds = 12
	const numWriters = 4
	const keysPerWorker = 20

	seed := time.Now().UnixNano()
	t.Logf("SIGKILL chaos seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	// Ground-truth state per worker across all crash-restart rounds
	acked := make([][]keyState, numWriters)
	for w := 0; w < numWriters; w++ {
		acked[w] = make([]keyState, keysPerWorker)
	}

	for round := 0; round < numRounds; round++ {
		cmd, addr, stderr := startCrashWorker(t, dir, maxFileSize)

		inflight := make([]inflightOp, numWriters)
		var stateMu sync.Mutex
		var stopClients atomic.Bool
		var clientsWg sync.WaitGroup

		// 4 concurrent writers (Put, Delete, atomic WriteBatch)
		for w := 0; w < numWriters; w++ {
			clientsWg.Add(1)
			writerID := w
			workerSeed := rng.Int63()
			go func() {
				defer clientsWg.Done()
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				localRng := rand.New(rand.NewSource(workerSeed))

				for step := 0; !stopClients.Load(); step++ {
					idx := localRng.Intn(keysPerWorker)
					key := fmt.Sprintf("w%d_k%02d", writerID, idx)

					stateMu.Lock()
					cur := acked[writerID][idx]
					stateMu.Unlock()

					var cmdLine string
					var pending inflightOp

					opChoice := localRng.Intn(10)
					if opChoice < 2 && cur.exists {
						// Delete existing key
						cmdLine = fmt.Sprintf("DEL %s", key)
						pending = inflightOp{active: true, idx1: idx, op1: keyState{exists: false}}
					} else if opChoice == 2 {
						// Atomic 2-key WriteBatch
						idx2 := (idx + 1 + localRng.Intn(keysPerWorker-1)) % keysPerWorker
						key2 := fmt.Sprintf("w%d_k%02d", writerID, idx2)
						v1 := fmt.Sprintf("r%d_b1_%d_%d_pad", round, writerID, step)
						v2 := fmt.Sprintf("r%d_b2_%d_%d_pad", round, writerID, step)
						cmdLine = fmt.Sprintf("BATCH %s %s %s %s", key, v1, key2, v2)
						pending = inflightOp{
							active:  true,
							isBatch: true,
							idx1:    idx,
							op1:     keyState{exists: true, val: v1},
							idx2:    idx2,
							op2:     keyState{exists: true, val: v2},
						}
					} else {
						// Put overwrite/insert
						val := fmt.Sprintf("r%d_v_%d_%d_padding_bytes", round, writerID, step)
						cmdLine = fmt.Sprintf("PUT %s %s", key, val)
						pending = inflightOp{active: true, idx1: idx, op1: keyState{exists: true, val: val}}
					}

					stateMu.Lock()
					inflight[writerID] = pending
					stateMu.Unlock()

					resp, err := sendWorkerCmd(conn, reader, cmdLine)
					if err != nil {
						// Connection dropped because child was SIGKILLed mid-op; leave inflight active!
						return
					}
					if resp != "OK" {
						t.Errorf("round %d writer %d unexpected response to %q: %q", round, writerID, cmdLine, resp)
						return
					}

					stateMu.Lock()
					acked[writerID][pending.idx1] = pending.op1
					if pending.isBatch {
						acked[writerID][pending.idx2] = pending.op2
					}
					inflight[writerID] = inflightOp{}
					stateMu.Unlock()
				}
			}()
		}

		// 2 concurrent readers checking live Gets never see corrupted payloads
		for r := 0; r < 2; r++ {
			clientsWg.Add(1)
			rSeed := rng.Int63()
			go func() {
				defer clientsWg.Done()
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				localRng := rand.New(rand.NewSource(rSeed))

				for !stopClients.Load() {
					w := localRng.Intn(numWriters)
					k := localRng.Intn(keysPerWorker)
					resp, err := sendWorkerCmd(conn, reader, fmt.Sprintf("GET w%d_k%02d", w, k))
					if err != nil {
						return
					}
					if resp != "NOTFOUND" && !strings.HasPrefix(resp, "VAL ") {
						t.Errorf("round %d live GET unexpected response: %q", round, resp)
						return
					}
				}
			}()
		}

		// 1 concurrent merger hammering Merge() repeatedly while writers & readers run
		clientsWg.Add(1)
		go func() {
			defer clientsWg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			for !stopClients.Load() {
				resp, err := sendWorkerCmd(conn, reader, "MERGE")
				if err != nil {
					return
				}
				if resp != "OK" {
					t.Errorf("round %d live MERGE unexpected response: %q", round, resp)
					return
				}
			}
		}()

		// Let chaos run for random 20..75ms, then SIGKILL abruptly!
		killDelay := time.Duration(20+rng.Intn(55)) * time.Millisecond
		time.Sleep(killDelay)

		stopClients.Store(true)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("round %d Process.Kill failed: %v", round, err)
		}
		_ = cmd.Wait()
		clientsWg.Wait()

		if stderr.Len() > 0 {
			t.Fatalf("round %d worker stderr not empty: %s", round, stderr.String())
		}

		// Reopen DB directly from disk and verify every single key + WriteBatch atomicity
		db, err := Open(dir, WithMaxFileSize(maxFileSize), WithSyncOnWrite(true))
		if err != nil {
			t.Fatalf("round %d recovery Open failed: %v", round, err)
		}

		if _, err := os.Stat(filepath.Join(dir, mergeTempDirName)); !errors.Is(err, os.ErrNotExist) {
			_ = db.Close()
			t.Fatalf("round %d expected .tmp to be cleaned after Open, got %v", round, err)
		}

		for w := 0; w < numWriters; w++ {
			inf := inflight[w]

			readKey := func(idx int) keyState {
				got, err := db.Get([]byte(fmt.Sprintf("w%d_k%02d", w, idx)))
				if errors.Is(err, ErrKeyNotFound) {
					return keyState{exists: false}
				}
				if err != nil {
					_ = db.Close()
					t.Fatalf("round %d Get(w%d_k%02d) failed after recovery: %v", round, w, idx, err)
				}
				return keyState{exists: true, val: string(got)}
			}

			for k := 0; k < keysPerWorker; k++ {
				if inf.active && (k == inf.idx1 || (inf.isBatch && k == inf.idx2)) {
					continue
				}
				actual := readKey(k)
				if actual != acked[w][k] {
					_ = db.Close()
					t.Fatalf("round %d acked key w%d_k%02d mismatch: got=%+v want=%+v", round, w, k, actual, acked[w][k])
				}
			}

			if inf.active && !inf.isBatch {
				actual := readKey(inf.idx1)
				if actual != acked[w][inf.idx1] && actual != inf.op1 {
					_ = db.Close()
					t.Fatalf("round %d inflight single op w%d_k%02d corrupted: got=%+v acked=%+v inflight=%+v",
						round, w, inf.idx1, actual, acked[w][inf.idx1], inf.op1)
				}
				acked[w][inf.idx1] = actual
			} else if inf.active && inf.isBatch {
				a1 := readKey(inf.idx1)
				a2 := readKey(inf.idx2)
				keptOld := a1 == acked[w][inf.idx1] && a2 == acked[w][inf.idx2]
				appliedBatch := a1 == inf.op1 && a2 == inf.op2
				if !keptOld && !appliedBatch {
					_ = db.Close()
					t.Fatalf("round %d TORN WriteBatch on w%d (%d,%d): got=(%+v, %+v), acked=(%+v, %+v), batch=(%+v, %+v)",
						round, w, inf.idx1, inf.idx2, a1, a2, acked[w][inf.idx1], acked[w][inf.idx2], inf.op1, inf.op2)
				}
				acked[w][inf.idx1] = a1
				acked[w][inf.idx2] = a2
			}
		}

		// Periodically verify Merge() succeeds cleanly after recovery
		if round%3 == 2 {
			if err := db.Merge(); err != nil {
				_ = db.Close()
				t.Fatalf("round %d post-recovery Merge failed: %v", round, err)
			}
		}

		if err := db.Close(); err != nil {
			t.Fatalf("round %d Close failed: %v", round, err)
		}
	}
}

func runFailpointSubprocess(t *testing.T, dir string, maxFileSize int64, action, point string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashFailpointHelper$")
	cmd.Env = append(os.Environ(),
		"BIGCAT_FAILPOINT="+point,
		"BIGCAT_FAILPOINT_ACTION="+action,
		"BIGCAT_CRASH_DIR="+dir,
		fmt.Sprintf("BIGCAT_CRASH_MAX_FILE_SIZE=%d", maxFileSize),
	)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 99 {
		t.Fatalf("failpoint (%s, %s) expected exit code 99, got err=%v, output=%s", action, point, err, string(out))
	}
}

// TestCrash_DeterministicMergeFailpoints hard-exits (os.Exit(99)) a real subprocess at
// every Phase 2/3/4 boundary AND inside recoverMergeState (double crashes), then verifies
// full data integrity and cleanup upon reopening.
func TestCrash_DeterministicMergeFailpoints(t *testing.T) {
	const maxFileSize = 180

	scenarios := []struct {
		name            string
		firstFailpoint  string
		secondFailpoint string // optional chained crash inside Open() -> recoverMergeState
	}{
		{name: "crash mid stageMergedFiles", firstFailpoint: "stage_mid_copy"},
		{name: "crash after manifest before COMMITTED", firstFailpoint: "commit_after_manifest"},
		{name: "crash right after COMMITTED", firstFailpoint: "commit_after_committed"},
		{name: "crash before deleting old files", firstFailpoint: "swap_before_delete_old"},
		{name: "crash after deleting old files before OLD_DELETED", firstFailpoint: "swap_after_delete_old"},
		{name: "crash after OLD_DELETED before renames", firstFailpoint: "swap_after_old_deleted"},
		{name: "crash mid-rename of new compacted files", firstFailpoint: "swap_mid_rename"},
		{name: "crash after AdoptOlderFiles before CAS", firstFailpoint: "swap_after_adopt"},
		{name: "crash after CAS before .tmp cleanup", firstFailpoint: "swap_after_cas"},
		// Chained crashes: crash inside Merge(), then crash AGAIN inside recoverMergeState!
		{
			name:            "double crash: commit_after_committed then recover_after_delete_old",
			firstFailpoint:  "commit_after_committed",
			secondFailpoint: "recover_after_delete_old",
		},
		{
			name:            "double crash: commit_after_committed then recover_after_old_deleted",
			firstFailpoint:  "commit_after_committed",
			secondFailpoint: "recover_after_old_deleted",
		},
		{
			name:            "double crash: swap_after_old_deleted then recover_mid_rename",
			firstFailpoint:  "swap_after_old_deleted",
			secondFailpoint: "recover_mid_rename",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, WithMaxFileSize(maxFileSize), WithSyncOnWrite(true))
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			// Create >= 3 sealed files with overwrites & deletions + 2 newIDs upon compaction,
			// plus a key in the active file.
			for i := 0; i < 8; i++ {
				k := []byte(fmt.Sprintf("fp_key_%02d", i))
				v := bytes.Repeat([]byte{byte('A' + i)}, 40)
				if err := db.Put(k, v); err != nil {
					t.Fatalf("Put %d failed: %v", i, err)
				}
			}
			for i := 0; i < 2; i++ {
				if err := db.Delete([]byte(fmt.Sprintf("fp_key_%02d", i))); err != nil {
					t.Fatalf("Delete %d failed: %v", i, err)
				}
			}
			if err := db.storage.Rotate(); err != nil {
				t.Fatalf("Rotate failed: %v", err)
			}
			if err := db.Put([]byte("active_only"), []byte("active_payload")); err != nil {
				t.Fatalf("Put active failed: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			// Crash #1 inside Merge()
			runFailpointSubprocess(t, dir, maxFileSize, "merge", sc.firstFailpoint)

			// Optional Crash #2 inside Open() -> recoverMergeState()
			if sc.secondFailpoint != "" {
				runFailpointSubprocess(t, dir, maxFileSize, "open", sc.secondFailpoint)
			}

			// Final Open() must heal everything cleanly
			db2, err := Open(dir, WithMaxFileSize(maxFileSize), WithSyncOnWrite(true))
			if err != nil {
				t.Fatalf("final recovery Open failed: %v", err)
			}
			defer db2.Close()

			if _, err := os.Stat(filepath.Join(dir, mergeTempDirName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("expected .tmp cleaned up after recovery, got %v", err)
			}

			sealedIDs, _ := db2.storage.SealedSnapshot()
			rolledBack := sc.firstFailpoint == "stage_mid_copy" || sc.firstFailpoint == "commit_after_manifest"
			for _, id := range sealedIDs {
				_, hintErr := os.Stat(filepath.Join(dir, storage.HintFileName(id)))
				if rolledBack && !errors.Is(hintErr, os.ErrNotExist) {
					t.Fatalf("expected no .hint for rolled-back sealed file %d, got %v", id, hintErr)
				}
				if !rolledBack && hintErr != nil {
					t.Fatalf("expected companion .hint for rolled-forward sealed file %d: %v", id, hintErr)
				}
			}

			for i := 0; i < 2; i++ {
				if _, err := db2.Get([]byte(fmt.Sprintf("fp_key_%02d", i))); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("deleted key fp_key_%02d resurrected or errored: %v", i, err)
				}
			}
			for i := 2; i < 8; i++ {
				k := []byte(fmt.Sprintf("fp_key_%02d", i))
				want := bytes.Repeat([]byte{byte('A' + i)}, 40)
				got, err := db2.Get(k)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("Get(%s) mismatch: got=%q err=%v", k, got, err)
				}
			}
			if got, err := db2.Get([]byte("active_only")); err != nil || !bytes.Equal(got, []byte("active_payload")) {
				t.Fatalf("Get(active_only) mismatch: got=%q err=%v", got, err)
			}

			// Verify DB remains healthy for new writes + another Merge after recovery
			if err := db2.Put([]byte("post_crash"), []byte("healthy")); err != nil {
				t.Fatalf("Put after crash recovery failed: %v", err)
			}
			if err := db2.Merge(); err != nil {
				t.Fatalf("Merge after crash recovery failed: %v", err)
			}
		})
	}
}

// TestCrash_SIGKILLDuringLargeMergeBurst populates a multi-file database (~250 keys across
// ~15+ rotated files), triggers Merge while mutating active-file keys, kills the process
// with SIGKILL mid-Merge across 6 iterations, and verifies zero data loss on recovery.
func TestCrash_SIGKILLDuringLargeMergeBurst(t *testing.T) {
	dir := t.TempDir()
	const maxFileSize = 256
	const numKeys = 250

	seed := time.Now().UnixNano()
	t.Logf("SIGKILL large-merge seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	// Pre-populate multi-file database
	db, err := Open(dir, WithMaxFileSize(maxFileSize), WithSyncOnWrite(true))
	if err != nil {
		t.Fatalf("initial Open failed: %v", err)
	}
	acked := make([]keyState, numKeys)
	for i := 0; i < numKeys; i++ {
		val := fmt.Sprintf("init_%03d_padding_for_rotation", i)
		if err := db.Put([]byte(fmt.Sprintf("m_%03d", i)), []byte(val)); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
		acked[i] = keyState{exists: true, val: val}
	}
	for i := 0; i < 80; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("m_%03d", i))); err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
		acked[i] = keyState{exists: false}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("initial Close failed: %v", err)
	}

	for iter := 0; iter < 6; iter++ {
		cmd, addr, stderr := startCrashWorker(t, dir, maxFileSize)

		var inflightMu sync.Mutex
		var inflightIdx = -1
		var inflightState keyState
		var stopWriters atomic.Bool
		var wg sync.WaitGroup

		// Active writer mutating keys while Merge runs
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			for step := 0; !stopWriters.Load(); step++ {
				idx := (iter*37 + step) % numKeys
				val := fmt.Sprintf("burst_%d_%d_pad", iter, step)
				inflightMu.Lock()
				inflightIdx = idx
				inflightState = keyState{exists: true, val: val}
				inflightMu.Unlock()

				resp, err := sendWorkerCmd(conn, reader, fmt.Sprintf("PUT m_%03d %s", idx, val))
				if err != nil {
					return
				}
				if resp != "OK" {
					t.Errorf("burst PUT failed: %s", resp)
					return
				}

				inflightMu.Lock()
				acked[idx] = keyState{exists: true, val: val}
				inflightIdx = -1
				inflightMu.Unlock()
			}
		}()

		// Trigger heavy Merge() and SIGKILL at a short random delay
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			_, _ = sendWorkerCmd(conn, reader, "MERGE")
		}()

		time.Sleep(time.Duration(5+rng.Intn(30)) * time.Millisecond)
		stopWriters.Store(true)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		wg.Wait()

		if stderr.Len() > 0 {
			t.Fatalf("iter %d stderr not empty: %s", iter, stderr.String())
		}

		reopened, err := Open(dir, WithMaxFileSize(maxFileSize), WithSyncOnWrite(true))
		if err != nil {
			t.Fatalf("iter %d recovery Open failed: %v", iter, err)
		}

		for i := 0; i < numKeys; i++ {
			got, err := reopened.Get([]byte(fmt.Sprintf("m_%03d", i)))
			var actual keyState
			if errors.Is(err, ErrKeyNotFound) {
				actual = keyState{exists: false}
			} else if err != nil {
				_ = reopened.Close()
				t.Fatalf("iter %d Get(m_%03d) failed: %v", iter, i, err)
			} else {
				actual = keyState{exists: true, val: string(got)}
			}

			if i == inflightIdx {
				if actual != acked[i] && actual != inflightState {
					_ = reopened.Close()
					t.Fatalf("iter %d inflight m_%03d corrupted: got=%+v acked=%+v inflight=%+v", iter, i, actual, acked[i], inflightState)
				}
				acked[i] = actual
			} else if actual != acked[i] {
				_ = reopened.Close()
				t.Fatalf("iter %d acked m_%03d mismatch: got=%+v want=%+v", iter, i, actual, acked[i])
			}
		}

		if err := reopened.Close(); err != nil {
			t.Fatalf("iter %d Close failed: %v", iter, err)
		}
	}
}
