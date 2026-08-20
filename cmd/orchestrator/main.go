// orchestrator is a control-plane process for the flotilla dashboard. It
// is not part of Raft itself — it never votes, replicates, or applies
// anything. All it does is spawn/kill real flotillanode OS processes,
// poll their /debug/status over HTTP, and proxy client-facing calls
// (/submit, /debug/get), so a browser frontend has one origin to talk to
// and a "kill" button has an actual process to send SIGKILL to.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type nodeConfig struct {
	ID   string
	Addr string // ":8001", what the node itself binds to
}

// fleetSize is the whole cluster: nodeN binds to 8000+N. Raft's quorum
// math (majority := (len(peers)+1)/2 + 1) is already size-agnostic, so
// this is the only place the node count actually lives.
const fleetSize = 10

var nodes = func() []nodeConfig {
	ns := make([]nodeConfig, fleetSize)
	for i := range ns {
		n := i + 1
		ns[i] = nodeConfig{ID: fmt.Sprintf("node%d", n), Addr: fmt.Sprintf(":%d", 8000+n)}
	}
	return ns
}()

func httpAddr(addr string) string { return "localhost" + addr }

func peersFor(self string) string {
	var b strings.Builder
	for _, n := range nodes {
		if n.ID == self {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%s", n.ID, httpAddr(n.Addr))
	}
	return b.String()
}

const runDir = "orchestrator-run"

var binPath string // absolute path to the built flotillanode binary

// process tracking — guarded by procMu, separate from the polled-status
// cache below since one is about OS processes and the other is about
// Raft-level state observed over HTTP.
var (
	procMu sync.Mutex
	procs  = map[string]*exec.Cmd{}
)

func startNode(id string) error {
	procMu.Lock()
	defer procMu.Unlock()
	if cmd, ok := procs[id]; ok && cmd.ProcessState == nil {
		return fmt.Errorf("%s is already running", id)
	}

	var addr string
	for _, n := range nodes {
		if n.ID == id {
			addr = n.Addr
		}
	}
	if addr == "" {
		return fmt.Errorf("unknown node %q", id)
	}

	logFile, err := os.Create(filepath.Join(runDir, id+".log"))
	if err != nil {
		return err
	}

	cmd := exec.Command(binPath, "-id="+id, "-addr="+addr, "-peers="+peersFor(id))
	cmd.Dir = runDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	procs[id] = cmd
	go func() { cmd.Wait(); logFile.Close() }() // reap, avoid zombies, regardless of how it exits
	return nil
}

func killNode(id string) error {
	procMu.Lock()
	cmd, ok := procs[id]
	procMu.Unlock()
	if !ok || cmd.Process == nil || cmd.ProcessState != nil {
		return fmt.Errorf("%s is not running", id)
	}
	return cmd.Process.Kill()
}

// --- live status polling + fun event feed ---

type nodeStatus struct {
	ID          string `json:"id"`
	Addr        string `json:"addr"`
	Alive       bool   `json:"alive"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	CommitIndex int    `json:"commitIndex"`
	LastApplied int    `json:"lastApplied"`
	LogLength   int    `json:"logLength"`
	LeaderID    string `json:"leaderId"`
}

type event struct {
	Seq     int    `json:"seq"`
	Ts      int64  `json:"ts"`
	Message string `json:"message"`
}

var (
	statusMu     sync.Mutex
	latestStatus = map[string]nodeStatus{}
	events       []event
	nextSeq      = 1
)

var statusClient = &http.Client{Timeout: 300 * time.Millisecond}

func fetchStatus(n nodeConfig) nodeStatus {
	resp, err := statusClient.Get("http://" + httpAddr(n.Addr) + "/debug/status")
	if err != nil {
		return nodeStatus{ID: n.ID, Addr: n.Addr, Alive: false}
	}
	defer resp.Body.Close()
	var s StatusReplyDTO
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nodeStatus{ID: n.ID, Addr: n.Addr, Alive: false}
	}
	return nodeStatus{
		ID: n.ID, Addr: n.Addr, Alive: true,
		State: s.State, Term: s.Term, CommitIndex: s.CommitIndex,
		LastApplied: s.LastApplied, LogLength: s.LogLength, LeaderID: s.LeaderID,
	}
}

// StatusReplyDTO mirrors raft.StatusReply's JSON shape without importing
// the raft package — this binary only ever talks to nodes over HTTP,
// deliberately, so it can kill -9 one without taking itself down too.
type StatusReplyDTO struct {
	ID          string
	State       string
	Term        int
	CommitIndex int
	LastApplied int
	LogLength   int
	LeaderID    string
}

func emit(format string, args ...interface{}) {
	statusMu.Lock()
	defer statusMu.Unlock()
	events = append(events, event{Seq: nextSeq, Ts: time.Now().UnixMilli(), Message: fmt.Sprintf(format, args...)})
	nextSeq++
	if len(events) > 200 {
		events = events[len(events)-200:]
	}
}

func pollLoop() {
	for {
		for _, n := range nodes {
			fresh := fetchStatus(n)

			statusMu.Lock()
			old, hadOld := latestStatus[n.ID]
			latestStatus[n.ID] = fresh
			statusMu.Unlock()

			if hadOld {
				diffEvents(old, fresh)
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
}

func diffEvents(old, fresh nodeStatus) {
	switch {
	case old.Alive && !fresh.Alive:
		emit("A cannon finds its mark. %s sinks beneath the waves.", fresh.ID)
	case !old.Alive && fresh.Alive:
		emit("%s rises from the depths, timbers creaking, ready to sail again.", fresh.ID)
	}
	if fresh.Alive && fresh.State == "Leader" && old.State != "Leader" {
		emit("%s takes the helm as Captain of term %d.", fresh.ID, fresh.Term)
	}
	if fresh.Alive && old.Alive && fresh.Term > old.Term && fresh.State != "Leader" {
		emit("Mutiny in the ranks. %s's clock advances to term %d.", fresh.ID, fresh.Term)
	}
}

// --- HTTP surface for the frontend ---

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func handleNodes(w http.ResponseWriter, r *http.Request) {
	statusMu.Lock()
	defer statusMu.Unlock()
	out := make([]nodeStatus, 0, len(nodes))
	for _, n := range nodes {
		if s, ok := latestStatus[n.ID]; ok {
			out = append(out, s)
		} else {
			out = append(out, nodeStatus{ID: n.ID, Addr: n.Addr, Alive: false})
		}
	}
	writeJSON(w, out)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.Atoi(r.URL.Query().Get("after"))
	statusMu.Lock()
	defer statusMu.Unlock()
	out := make([]event, 0)
	for _, e := range events {
		if e.Seq > after {
			out = append(out, e)
		}
	}
	writeJSON(w, out)
}

// nodeIDFromPath extracts the {id} segment from "/api/nodes/{id}/action".
func nodeIDFromPath(prefix, path string) string {
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.SplitN(rest, "/", 2)
	return parts[0]
}

func handleKill(w http.ResponseWriter, r *http.Request) {
	id := nodeIDFromPath("/api/nodes/", strings.TrimSuffix(r.URL.Path, "/kill"))
	if err := killNode(id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func handleStart(w http.ResponseWriter, r *http.Request) {
	id := nodeIDFromPath("/api/nodes/", strings.TrimSuffix(r.URL.Path, "/start"))
	if err := startNode(id); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

// handleReset is the closest thing to a "reset term" button that's
// actually safe: term is persisted to disk alongside the committed log
// and votedFor (see raft.Storage), there's no way to zero just the term
// without risking the one-vote-per-term safety guarantee. So this kills
// every ship, deletes every node's on-disk state, and relaunches the
// whole fleet fresh, a real coordinated restart, not a partial edit. It
// deliberately also wipes committed cargo; there's no other honest way
// to make the term number small again.
func handleReset(w http.ResponseWriter, r *http.Request) {
	procMu.Lock()
	for id, cmd := range procs {
		if cmd.Process != nil && cmd.ProcessState == nil {
			cmd.Process.Kill()
		}
		delete(procs, id)
	}
	procMu.Unlock()

	time.Sleep(300 * time.Millisecond) // let the OS actually reclaim the old ports/db files

	for _, n := range nodes {
		os.Remove(filepath.Join(runDir, n.ID+".db"))
	}

	statusMu.Lock()
	latestStatus = map[string]nodeStatus{}
	statusMu.Unlock()

	for _, n := range nodes {
		if err := startNode(n.ID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	emit("⚓ The fleet regroups at port. Every watch resets to one, every hold emptied.")
	writeJSON(w, map[string]bool{"ok": true})
}

func handleSubmitProxy(w http.ResponseWriter, r *http.Request) {
	id := nodeIDFromPath("/api/nodes/", strings.TrimSuffix(r.URL.Path, "/submit"))
	var addr string
	for _, n := range nodes {
		if n.ID == id {
			addr = n.Addr
		}
	}
	if addr == "" {
		http.Error(w, "unknown node", http.StatusBadRequest)
		return
	}
	body, _ := io.ReadAll(r.Body)
	resp, err := http.Post("http://"+httpAddr(addr)+"/submit", "application/json", strings.NewReader(string(body)))
	if err != nil {
		writeJSON(w, map[string]interface{}{"Success": false, "Error": "connection failed"})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var cmd struct{ Op, Key, Value string }
	json.Unmarshal(body, &cmd)
	var reply struct{ Success bool }
	json.Unmarshal(respBody, &reply)
	if reply.Success {
		switch cmd.Op {
		case "DELETE":
			emit("%s heaves \"%s\" overboard, the fleet confirms it's gone.", id, cmd.Key)
		default:
			emit("📦 %s stows \"%s\" = \"%s\" in the hold, confirmed by a majority.", id, cmd.Key, cmd.Value)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBody)
}

func handleGetProxy(w http.ResponseWriter, r *http.Request) {
	id := nodeIDFromPath("/api/nodes/", strings.TrimSuffix(r.URL.Path, "/get"))
	var addr string
	for _, n := range nodes {
		if n.ID == id {
			addr = n.Addr
		}
	}
	if addr == "" {
		http.Error(w, "unknown node", http.StatusBadRequest)
		return
	}
	key := r.URL.Query().Get("key")
	resp, err := http.Get("http://" + httpAddr(addr) + "/debug/get?key=" + key)
	if err != nil {
		writeJSON(w, map[string]interface{}{"Success": false, "Error": "connection failed"})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var reply struct {
		Success bool
		Found   bool
	}
	json.Unmarshal(respBody, &reply)
	if reply.Success && reply.Found {
		emit("🔭 %s hauls up \"%s\" from the depths and shows it to the crew.", id, key)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBody)
}

func main() {
	wd, _ := os.Getwd()
	binPath = filepath.Join(wd, runDir, "flotillanode")

	os.RemoveAll(runDir)
	os.MkdirAll(runDir, 0755)

	log.Println("building flotillanode...")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/flotillanode")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		log.Fatalf("build failed: %v", err)
	}

	for _, n := range nodes {
		if err := startNode(n.ID); err != nil {
			log.Fatalf("failed to start %s: %v", n.ID, err)
		}
	}
	emit("The fleet sets sail. %d ships launched, awaiting a Captain.", fleetSize)

	go pollLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/nodes", handleNodes)
	mux.HandleFunc("/api/events", handleEvents)
	mux.HandleFunc("/api/reset", handleReset)
	mux.HandleFunc("/api/nodes/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/kill"):
			handleKill(w, r)
		case strings.HasSuffix(r.URL.Path, "/start"):
			handleStart(w, r)
		case strings.HasSuffix(r.URL.Path, "/submit"):
			handleSubmitProxy(w, r)
		case strings.HasSuffix(r.URL.Path, "/get"):
			handleGetProxy(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "7500"
	}
	addr := ":" + port
	log.Printf("orchestrator listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, withCORS(mux)))
}

// withCORS lets the dashboard live on a different origin than the
// orchestrator, e.g. a Vercel-hosted frontend calling a Render-hosted
// backend. Set ALLOWED_ORIGIN to lock this down to the real frontend URL
// in production; it defaults to "*" so local/demo setups work unconfigured.
func withCORS(next http.Handler) http.Handler {
	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	if allowedOrigin == "" {
		allowedOrigin = "*"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
