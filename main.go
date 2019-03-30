package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// node represents a Linux system containing a mounted BTRFS
type node struct {
	address       string         // address of the system (either IP or hostname)
	sshPort       int            // SSH port (0 for localhost)
	mountPoint    string         // BTRFS mount point
	snapshotPath  string         // directory containing snapshots relative to mount point
	snapshotRegex *regexp.Regexp // used to match snapshots
	executor      executor       // used to run commands
}

func main() {
	dryRun := flag.Bool("n", false, "dry run")
	dst := flag.String("dst", "", "destination host:port/path")
	dstSnapshotPath := flag.String("dst-snapshot-path", "", "directory containing snapshots relative to mount point")
	verbose := flag.Bool("v", false, "verbose output")
	progress := flag.Bool("progress", false, "show transfer progress")
	flag.Parse()

	defaultExecutor.verbose = *verbose
	defaultExecutor.logProgress = *progress

	snapshotRegex := regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`)
	source := node{
		address:       "localhost",
		sshPort:       0,
		mountPoint:    "/mnt",
		snapshotPath:  "snapshot",
		snapshotRegex: snapshotRegex,
		executor:      defaultExecutor,
	}

	destination, err := parseNode(*dst)
	if err != nil {
		log.Fatal(err)
	}

	destination.snapshotPath = *dstSnapshotPath
	destination.snapshotRegex = snapshotRegex
	destination.executor = defaultExecutor

	sourceSnapshots, err := source.getSnapshots()
	if err != nil {
		log.Fatalf("failed to get local snapshots: %v", err)
	}
	destinationSnapshots, err := destination.getSnapshots()
	if err != nil {
		log.Fatalf("failed to get remote snapshots: %v", err)
	}

	if *verbose {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "Source snapshots:\n")
		for _, s := range sourceSnapshots {
			fmt.Fprintf(&buf, "  %s\n", s)
		}
		fmt.Fprintf(&buf, "Destination snapshots:\n")
		for _, s := range destinationSnapshots {
			fmt.Fprintf(&buf, "  %s\n", s)
		}
		log.Println(buf.String())
	}

	if err := transmitSnapshots(&source, &destination, sourceSnapshots, destinationSnapshots, *dryRun); err != nil {
		log.Fatal(err)
	}
}

func parseNode(str string) (node, error) {
	destinationRegexp := regexp.MustCompile(`^([a-z0-9\-\.]+):([0-9]+)(\/[a-zA-Z0-9\-\.\/]+)$`)
	matches := destinationRegexp.FindStringSubmatch(str)
	if len(matches) != 4 {
		return node{}, fmt.Errorf("invalid node: %s", str)
	}

	port, err := strconv.Atoi(matches[2])
	if err != nil {
		return node{}, fmt.Errorf("invalid node: %s", str)
	}

	return node{
		address: matches[1],
		sshPort: port,
		mountPoint: matches[3],
	}, nil
}

func transmitSnapshots(source, destination *node, localSnapshots, remoteSnapshots []string, dryRun bool) error {
	mostRecentRemote := remoteSnapshots[len(remoteSnapshots)-1]
	previousSnapshot := ""

	for _, snapshot := range localSnapshots {
		if previousSnapshot != "" {
			err := sendSnapshot(source, destination, snapshot, previousSnapshot, dryRun)
			if err != nil {
				log.Printf("Sending %s failed. Attempting to delete snapshot at destination...", snapshot)
				if err := destination.deleteSnapshots([]string{snapshot}); err != nil {
					log.Printf("Deleting snasphot failed: %v", err)
				}
				return fmt.Errorf("transmitSnapshots: %v", err)
			}
			previousSnapshot = snapshot
		} else if snapshot == mostRecentRemote {
			previousSnapshot = mostRecentRemote
		}
	}

	return nil
}

func sendSnapshot(source, destination *node, snapshot, previousSnapshot string, dryRun bool) error {
	p := path.Join(source.mountPoint, source.snapshotPath, previousSnapshot)
	s := path.Join(source.mountPoint, source.snapshotPath, snapshot)

	sendCmd := []string{"btrfs", "send", "--quiet", "-p", p, s}
	if source.sshPort != 0 {
		sendCmd = sshCmd(source, sendCmd)
	}
	receiveCmd := []string{"btrfs", "receive", destination.mountPoint}
	if destination.sshPort != 0 {
		receiveCmd = sshCmd(destination, receiveCmd)
	}

	log.Printf("Sending %s", snapshot)

	if dryRun {
		return nil
	}

	_, transmitted, err := source.executor.exec([][]string{sendCmd, receiveCmd})
	if err != nil {
		return fmt.Errorf("sendSnapshot: %v", err)
	}

	log.Printf("Sending %s done: %s transmitted", snapshot, formatBytes(transmitted))

	return nil
}

// getSnapshots returns a sorted list of snapshots.
func (n *node) getSnapshots() ([]string, error) {
	cmd := []string{"btrfs", "subvolume", "list", n.mountPoint}
	if n.sshPort != 0 {
		cmd = sshCmd(n, cmd)
	}

	out, _, err := n.executor.exec([][]string{cmd})
	if err != nil {
		return nil, err
	}

	subVolumes, err := parseSubVolumes(out)
	if err != nil {
		return nil, err
	}
	snapshots := filterSnapshots(subVolumes, n.snapshotPath, n.snapshotRegex)
	sort.Strings(snapshots)
	return snapshots, nil
}

func (n *node) deleteSnapshots(snapshots []string) error {
	if len(snapshots) == 0 {
		return nil
	}
	cmd := []string{"btrfs", "subvolume", "delete"}
	cmd = append(cmd, snapshots...)
	if n.sshPort != 0 {
		cmd = sshCmd(n, cmd)
	}
	_, _, err := n.executor.exec([][]string{cmd})
	return err
}

// parseSubVolumes extracts the sub-volume names from the "btrfs subvolume list" command.
func parseSubVolumes(out string) ([]string, error) {
	var names []string
	for _, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		tokens := strings.Split(string(line), " ")
		if len(tokens) != 9 {
			return nil, fmt.Errorf("parseSubVolumes: unexpected btrfs output: %s", line)
		}
		names = append(names, strings.TrimRight(tokens[8], "\n"))
	}

	return names, nil
}

// filterSnapshots returns all snapshots from the list of sub-volumes. It filters by snapshotDir and the regex r.
func filterSnapshots(subVolumes []string, snapshotDir string, r *regexp.Regexp) []string {
	snapshotDir = path.Clean(snapshotDir)
	var snapshots []string
	for _, volume := range subVolumes {
		dir, name := path.Split(volume)
		if path.Clean(dir) != snapshotDir {
			continue
		}
		if !r.MatchString(name) {
			continue
		}
		snapshots = append(snapshots, name)
	}
	return snapshots
}

// executor allows to execute commands as new processes. Its main purpose is to mock execution for testing.
type executor interface {
	exec(cmds [][]string) (string, int, error)
}

type executorImpl struct{
	verbose bool
	logProgress bool
}

var defaultExecutor = executorImpl{}

func (e executorImpl) exec(cmds [][]string) (string, int, error) {
	if e.verbose {
		log.Printf("exec: %#v", cmds)
	}

	var cs []*exec.Cmd
	var out bytes.Buffer
	var errs []error
	var pipes []*meteredPipe

	for i, cmd := range cmds {
		c := exec.Command(cmd[0], cmd[1:]...)

		if len(cs) > 0 {
			pipe, err := cs[len(cs)-1].StdoutPipe()
			if err != nil {
				return "", 0, fmt.Errorf("execPipe: StdoutPipe: %v", err)
			}
			meteredPipe := &meteredPipe{r: pipe, logProgress: e.logProgress}
			pipes = append(pipes, meteredPipe)
			c.Stdin = meteredPipe
		}
		if i == len(cmds)-1 {
			c.Stdout = &out
		}
		c.Stderr = os.Stderr

		cs = append(cs, c)
	}

	for _, c := range cs {
		if err := c.Start(); err != nil {
			errs = append(errs, err)
		}
	}

	// Wait() must be called in reverse because all reads from the stdout pipe must be completed before calling it.
	// See StdoutPipe(): "[...] it is incorrect to call Wait before all reads from the pipe have completed."
	for i := len(cs) - 1; i >= 0; i-- {
		if err := cs[i].Wait(); err != nil {
			errs = append(errs, err)
		}
	}

	// take the maximum of data transmitted through the pipes
	transmitted := 0
	for _, p := range pipes {
		if p.meter > transmitted {
			transmitted = p.meter
		}
	}

	if len(errs) > 0 {
		return "", transmitted, fmt.Errorf("%+v", errs)
	}

	return out.String(), transmitted, nil
}

type meteredPipe struct {
	r     io.ReadCloser
	meter int

	// logging
	logProgress bool
	lastLog time.Time
	lastLogMeter int
}

func (m *meteredPipe) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	m.meter += n

	if !m.logProgress {
		return n, err
	}
	if m.lastLog.IsZero() {
		m.lastLog = time.Now()
		return n, err
	}
	if time.Since(m.lastLog) > time.Second {
		log.Printf("Transmitted %s", formatBytes(m.meter - m.lastLogMeter))
		m.lastLogMeter = m.meter
		m.lastLog = time.Now()
	}

	return n, err
}

func (m *meteredPipe) Close() error {
	return m.r.Close()
}

func sshCmd(n *node, remoteCmd []string) []string {
	cmd := []string{"ssh", fmt.Sprintf("-p%d", n.sshPort), n.address, "--"}
	return append(cmd, remoteCmd...)
}

func formatBytes(b int) string {
	units := []string{"B", "kiB", "MiB", "GiB", "TiB"}
	bf := float64(b)
	base := 0
	for ; base < len(units) - 1 && bf >= 1024; base++ {
		bf /= 1024.0
	}
	return fmt.Sprintf("%.1f %s", bf, units[base])
}
