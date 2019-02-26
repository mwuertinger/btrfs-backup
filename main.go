package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
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
	snapshotRegex := regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`)
	source := node{
		address:       "localhost",
		sshPort:       0,
		mountPoint:    "/mnt",
		snapshotPath:  "snapshot",
		snapshotRegex: snapshotRegex,
		executor:      defaultExecutor,
	}
	destination := node{
		address:       "target-host",
		sshPort:       10022,
		mountPoint:    "/mnt",
		snapshotPath:  "",
		snapshotRegex: snapshotRegex,
		executor:      defaultExecutor,
	}

	sourceSnapshots, err := source.getSnapshots()
	if err != nil {
		log.Fatalf("failed to get local snapshots: %v", err)
	}
	destinationSnapshots, err := destination.getSnapshots()
	if err != nil {
		log.Fatalf("failed to get remove snapshots: %v", err)
	}

	fmt.Println("local:")
	for _, snapshot := range sourceSnapshots {
		fmt.Println(snapshot)
	}
	fmt.Println("\ndestination:")
	for _, snapshots := range destinationSnapshots {
		fmt.Println(snapshots)
	}

	if err := transmitSnapshots(&source, &destination, sourceSnapshots, destinationSnapshots); err != nil {
		log.Fatal(err)
	}
}

func transmitSnapshots(source, destination *node, localSnapshots, remoteSnapshots []string) error {
	mostRecentRemote := remoteSnapshots[len(remoteSnapshots)-1]
	previousSnapshot := ""

	for _, snapshot := range localSnapshots {
		if previousSnapshot != "" {
			err := sendSnapshot(source, destination, snapshot, previousSnapshot)
			if err != nil {
				return fmt.Errorf("transmitSnapshots: %v", err)
			}
			previousSnapshot = snapshot
		} else if snapshot == mostRecentRemote {
			previousSnapshot = mostRecentRemote
		}
	}

	return nil
}

func sendSnapshot(source, destination *node, snapshot, previousSnapshot string) error {
	p := path.Join(source.mountPoint, source.snapshotPath, previousSnapshot)
	s := path.Join(source.mountPoint, source.snapshotPath, snapshot)

	sendCmd := []string{"btrfs", "send", "-p", p, s}
	if source.sshPort != 0 {
		sendCmd = sshCmd(source, sendCmd)
	}
	receiveCmd := []string{"btrfs", "receive", destination.mountPoint}
	if destination.sshPort != 0 {
		receiveCmd = sshCmd(destination, receiveCmd)
	}

	log.Printf("%s | %s", strings.Join(sendCmd, " "), strings.Join(receiveCmd, " "))

	_, err := source.executor.exec([][]string{sendCmd, receiveCmd})
	if err != nil {
		return fmt.Errorf("sendSnapshot: %v", err)
	}
	return nil
}

// getSnapshots returns a sorted list of snapshots.
func (n *node) getSnapshots() ([]string, error) {
	cmd := []string{"btrfs", "subvolume", "list", n.mountPoint}
	if n.sshPort != 0 {
		cmd = sshCmd(n, cmd)
	}

	out, err := n.executor.exec([][]string{cmd})
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
	cmd := []string{"btrfs", "subvolume", "delete"}
	for _, snapshot := range snapshots {
		cmd = append(cmd, snapshot)
	}
	if n.sshPort != 0 {
		cmd = sshCmd(n, cmd)
	}
	_, err := n.executor.exec([][]string{cmd})
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
	exec(cmds [][]string) (string, error)
}

type executorImpl struct{}

var defaultExecutor = executorImpl{}

func (_ executorImpl) exec(cmds [][]string) (string, error) {
	var cs []*exec.Cmd
	var out bytes.Buffer
	var errs []error

	for i, cmd := range cmds {
		c := exec.Command(cmd[0], cmd[1:]...)

		if len(cs) > 0 {
			pipe, err := cs[len(cs)-1].StdoutPipe()
			if err != nil {
				return "", fmt.Errorf("execPipe: StdoutPipe: %v", err)
			}
			c.Stdin = pipe
		}
		if i == len(cmds)-1 {
			c.Stdout = &out
		}

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

	if len(errs) > 0 {
		return "", fmt.Errorf("%+v", errs)
	}

	return out.String(), nil
}

func sshCmd(n *node, remoteCmd []string) []string {
	return []string{"ssh", fmt.Sprintf("-p%d", n.sshPort), n.address, strings.Join(remoteCmd, " ")}
}
