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

func main() {
	destination := destination{
		host: "target-host",
		port: 10022,
	}
	mount := "/mnt" // btrfs mount point
	snapshotRegex := regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`)
	snapshotDir := "snapshot" // relative to mount point

	localSnapshots, err := getSnapshots(localhost, mount, snapshotDir, snapshotRegex)
	if err != nil {
		log.Fatalf("failed to get local snapshots: %v", err)
	}
	remoteSnapshots, err := getSnapshots(destination, mount, "", snapshotRegex)
	if err != nil {
		log.Fatalf("failed to get remove snapshots: %v", err)
	}

	fmt.Println("local:")
	for _, snapshot := range localSnapshots {
		fmt.Println(snapshot)
	}
	fmt.Println("\ndestination:")
	for _, snapshots := range remoteSnapshots {
		fmt.Println(snapshots)
	}

	if err := transmitSnapshots(localhost, destination, mount, snapshotDir, localSnapshots, remoteSnapshots); err != nil {
		log.Fatal(err)
	}
}

func transmitSnapshots(e executor, d destination, mount, dir string, localSnapshots, remoteSnapshots []string) error {
	mostRecentRemote := remoteSnapshots[len(remoteSnapshots)-1]
	previousSnapshot := ""

	for _, snapshot := range localSnapshots {
		if previousSnapshot != "" {
			err := sendSnapshot(e, d, snapshot, previousSnapshot, mount, dir)
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

func sendSnapshot(e executor, d destination, snapshot, previousSnapshot, mountPoint, snapshotDir string) error {
	p := path.Join(mountPoint, snapshotDir, previousSnapshot)
	s := path.Join(mountPoint, snapshotDir, snapshot)

	cmd1 := []string{"btrfs", "send", "-p", p, s}
	cmd2 := sshCmd(d, []string{"btrfs", "receive", mountPoint})

	log.Printf("%s | %s", strings.Join(cmd1, " "), strings.Join(cmd2, " "))

	//_, err := e.execPipe(cmd1, cmd2)
	//if err != nil {
	//	return fmt.Errorf("sendSnapshot: %v", err)
	//}
	return nil
}

// getSnapshots returns a sorted list of snapshots.
func getSnapshots(e executor, mountPoint string, snapshotDir string, r *regexp.Regexp) ([]string, error) {
	out, err := e.exec([]string{"btrfs", "subvolume", "list", mountPoint})
	if err != nil {
		return nil, err
	}

	subVolumes, err := parseSubVolumes(out)
	if err != nil {
		return nil, err
	}
	snapshots := filterSnapshots(subVolumes, snapshotDir, r)
	sort.Strings(snapshots)
	return snapshots, nil
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

// executor allows to execute commands locally or remotely. It also allows to mock execution for testing.
type executor interface {
	exec(cmd []string) (string, error)
	execPipe(cmd1, cmd2 []string) (string, error)
}

// destination determines where a command is to be executed.
type destination struct {
	host string
	port int
}

// localhost is a special destination for running commands locally
var localhost destination

// exec runs the specified cmd at destination d and returns stdout.
func (d destination) exec(cmd []string) (string, error) {
	if d != localhost {
		cmd = sshCmd(d, cmd)
	}
	c := exec.Command(cmd[0], cmd[1:]...)
	var buf bytes.Buffer
	c.Stdout = &buf
	if err := c.Run(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (d destination) execPipe(cmd1, cmd2 []string) (string, error) {
	c1 := exec.Command(cmd1[0], cmd1[1:]...)
	c2 := exec.Command(cmd2[0], cmd2[1:]...)

	pipe, err := c1.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("execPipe: StdoutPipe: %v", err)
	}
	c2.Stdin = pipe

	var buf bytes.Buffer
	c2.Stdout = &buf

	if err := c1.Start(); err != nil {
		return "", fmt.Errorf("execPipe: c1.Start: %v", err)
	}
	if err := c2.Start(); err != nil {
		return "", fmt.Errorf("execPipe: c2.Start: %v", err)
	}
	if err := c1.Wait(); err != nil {
		return "", fmt.Errorf("execPipe: c1.Wait: %v", err)
	}
	if err := c2.Wait(); err != nil {
		return "", fmt.Errorf("execPipe: c2.Wait: %v", err)
	}

	return buf.String(), nil
}

func sshCmd(d destination, remoteCmd []string) []string {
	return []string{"ssh", fmt.Sprintf("-p%d", d.port), d.host, strings.Join(remoteCmd, " ")}
}