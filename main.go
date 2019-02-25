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
	remote := destination{
		host: "target-host",
		port: 10022,
	}

	snapshotRegex := regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`)
	snapshotDir := "snapshot" // relative to mount point

	snapshotsLocal, err := getSnapshots(localhost, "/mnt", snapshotDir, snapshotRegex)
	if err != nil {
		log.Fatal(err)
	}
	snapshotsRemote, err := getSnapshots(remote, "/mnt", "", snapshotRegex)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("local:")
	for _, snapshot := range snapshotsLocal {
		fmt.Println(snapshot)
	}
	fmt.Println("\nremote:")
	for _, snapshots := range snapshotsRemote {
		fmt.Println(snapshots)
	}

	mostRecentRemote := snapshotsRemote[len(snapshotsRemote)-1]
	previousSnapshot := ""
	for _, snapshot := range snapshotsLocal {
		if previousSnapshot != "" {
			err := sendSnapshot(localhost, snapshot, previousSnapshot, "/mnt", snapshotDir, remote)
			if err != nil {
				log.Fatal(err)
			}
			previousSnapshot = snapshot
		}

		if snapshot == mostRecentRemote {
			previousSnapshot = mostRecentRemote
		}
	}
}

func sendSnapshot(e executor, snapshot, previousSnapshot, mountPoint, snapshotDir string, d destination) error {
	fmt.Printf("btrfs send -p %s %s\n", path.Join(mountPoint, snapshotDir, previousSnapshot), path.Join(mountPoint, snapshotDir, snapshot))
	return nil
}

// getSnapshots returns a sorted list of snapshots.
func getSnapshots(e executor, mountPoint string, snapshotDir string, r *regexp.Regexp) ([]string, error) {
	out, err := e.exec("btrfs", "subvolume", "list", mountPoint)
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
	exec(cmd ... string) (string, error)
}

// destination determines where a command is to be executed.
type destination struct {
	host string
	port int
}

// localhost is a special destination for running commands locally
var localhost destination

// exec runs the specified cmd at destination d and returns stdout.
func (d destination) exec(cmd ... string) (string, error) {
	var c *exec.Cmd
	if d == localhost {
		c = exec.Command(cmd[0], cmd[1:]...)
	} else {
		c = exec.Command("ssh", fmt.Sprintf("-p%d", d.port), d.host, strings.Join(cmd, " "))
	}
	var buf bytes.Buffer
	c.Stdout = &buf
	if err := c.Run(); err != nil {
		return "", err
	}
	return buf.String(), nil
}
