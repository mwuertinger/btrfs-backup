[![CircleCI](https://circleci.com/gh/mwuertinger/btrfs-backup.svg?style=svg)](https://circleci.com/gh/mwuertinger/btrfs-backupu)
# BTRFS Backup
This tool allows to send BTRFS snapshots incrementally to a remote host. BTRFS
supports cheap snapshots which can be serialized to a byte stream. Snapshots
can be transmitted incrementally by specifying a parent.

## Install
```
go get github.com/mwuertinger/btrfs-backup
```

## Prerequisites
- You have two Linux hosts containing BTRFS filesystems
- You frequently create snapshots on the source system
- You transferred the first BTRFS snapshot manually to the target system:
  `btrfs subvolume send /mnt/snapshot/2019-01-01 | ssh target-host btrfs /mnt`

## Usage
```
btrfs-backup -src /mnt -dst target-host:22/mnt
```

## How it works
The tool lists the snapshots on source and destination hosts in alphanumerical
order and looks for the first matching snapshot, eg:
```
source		target
================================
2019-01-01	2019-01-01
2019-01-02	2019-01-02
2019-01-03
2019-01-04
```
It then iterates over the list and starts sending the first missing snapshot to
the target machine using eg. `btrfs subvolume send -p 2019-01-02 2019-01-03`.
