# Point-in-Time snapshots

## Problem Statement

The current backup implementation starts by capturing the state of the data set
as a set of tree records, with each tree containing references to all of its
child members. For files, this is a BLAKE3 hash digest of the file contents, for
symbolic links it is the base64 encoded contents, and likewise for very short
files. Directories are the SHA1 checksum of the textual representation of the
references of its members, hence the entire directory structure forms a Merkle
tree. If a single byte of a single file is changed, then every tree from the
root to that file will also change.

Once the directory structure is recorded, the backup process reads all new file
content into pack files. If a file's contents changed between these two points
in time, then the tree entry will be inaccurate. With respect to the database
records, the references are still consistent and the file can be restored.
However, the next time a backup occurs the unmodified file's hash digest will be
different from what was recorded previously and it will be saved a second time.
While this is not a consistency issue, it does result in the same content being
saved twice. For small files this may be an acceptable cost, but for much larger
files it may result in an additional storage cost.

## Objective

The solution to this problem is point-in-time snapshots. These represent an
unchanging view of the file system at a particular point in time, enabling a
reliable backup that captures the data accurately. Support for such snapshots
must be implemented in the file system, and how that works will differ for each
operating system and file system.

## OS support

Support for point-in-time snapshots within the file system is critical, without
this there is no feasible means of effecting the same result in an application.
For the sake of simplicity, the initial implementation of this feature can focus
on a single file system for each of the supported operating systems.

### Linux

Btrfs, ZFS, and Bcachefs all support snapshots, however only ZFS is proven
technology with stable support for multi-device pools, while Btrfs is better
suited to a single-drive installation, and Bcachefs is still experimental.

The following example creates a (recursive) snapshot, mounts it in a temporary
directory, then unmounts and removes the snapshot (recursively).

```shell
# create the snapshot, including any child data sets
zfs snapshot -r pool/dataset@snapshot

# show this and other snapshots
zfs list -r -t snapshot

# make a temporary directory and mount the snapshot
mkdir /tmp/snappy
mount -t zfs pool/dataset@snapshot /tmp/snappy

# [work with the snapshot mounted in /tmp/snappy...]

# unmount the snapshot and destroy the snapshot and its children
umount /tmp/snappy
zfs destroy -r pool/dataset@snapshot
```

### macOS

APFS supports snapshots and has been the default on modern Apple computers since
the release of High Sierra (10.13) in 2017. There do not appear to be any
appropriate Rust crates for APFS, and supposedly using the snapshots API
requires approval from Apple. However, the `tmutil` command works perfectly well
for the purpose of creating and managing local snapshots.

The following examples demonstrate using `tmutil` and other commands to manage
local snapshots and mount points.

#### Creating a local snapshot

```shell
$ tmutil localsnapshot
NOTE: local snapshots are considered purgeable and may be removed at any time by deleted(8).
Created local snapshot with date: 2026-09-30-200236
```

#### Listing snapshots

```shell
# when there are no local snapshots yet
$ tmutil listlocalsnapshots /
Snapshots for disk /:

# when one snapshot has been created
$ tmutil listlocalsnapshots /
Snapshots for disk /:
com.apple.TimeMachine.2026-09-30-200236.local

# when there are two snapshots
$ tmutil listlocalsnapshots /
Snapshots for disk /:
com.apple.TimeMachine.2026-09-30-200236.local
com.apple.TimeMachine.2026-09-30-195600.local
```

#### Mounting a snapshot

Some examples online show a simple procedure and that did work at one point, but
later the error `Resource busy` kept happening and the mount would not succeed.
The work-around is to identify the exact raw disk slice to use for the mount.

**TODO** might be caused by system update waiting to happen. this _used_ to work

```shell
mkdir /private/tmp/snappy
mount_apfs -o ro -s com.apple.TimeMachine.2026-09-30-200236.local / /private/tmp/snappy
# that used to work, now it says this every time
mount_apfs: volume could not be mounted: Resource busy
```

Use the `diskutil` command to get a thorough listing of every storage device
attached to the system. The output below is for a _single_ physical SSD that is
represented by multiple "disks" and various partitions/slices.

```shell
$ diskutil list
/dev/disk0 (internal, physical):
   #:                       TYPE NAME                    SIZE       IDENTIFIER
   0:      GUID_partition_scheme                        *500.3 GB   disk0
   1:             Apple_APFS_ISC Container disk1         524.3 MB   disk0s1
   2:                 Apple_APFS Container disk3         494.4 GB   disk0s2
   3:        Apple_APFS_Recovery Container disk2         5.4 GB     disk0s3

/dev/disk3 (synthesized):
   #:                       TYPE NAME                    SIZE       IDENTIFIER
   0:      APFS Container Scheme -                      +494.4 GB   disk3
                                 Physical Store disk0s2
   1:                APFS Volume Macintosh HD            12.6 GB    disk3s1
   2:              APFS Snapshot com.apple.os.update-... 12.6 GB    disk3s1s1
   3:                APFS Volume Preboot                 9.0 GB     disk3s2
   4:                APFS Volume Recovery                1.3 GB     disk3s3
   5:                APFS Volume Data                    358.6 GB   disk3s5
   6:                APFS Volume VM                      2.1 GB     disk3s6
```

The one of interest is labeled `Data` as it is the largest and contains the
actual data that we care about (not recovery or boot partitions). The `Data`
partition is number `3` so the full device path is `/dev/disk3s5` -- `s` for
slice maybe?

```shell
$ mount_apfs -o ro -s com.apple.TimeMachine.2026-09-30-200236.local /dev/disk3s5 /private/tmp/snappy
# no output, but exit code is 0 and /private/tmp/snappy shows the snapshot volume
```

#### Unmounting and removing the snapshot

```shell
$ umount /private/tmp/snappy
# no output, exit code is 0
$ tmutil deletelocalsnapshots 2026-09-30-200236
Deleted local snapshot '2026-09-30-200236'
# exit code is 0
rmdir /private/tmp/snappy
# no output, exit code is 0
```

The delete can take several seconds even if very little was changed.

### Windows

Volume Shadow Copy Service (VSS) is a combination of a Windows service and an
administrative command called `vssadmin` (that requires elevated privileges).
Invoking the command will automatically start the service.
