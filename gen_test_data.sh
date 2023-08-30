#!/bin/bash
set -euxo pipefail
DIR="$(realpath "$(dirname "$0")")"

remkdir() {
  D="$1"; rm -rf "$D" > /dev/null || true; mkdir -p "$D" > /dev/null; echo "$D"
}

TMP_DIR="$(remkdir "$DIR/.tmp")"
TEST_DATA_DIR="$(remkdir "$DIR/test_data")"

FS_FILE="$TMP_DIR/fs.btrfs"
MOUNT_DIR="$(remkdir "$TMP_DIR/mount")"

truncate -s 256M "$FS_FILE"
mkfs.btrfs "$FS_FILE"

on_exit() {
  [[ -d "$MOUNT_DIR" ]] && sudo umount "$MOUNT_DIR" || true
}
trap 'on_exit' EXIT ERR

sudo mount "$FS_FILE" "$MOUNT_DIR"
sudo chown "$USER" "$MOUNT_DIR"
sudo chmod u=rwx "$MOUNT_DIR"

# Folder containing all data
FS_DATA_DIR="$MOUNT_DIR/data"
btrfs subvolume create "$FS_DATA_DIR"

# Folder containing all snapshots
FS_SNAP_DIR="$(remkdir "$MOUNT_DIR/snap")"
btrfs subvolume snapshot -r "$FS_DATA_DIR" "$FS_SNAP_DIR/000"

# Gen all data
I=1
while read -r command; do
    (cd "$FS_DATA_DIR"; eval $command; sync)
    PREV_SNAP_NAME="$(printf "%03i" $((I - 1)))"
    SNAP_NAME="$(printf "%03i" $I)"
    btrfs subvolume snapshot -r "$FS_DATA_DIR" "$FS_SNAP_DIR/$SNAP_NAME" > /dev/null

    # sudo btrfs send -p "$FS_SNAP_DIR"/000 "$FS_SNAP_DIR/$SNAP_NAME" > "$TEST_DATA_DIR/full-$SNAP_NAME.snap"
    sudo btrfs send -p "$FS_SNAP_DIR/$PREV_SNAP_NAME" "$FS_SNAP_DIR/$SNAP_NAME" > "$TEST_DATA_DIR/inc-$SNAP_NAME.snap"
    sudo btrfs send --no-data -p "$FS_SNAP_DIR/$PREV_SNAP_NAME" "$FS_SNAP_DIR/$SNAP_NAME" > "$TEST_DATA_DIR/inc-no-data-$SNAP_NAME.snap"

    echo "$I: $command" >&2
    I=$((I + 1))
done < "$DIR/test_commands"
