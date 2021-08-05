setup_data_volume() {
    local MOUNT_POINT=$1

    ROOT_DEV_NAME=$(blkid --list-one --match-token PTTYPE=gpt --output device | sed -e "s#/dev/##" | head -1)
    if [ -z "$ROOT_DEV_NAME" ]; then
        echo "Error: Failed to get root device name"
        return 1
    fi

    DATA_DEV_NAME=$(lsblk --nodeps --output NAME --noheadings | grep -v ^$ROOT_DEV_NAME | head -1)
    if [ -z "$DATA_DEV_NAME" ]; then
        echo "Error: Failed to get data device name"
        return 1
    fi

    DATA_DEV=/dev/$DATA_DEV_NAME
    FS_TYPE=$(lsblk $DATA_DEV --output FSTYPE --noheading)
    if [ -z "$FS_TYPE" ]; then
        mkfs.ext4 $DATA_DEV
        # Obtaining the UUID below needs this
        sleep 1
    else
        echo "Warn: Data device has an existing filesystem, skipping"
    fi

    mkdir -p $MOUNT_POINT
    UUID=$(lsblk $DATA_DEV --output UUID --noheadings)
    if [ -z "$UUID" ]; then
        echo "Error: Failed to get UUID for data device: $DATA_DEV"
        return 1
    fi

    grep -q ^UUID=$UUID /etc/fstab
    if [ $? != 0 ]; then
        echo "UUID=$UUID $MOUNT_POINT ext4 rw,discard,x-systemd.growfs 0 0" >> /etc/fstab

    fi

    mount $MOUNT_POINT
}
