#!/bin/sh

READYSET_DIR=/var/lib/readyset

systemctl --system daemon-reload >/dev/null || true

if [ "$1" = "remove" ] || [ "$1" = "purge" ]; then
    # Prevents service state from being changed (it was stopped by prerm).
    deb-systemd-helper mask readyset.service >/dev/null
fi

if [ "$1" = "purge" ] || [ "$1" = "abort-install" ]; then
    # Remove the readyset user and data dir.  The conf file will automatically be deleted.
    if getent passwd readyset >/dev/null;
    then
        userdel readyset
    fi
    if [ -d ${READYSET_DIR} ] || [ -L ${READYSET_DIR} ];
    then
        rm -rf ${READYSET_DIR}
    fi
fi
