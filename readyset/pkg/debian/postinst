#!/bin/sh

# Remove existing masks on readyset.service that might have been created by an earlier removal.
deb-systemd-helper unmask readyset.service > /dev/null || true

# was-enabled defaults to true, so new installations run enable.
if deb-systemd-helper --quiet was-enabled readyset.service
then
    # Enables the unit on first installation, creates new
    # symlinks on upgrades if the unit file has changed.
    deb-systemd-helper enable readyset.service > /dev/null || true
else
    # Update the statefile to add new symlinks (if any), which need to be
    # cleaned up on purge. Also remove old symlinks.
    deb-systemd-helper update-state readyset.service > /dev/null || true
fi

# Print some instructions if package is being installed or configured.
if [ "$1" = "configure" ] || [ "$1" = "install" ]; then
    cat << EOF

********************************************************************************
Notice: The UPSTREAM_DB_URL and LISTEN_ADDRESS values must be set in
/etc/readyset/readyset.conf before starting the readyset service.

********************************************************************************

EOF
fi

# Ensure the script exits with a success status
exit 0
