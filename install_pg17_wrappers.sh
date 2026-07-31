#!/bin/bash
# Installs PG17 client wrappers backed by the postgres:17 docker image.
#
# Why: this box is Amazon Linux 2 (EL7). There is no native PG17 client for it
# (no amazon-linux-extras postgres topic; PGDG has no rhel-7 build for 17), and
# the RDS servers are now PG17 -- the host's PG13 pg_dump can no longer dump a
# v17 server. These wrappers put pg_dump/pg_restore/psql/... in /usr/local/bin
# (which precedes /usr/bin in PATH), each dispatching to postgres:17-alpine.
# This fixes sync_data.sh and every other caller with no script edits.
#
# Run:      sudo bash ~/install_pg17_wrappers.sh
# Undo:     sudo rm /usr/local/bin/pg-docker-wrapper \
#              /usr/local/bin/{pg_dump,pg_restore,psql,pg_dumpall,pg_isready,\
#              vacuumdb,reindexdb,createdb,dropdb,createuser,dropuser,clusterdb}
set -eo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: run this with sudo:  sudo bash $0" >&2
    exit 1
fi

IMAGE="postgres:17-alpine"
WRAPPER=/usr/local/bin/pg-docker-wrapper
TOOLS="pg_dump pg_restore psql pg_dumpall pg_isready vacuumdb reindexdb createdb dropdb createuser dropuser clusterdb"

echo "==> writing wrapper: $WRAPPER"
cat > "$WRAPPER" <<'WRAP'
#!/bin/bash
# PG17 client tools backed by the postgres:17 docker image (see installer).
# Symlinks pg_dump/pg_restore/psql/... point here; basename picks the tool.
set -eo pipefail
IMAGE="${PG_DOCKER_IMAGE:-postgres:17-alpine}"
TOOL="$(basename "$0")"

# forward the PG* env vars that are set (pass-through keeps secrets out of argv)
env_args=()
for v in PGPASSWORD PGHOST PGPORT PGUSER PGDATABASE PGSSLMODE PGSSLROOTCERT \
         PGPASSFILE PGOPTIONS PGCONNECT_TIMEOUT PGCLIENTENCODING; do
    if [ -n "${!v}" ]; then env_args+=( -e "$v" ); fi
done

# bind-mount, at identical paths, the dirs a client reads/writes (deduped)
mount_args=(); declare -A seen
for d in /tmp "$HOME" "$PWD"; do
    if [ -n "$d" ] && [ -d "$d" ] && [ -z "${seen[$d]}" ]; then
        seen[$d]=1; mount_args+=( -v "$d:$d" )
    fi
done

# Terminal handling. libpq prompts for passwords on /dev/tty, and falls back to
# (prompt on stderr, read from stdin) when it cannot open one. A container only has
# a controlling terminal if docker allocates a pty. Three cases, and each needs
# something different -- getting this wrong silently eats data or hangs:
#
#  1. stdin AND stdout are both terminals -> a genuine interactive session (psql).
#     Allocate a pty; everything behaves natively.
#
#     Do NOT key this on stdin alone. With `pg_dump ... > file` stdin is still the
#     terminal, so a pty gets allocated, and a pty collapses stdout, stderr and
#     /dev/tty into one stream: the password prompt lands in the output file (so
#     the command looks hung) and pty newline translation (LF -> CRLF) silently
#     corrupts binary -Fc dumps.
#
#  2. stdin is NOT a terminal -> it carries data (`pg_restore ... < dump`). libpq's
#     stdin fallback would read the first line of that data and send it as the
#     password. Bind-mount the real terminal onto /dev/tty so it prompts there
#     instead and leaves stdin alone, exactly like the native client.
#
#  3. stdin IS a terminal but stdout is redirected (`pg_dump ... > file`) -> do
#     nothing. libpq's stdin fallback works here: the prompt goes to stderr (still
#     the terminal) and `docker -i` forwards the typed password to the container's
#     stdin. Mounting /dev/tty in this case actively BREAKS it, because the docker
#     client is itself reading that same terminal to forward it and consumes the
#     keystrokes before the container can read them -- you get a prompt that never
#     accepts input, then "fe_sendauth: no password supplied".
#     (Caveat: echo cannot be disabled on a forwarded stdin, so the password is
#     visible as you type. Use PGPASSWORD or ~/.pgpass to avoid that.)
tty_args=( -i )
tty_mount=()
if [ -t 0 ] && [ -t 1 ]; then
    tty_args+=( -t )
elif [ ! -t 0 ]; then
    #     Resolve the terminal's device path with `tty` reading a dup of the fd.
    #     Do NOT use `readlink /proc/self/fd/N` here: a `2>/dev/null` guard rewires
    #     fd 2, and command substitution rewires fd 1, so both resolve to the wrong
    #     thing (that mistake yields `-v /dev/null:/dev/tty`, which silently makes
    #     every password prompt read EOF). `tty` only inspects fd 0, which we set.
    host_tty=""
    for fd in 2 1 0; do
        if [ -t "$fd" ]; then
            host_tty=$(tty <&"$fd" 2>/dev/null || true)
            break
        fi
    done
    if [ -n "$host_tty" ] && [ -c "$host_tty" ]; then
        tty_mount=( -v "$host_tty:/dev/tty" )
    fi
fi

exec docker run --rm "${tty_args[@]}" "${tty_mount[@]}" \
    --network host \
    --user "$(id -u):$(id -g)" \
    "${env_args[@]}" "${mount_args[@]}" -w "$PWD" \
    "$IMAGE" "$TOOL" "$@"
WRAP
chmod 755 "$WRAPPER"

echo "==> linking tools in /usr/local/bin -> pg-docker-wrapper"
for t in $TOOLS; do
    ln -sf pg-docker-wrapper "/usr/local/bin/$t"
    echo "    $t"
done

echo "==> ensuring image is present ($IMAGE)"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "    pulling $IMAGE ..."
    docker pull "$IMAGE"
fi

echo "==> verifying as ${SUDO_USER:-root}"
RUN_AS=(); [ -n "$SUDO_USER" ] && RUN_AS=(sudo -u "$SUDO_USER")
if "${RUN_AS[@]}" bash -lc 'hash -r; printf "which pg_dump -> "; which pg_dump; pg_dump --version; psql --version'; then
    echo
    echo "DONE. pg_dump/psql now run PG17 from docker."
else
    echo "WARN: verification command failed -- check docker access for ${SUDO_USER:-root}." >&2
fi
echo
echo "If a shell already used pg_dump, run 'hash -r' in it (or open a new shell),"
echo "then test:  make sync-data ARGS=\"--list\"   followed by a --db-only run."
