#!/bin/sh
#
# process-gitaly-logs: Extracts and post-processes Gitaly logs
#
# Mandatory arguments:
#   <OUTPUT_DIR>    : Directory to write output to

set -e

main() {
    out_dir=$1

    log_file="${out_dir}/gitaly.log"
    journalctl --output=cat _PID=$(pidof -s gitaly) > "${log_file}"

    python3 /src/plot.py "${log_file}" "${out_dir}"
}

main "$@"
