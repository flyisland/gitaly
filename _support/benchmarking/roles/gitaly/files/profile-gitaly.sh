#!/bin/sh
#
# profile-gitaly: Profile host with perf and libbpf-tools.
# Must be run as root.
#
# Mandatory arguments:
# 	-d <DURATION_SECS> : Number of seconds to profile for
# 	-g <GIT_REPO>      : Name of Git repository being used
# 	-o <OUTPUT_DIR>    : Directory to write output to
# 	-r <RPC>           : Name of RPC being executed

set -e

usage() {
	echo "Usage: $0 -d <DURATION_SECS> -o <OUTPUT_DIR> -r <RPC> \
-g <GIT_REPOSITORY>"
	exit 1
}

profile() {
	# Profile Gitaly and child processes
	perf record --freq=99 -g --pid="$(pidof -s gitaly)" \
	    --output="${gitaly_perf_data}" -- sleep "${seconds}" &

	# Profile whole system
	# --call-graph=dwarf - Use DWARF debug info for call stack, works well with
	#                      C programs but is much larger than fp, causing
	#                      flamegraph generation to be proportionately slower.
	perf record --freq=99 --call-graph=dwarf --all-cpus \
		--output="${all_perf_data}" -- sleep "${seconds}" &

	# Capture arguments of all processes forked by Gitaly.
	timeout "${seconds}" execsnoop \
		--uid=1999 --quote > "${out_dir}/gitaly-execs.txt" &

	# Histogram of duration programs were scheduled by the kernel.
	cpudist "${seconds}" 1 > "${out_dir}/cpu-dist-on.txt" &

	# Histogram of duration programs were slept by the kernel.
	cpudist --offcpu "${seconds}" 1 > "${out_dir}/cpu-dist-off.txt" &

	# Histogram of latency to block I/O, separated by disk.
	# `git-repositories` will be mounted as `/dev/sdb`.
	biolatency --disk "${seconds}" 1 > "${out_dir}/biolatency.txt" &

	# Details of processes performing the most block I/O.
	biotop --noclear --rows 100 "${seconds}" 1 > "${out_dir}/biotop.txt" &

	# Capture kernel page cache hit rate.
	cachestat "${seconds}" 1 > "${out_dir}/page-cachestat.txt" &

	wait
}

generate_flamegraphs() {
	gitaly_perf_svg="${out_dir}/gitaly-perf.svg"
	perf script --header --input="${gitaly_perf_data}" \
	  | stackcollapse \
	  | flamegraph > "${gitaly_perf_svg}" &

	all_perf_svg="${out_dir}/all-perf.svg"
	perf script --header --input="${all_perf_data}" \
		| stackcollapse \
		| flamegraph > "${all_perf_svg}" &

	wait
}

main() {
	if [ "$(id -u)" -ne 0 ]; then
		echo "$0 must be run as root" >&2
		exit 1
	fi

	while getopts "hd:g:o:r:" arg; do
		case "${arg}" in
			d) seconds=${OPTARG} ;;
			g) repo=${OPTARG} ;;
			o) out_dir=${OPTARG} ;;
			r) rpc=${OPTARG} ;;
			h|*) usage ;;
		esac
	done

	if [ "${seconds}" -le 0 ] \
		|| [ -z "${out_dir}" ] \
		|| [ -z "${rpc}" ] \
		|| [ -z "${repo}" ]; then
		usage
	fi

	if ! pidof gitaly > /dev/null; then
		echo "Gitaly is not running, aborting" >&2
		exit 1
	fi

	# Ansible's minimal shell will may not include /usr/local/bin in $PATH
	if ! printenv PATH | grep "/usr/local/bin" > /dev/null; then
		export PATH="${PATH}:/usr/local/bin"
	fi

	perf_tmp_dir=$(mktemp -d "/tmp/gitaly-perf-${repo}-${rpc}.XXXXXX")
	gitaly_perf_data="${perf_tmp_dir}/gitaly-perf.out"
	all_perf_data="${perf_tmp_dir}/all-perf.out"

	profile

	generate_flamegraphs

	chown -R git:git "${out_dir}"
	rm -rf "${perf_tmp_dir}"
}

main "$@"
