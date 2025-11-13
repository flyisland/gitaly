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
	# Profile on-CPU time for Gitaly and child processes
	perf record --freq=99 -g --pid="$(pidof -s gitaly)" \
	    --output="${gitaly_perf_data}" -- sleep "${seconds}" &

	# Profile on-CPU time for whole system
	perf record --freq=97 -g --all-cpus \
		--output="${all_perf_data}" -- sleep "${seconds}" &

	# Profile off-CPU time for whole system (with filtering as a post-processing step)
	min_stall_duration_us=1000
	offcpu_profile_raw_output_file="${out_dir}/offcpu_profile.raw.txt.gz"
	bpftrace /usr/local/gitaly_offcpu_profiler/offcpu_profile.bt "${seconds}" "${min_stall_duration_us}" \
		| gzip > "${offcpu_profile_raw_output_file}" &

	# Hitting the pprof/profile endpoint first will turn on CPU profiling, which will
	# then be embedded automatically into the CPU trace.
	# See https://github.com/golang/go/issues/66679 for a discussion on why.
	curl -o "${out_dir}/pprof-profile.out" "localhost:9236/debug/pprof/profile?seconds=${seconds}" &
	curl -o "${out_dir}/pprof-trace.out" "localhost:9236/debug/pprof/trace?seconds=${seconds}" &

	wait
}

generate_flamegraphs() {
	gitaly_perf_txt="${out_dir}/gitaly-perf.txt.gz"
	gitaly_perf_svg="${out_dir}/gitaly-perf.svg"
	perf script --header --input="${gitaly_perf_data}" \
	  | gzip > "${gitaly_perf_txt}"
    zcat "${gitaly_perf_txt}" \
	  | stackcollapse-perf --kernel \
	  | flamegraph --hash --colors=perl > "${gitaly_perf_svg}"

	all_perf_txt="${out_dir}/all-perf.txt.gz"
	all_perf_svg="${out_dir}/all-perf.svg"
	perf script --header --input="${all_perf_data}" \
		| gzip > "${all_perf_txt}"
    zcat "${all_perf_txt}" \
		| stackcollapse-perf --kernel \
		| flamegraph --hash --colors=perl > "${all_perf_svg}"

	/usr/local/gitaly_offcpu_profiler/offcpu_profile_postprocessing.sh "${offcpu_profile_raw_output_file}"
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
