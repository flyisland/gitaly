#!/bin/bash

set -e

# Purpose: Run (or rerun) postprocessing steps to filter stack traces from bpftrace and generate a flamegraph and iciclegraph.
#
# Usage: run_postprocessing_for_raw_profile.sh <bpftrace.txt.gz>
#
# Expects the following tools to be installed and in the PATH:
#  * flamegraph.pl and stackcollapse-bpftrace.pl
#    $ git clone https://github.com/brendangregg/FlameGraph.git
#  * Generic utilities: grep, date, gzip, perl

STACK_EXCLUSION_FILTERS="$( dirname $0 )/stack_exclusion_filters.txt"
STACK_INCLUSION_FILTERS="$( dirname $0 )/stack_inclusion_filters.txt"

BPFTRACE_RESULTS_FILE=$1
OUTDIR=$( dirname "$BPFTRACE_RESULTS_FILE" )
OUTFILE_PREFIX="${OUTDIR}/offcpu_profile"
[[ -f "$BPFTRACE_RESULTS_FILE" ]] || { echo "ERROR: Missing input file: $BPFTRACE_RESULTS_FILE" ; exit 1 ; }
[[ -d "$OUTDIR" ]] || { echo "ERROR: Output dir does not exist: $OUTDIR" ; exit 1 ; }

echo "$( date -u +%Y-%m-%d\ %H:%M:%S\ %Z )  Generating flamegraph and iciclegraph from filtered stack traces."
UNFILTERED_STACK_TRACES_OUTFILE="$OUTFILE_PREFIX.stack_traces.unfiltered.txt.gz"
zcat "$BPFTRACE_RESULTS_FILE" | grep -A100000 "Profile results start" | grep -B100000 "Profile results end" | gzip > "$UNFILTERED_STACK_TRACES_OUTFILE"
FILTERED_FOLDED_STACK_TRACES_OUTFILE="$OUTFILE_PREFIX.stack_traces.folded.txt.gz"
zcat "$UNFILTERED_STACK_TRACES_OUTFILE" | perl -pe 's/^@\S*\[$/\@\[/' | stackcollapse-bpftrace | grep -E -f "$STACK_INCLUSION_FILTERS" | grep -v -E -f "$STACK_EXCLUSION_FILTERS" | gzip > "$FILTERED_FOLDED_STACK_TRACES_OUTFILE"
FLAMEGRAPH_OUTFILE="$OUTFILE_PREFIX.flamegraph.svg"
zcat "$FILTERED_FOLDED_STACK_TRACES_OUTFILE" | flamegraph --hash --colors=perl > "$FLAMEGRAPH_OUTFILE"
ICICLEGRAPH_OUTFILE="$OUTFILE_PREFIX.iciclegraph.svg"
zcat "$FILTERED_FOLDED_STACK_TRACES_OUTFILE" | flamegraph --hash --colors=perl --reverse --inverted > "$ICICLEGRAPH_OUTFILE"
echo "$( date -u +%Y-%m-%d\ %H:%M:%S\ %Z )  Finished post-processing."

echo
echo "Results summary:"
echo
zcat "$BPFTRACE_RESULTS_FILE" | grep -A10000 "Profile summary start" | grep -B10000 "Profile summary end"

echo
echo "Used the following filters to exclude or include stack traces in the flamegraph/iciclegraph:"
echo
echo "Exclusion filters:"
echo "----------------------------------------"
cat "$STACK_EXCLUSION_FILTERS"
echo "----------------------------------------"
echo
echo "Inclusion filters:"
echo "----------------------------------------"
cat "$STACK_INCLUSION_FILTERS"
echo "----------------------------------------"

echo
echo "Results files:"
echo "  Raw bpftrace output:     $BPFTRACE_RESULTS_FILE"
echo "  Unfiltered stack traces: $UNFILTERED_STACK_TRACES_OUTFILE"
echo "  Filtered folded stacks:  $FILTERED_FOLDED_STACK_TRACES_OUTFILE"
echo "  Filtered flamegraph:     $FLAMEGRAPH_OUTFILE"
echo "  Filtered iciclegraph:    $ICICLEGRAPH_OUTFILE"
