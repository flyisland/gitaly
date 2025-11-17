#!/bin/env perl

use strict;

# Purpose: Print a summary of CPU usage during a CPU profiling run.
# Consumes the output of "perf script" for a timer-based CPU sampling profile.
# Displays a per-second count of on-CPU samples.

# Example input:
# reference-trans 1394851 [000] 174750.111250:   10309278 cpu-clock:ppp: 
#         ffffffff95636226 pte_offset_map_nolock+0x76 ([kernel.kallsyms])
#         ffffffff95620942 handle_pte_fault+0x32 ([kernel.kallsyms])
#         ffffffff9562112e __handle_mm_fault+0x64e ([kernel.kallsyms])
#         ffffffff956213ff handle_mm_fault+0x17f ([kernel.kallsyms])

our $NUM_COMMS_TO_SHOW = 5;

sub main
{
    my $counts_by_comm_by_period = parse_perf_events();
    my $top_comms = find_top_n_comms($counts_by_comm_by_period);
    my $display_fields = [ 'ALL_SAMPLES', 'BUSY', 'IDLE', @$top_comms, 'OTHERS' ];
    show_output_headers($display_fields);
    foreach my $period_counts (@$counts_by_comm_by_period) {
        next unless ($period_counts->{'ALL_SAMPLES'} > 0);
        compute_others_count($period_counts, $top_comms);
        show_period_summary($period_counts, $display_fields);
    }
}

sub parse_perf_events
{
    my $counts_by_comm_by_period = [];
    my ($curr_period, $prev_period, $curr_period_counts);

    while (my $line = <>) {
        # Extract event metadata.  We only need fields from the first line of each event.
        next if $line =~ /^#/;
        my ($comm, $tid, $cpuid, $curr_ts) = ($line =~ /^(.{1,16}?)\s+(\d+)\s+\[(\d+)\]\s+(\d+\.\d+):/) or next;

        # If switching 1-second aggregation periods, store the previous period's counters and initialize the next period's counters.
        $curr_period = period_for_timestamp($curr_ts);
        if (! defined($prev_period)) {
            $prev_period = $curr_period;
            $curr_period_counts = init_counters_for_period($curr_period);
        } elsif ($prev_period != $curr_period) {
            push @$counts_by_comm_by_period, $curr_period_counts;
            $prev_period = $curr_period;
            $curr_period_counts = init_counters_for_period($curr_period);
        }

        # Accumulate new sample into the counters for the current aggregation period.
        accumulate_measurements($curr_period_counts, $comm);
    }

    # Store the counters for the last (partial) period.
    push @$counts_by_comm_by_period, $curr_period_counts if $curr_period_counts;

    die "ERROR: Found no valid lines of input.  Was the input file an uncompressed perf-script output?\n"
        unless scalar(@$counts_by_comm_by_period) > 0;

    return $counts_by_comm_by_period;
}

sub period_for_timestamp
{
    my ($ts) = @_;
    return int($ts);
}

sub init_counters_for_period
{
    my ($curr_period) = @_;
    return {
        'period' => $curr_period,
        'ALL_SAMPLES' => 0,
        'IDLE' => 0,
        'BUSY' => 0,
    };
}

our %SYNTHETIC_FIELDS = map { $_ => 1 } qw/period ALL_SAMPLES IDLE BUSY/;
sub accumulate_measurements
{
    my ($counts, $comm) = @_;
    $counts->{'ALL_SAMPLES'}++;
    if ($comm eq "swapper") {
        $counts->{'IDLE'}++;
    } else {
        $counts->{'BUSY'}++;
        $counts->{$comm}++;
    }
}

sub find_top_n_comms
{
    my ($counts_by_comm_by_period) = @_;
    my $sum_by_comm = {};
    foreach my $period_counts (@$counts_by_comm_by_period) {
        foreach my $comm (grep {! $SYNTHETIC_FIELDS{$_}} keys %$period_counts) {
            $sum_by_comm->{$comm} += $period_counts->{$comm};
        }
    }

    my @ranked_comms = (
        sort { $sum_by_comm->{$b} <=> $sum_by_comm->{$a} }
        keys %$sum_by_comm
    );
    return [ @ranked_comms[0 .. $NUM_COMMS_TO_SHOW-1] ];
}

sub compute_others_count
{
    my ($period_counts, $top_comms) = @_;
    my $sum_top_comms = 0;
    map { $sum_top_comms += $period_counts->{$_} } @$top_comms;
    $period_counts->{'OTHERS'} = $period_counts->{'BUSY'} - $sum_top_comms;
}

sub show_output_headers
{
    my ($display_fields) = @_;
    printf("%12s: " . join(" ", map {"%16s"} @$display_fields) . "\n",
        'Timestamp',
        @$display_fields
    );
}

sub show_period_summary
{
    my ($period_counts, $display_fields) = @_;
    printf("%12d: " . join(" ", map {"%9d = %3.0f%%"} @$display_fields) . "\n",
        $period_counts->{'period'},
        (map {( $period_counts->{$_}, (100 * $period_counts->{$_} / $period_counts->{'ALL_SAMPLES'}) )} @$display_fields)
    );
}

main();
