import pandas as pd
from plotnine import *
import sys
import json


def load(fname):
    # The log file is a newline-separated collection of JSON objects, each of which can
    # be nested and needs to be flattened.
    df = pd.json_normalize(pd.Series(open(fname).readlines()).apply(json.loads))

    # The time column is often used for aggregations.
    df["time"] = pd.to_datetime(df["time"])
    return df


def stats_snapshot(df, outdir):
    df = df[df["snapshot.duration_ms"].notna()]
    if len(df) == 0:
        print("No snapshot creation events found in the log")
        return

    df = df[df["grpc.request.glRepository"].notna()]

    df = (
        df.groupby(["time_interval", "grpc.request.glRepository"])[
            "snapshot.duration_ms"
        ]
        .quantile(0.95)
        .reset_index()
    )
    with open(f"{outdir}/snapshot_creation_latency_by_repo.txt", "w") as f:
        f.write(df.to_string(index=False))

    p = (
        ggplot(
            df,
            aes(
                x="time_interval",
                y="snapshot.duration_ms",
                color="grpc.request.glRepository",
            ),
        )
        + geom_line()
        + scale_x_datetime(date_labels="%H:%M:%S", date_breaks="5 seconds")
        + theme(
            axis_text_x=element_text(rotation=45, hjust=1), figure_size=(12, 8), dpi=200
        )
        + labs(
            title="Snapshot Creation Latency",
            x="Time",
            y="Latency (ms, p95)",
            color="Repository",
        )
    )

    p.save(f"{outdir}/snapshot_creation_latency_by_repo.png")


def analyze_snapshot_creation_rate(df, outdir):
    # Filter for snapshot creation events only
    snapshots = df[df["snapshot.duration_ms"].notna()]

    if len(snapshots) == 0:
        print("No snapshot creation events found in the log")
        return

    interval = "10s"  # 10-second windows
    snapshots = with_interval(snapshots, interval)

    metrics = []

    for time_window in snapshots["time_interval"].unique():
        window_data = snapshots[snapshots["time_interval"] == time_window]

        count = len(window_data)
        print(f"There are { count} snapshots in this window {time_window}")

        if count > 0:
            creation_rate = count / pd.Timedelta(interval).total_seconds()
        else:
            creation_rate = 0

        # Calculate latency percentiles
        metrics.append(
            {
                "time_interval": time_window,
                "count": count,
                "creation_rate_per_sec": creation_rate,
                "p95_latency_ms": window_data["snapshot.duration_ms"].quantile(0.95),
            }
        )

    metrics_df = pd.DataFrame(metrics)

    # Plot :: Latency vs Creation Rate (Throughput)
    p1 = (
        ggplot(metrics_df, aes(x="creation_rate_per_sec"))
        + geom_point(aes(y="p95_latency_ms"), color="#0000ff", size=3, alpha=0.7)
        + geom_smooth(
            aes(y="p95_latency_ms"), color="#0000ff", method="lm", se=False, size=1
        )
        + theme_minimal()
        + theme(figure_size=(12, 7), dpi=200)
        + labs(
            title="Impact of Creation Rate (Throughput) on Snapshot Creation Speed",
            subtitle="P95 (blue) latencies vs actual snapshot throughput",
            x="Creation Rate - Throughput (snapshots completed/second)",
            y="Snapshot Creation Latency (ms)",
        )
    )
    p1.save(f"{outdir}/latency_vs_creation_rate.png")
    print(f"Saved: {outdir}/latency_vs_creation_rate.png")


def with_interval(df, interval):
    df["time_interval"] = df["time"].dt.floor(interval)
    return df


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: plot.py <gitaly.log> <output dir>")
        sys.exit(1)

    log_filename = sys.argv[1]
    output_directory = sys.argv[2]

    df = load(log_filename)

    with_interval(df, "1s")
    stats_snapshot(df, output_directory)
    analyze_snapshot_creation_rate(df, output_directory)
