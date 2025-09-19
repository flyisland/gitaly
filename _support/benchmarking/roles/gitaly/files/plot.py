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


def stats_rpc_count(df, outdir):
    df = df[df["grpc.request.glRepository"].str.len() > 0]
    df = df[df["grpc.method"].str.len() > 0]
    df = df[df["grpc.code"] == "OK"]

    df = (
        df.groupby(["time_interval", "grpc.request.glRepository", "grpc.method"])
        .size()
        .reset_index(name="request_count")
    )

    with open(f"{outdir}/rpc_count_by_repo.txt", "w") as f:
        f.write(df.to_string(index=False))

    p = (
        ggplot(
            df,
            aes(
                x="time_interval",
                y="request_count",
                color="grpc.method",
                shape="grpc.request.glRepository",
            ),
        )
        + geom_line()
        + scale_x_datetime(date_labels="%H:%M:%S", date_breaks="5 seconds")
        + theme(
            axis_text_x=element_text(rotation=45, hjust=1), figure_size=(12, 8), dpi=200
        )
        + labs(
            title="gRPC Request Count",
            x="Time",
            y="Count",
            color="Method",
            shape="Repository",
        )
        + facet_wrap("grpc.request.glRepository", ncol=1)
    )

    p.save(f"{outdir}/rpc_count_by_repo.png")


def stats_rpc_latency(df, outdir):
    df = df[df["grpc.request.glRepository"].str.len() > 0]
    df = df[df["grpc.method"].str.len() > 0]
    df = df[df["grpc.time_ms"].notna()]

    df = (
        df.groupby(["time_interval", "grpc.request.glRepository", "grpc.method"])[
            "grpc.time_ms"
        ]
        .quantile(0.95)
        .reset_index()
    )
    with open(f"{outdir}/rpc_latency_by_repo.txt", "w") as f:
        f.write(df.to_string(index=False))

    p = (
        ggplot(
            df,
            aes(
                x="time_interval",
                y="grpc.time_ms",
                color="grpc.method",
                shape="grpc.request.glRepository",
            ),
        )
        + geom_line()
        + scale_x_datetime(date_labels="%H:%M:%S", date_breaks="5 seconds")
        + theme(
            axis_text_x=element_text(rotation=45, hjust=1), figure_size=(12, 16), dpi=200
        )
        + labs(
            title="gRPC Response Latency",
            x="Time",
            y="Latency (ms, p95)",
            color="Method",
            shape="Repository",
        )
        + facet_wrap("grpc.request.glRepository", ncol=1)
    )

    p.save(f"{outdir}/rpc_latency_by_repo.png")


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


def analyze_snapshot_duration_by_repository(df, outdir):
    # Filter for snapshot events AND valid repository paths
    snapshots = df[
        (df["snapshot.duration_ms"].notna())
        & (df["grpc.request.glProjectPath"].notna())
    ]

    if len(snapshots) == 0:
        print("No snapshot events with valid repository paths found")
        return

    # Get repository counts
    repo_counts = snapshots["grpc.request.glProjectPath"].value_counts()

    # Define custom color palette
    custom_colors = [
        "#ffd700",
        "#ffb14e",
        "#fa8775",
        "#ea5f94",
        "#cd34b5",
        "#9d02d7",
        "#0000ff",
    ]

    # Plot :: Overlapping histograms of snapshot duration by repository, log scale on X axis
    p = (
        ggplot(
            snapshots, aes(x="snapshot.duration_ms", fill="grpc.request.glProjectPath")
        )
        + geom_histogram(bins=30, alpha=0.5, position="identity")
        + theme_minimal()
        + theme(
            figure_size=(16, 10),
            dpi=200,
            legend_position="right",
            legend_title=element_text(size=10, weight="bold"),
            legend_text=element_text(size=8),
        )
        + labs(
            title="Snapshot Duration Distribution by Repository",
            subtitle="Overlapping histograms show how snapshot duration varies across repositories",
            x="Duration (ms) - Log Scale",
            y="Count",
        )
        + scale_x_log10()
        + scale_fill_manual(
            values=custom_colors * 3, name="Repository"
        )  # *3 to ensure enough colors, however they will start repetiting
    )
    p.save(f"{outdir}/snapshot_duration_by_repository.png")
    print(f"\nSaved: {outdir}/snapshot_duration_by_repository.png")


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
    stats_rpc_latency(df, output_directory)
    stats_rpc_count(df, output_directory)
    analyze_snapshot_creation_rate(df, output_directory)
    analyze_snapshot_duration_by_repository(df, output_directory)
