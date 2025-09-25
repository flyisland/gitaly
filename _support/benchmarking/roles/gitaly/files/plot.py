import pandas as pd
from plotnine import *
import sys
import json

# Define custom color palette
custom_colors = [
    "#ffd700",
    "#fa8775",
    "#ffb14e",
    "#ea5f94",
    "#cd34b5",
    "#9d02d7",
    "#0000ff",
]

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

    df = (
        df.groupby(["time_interval", "grpc.request.glRepository", "grpc.method", "grpc.code"])
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
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
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
        + facet_grid("grpc.request.glRepository", "grpc.code")
    )

    p.save(f"{outdir}/rpc_count_by_repo.png")


def stats_rpc_latency(df, outdir):
    df = df[df["grpc.request.glRepository"].str.len() > 0]
    df = df[df["grpc.method"].str.len() > 0]
    df = df[df["grpc.time_ms"].notna()]

    df = (
        df.groupby(["time_interval", "grpc.request.glRepository", "grpc.method", "grpc.code"])[
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
        + scale_y_continuous(limits=(0, 12000))
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
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
        + facet_grid("grpc.request.glRepository", "grpc.code")
    )

    p.save(f"{outdir}/rpc_latency_by_repo.png")


def stats_snapshot(df, outdir):
    if "snapshot.duration_ms" not in df.columns:
        print("No snapshot creation events found in the log")
        return

    df = df[df["snapshot.duration_ms"].notna()]
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
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
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
    if "snapshot.duration_ms" not in df.columns:
        print("No snapshot creation events found in the log")
        return

    # Filter for snapshot creation events only
    snapshots = df[df["snapshot.duration_ms"].notna()]

    interval = "1s"  # 1 second windows
    snapshots = with_interval(snapshots, interval)

    metrics = []

    # Group by both time_interval AND snapshot.exclusive
    for time_window in snapshots["time_interval"].unique():
        for exclusive_value in snapshots["snapshot.exclusive"].unique():
            window_data = snapshots[
                (snapshots["time_interval"] == time_window) & 
                (snapshots["snapshot.exclusive"] == exclusive_value)
            ]

            count = len(window_data)
            print(f"There are {count} snapshots (exclusive={exclusive_value}) in window {time_window}")

            if count > 0:
                creation_rate = count / pd.Timedelta(interval).total_seconds()
                p95_latency = window_data["snapshot.duration_ms"].quantile(0.95)
            else:
                creation_rate = 0
                p95_latency = None

            # Calculate latency percentiles
            metrics.append(
                {
                    "time_interval": time_window,
                    "exclusive": exclusive_value,
                    "count": count,
                    "creation_rate_per_sec": creation_rate,
                    "p95_latency_ms": p95_latency,
                }
            )

    metrics_df = pd.DataFrame(metrics)
    
    # Remove rows with no data for cleaner plotting
    plot_data = metrics_df[metrics_df["p95_latency_ms"].notna()]

    # Plot :: Latency vs Creation Rate (Throughput) grouped by exclusive flag
    p = (
        ggplot(plot_data, aes(x="creation_rate_per_sec", color="exclusive"))
        + geom_point(aes(y="p95_latency_ms"), size=3, alpha=0.7)
        + geom_smooth(
            aes(y="p95_latency_ms"), method="lm", se=False, size=1
        )
        + scale_color_manual(values=custom_colors, name="Exclusive")
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
        + theme(figure_size=(12, 7), dpi=200)
        + labs(
            title="Impact of Creation Rate on Snapshot P95 Duration in 1s interval",
            subtitle="P95 latencies vs actual snapshot throughput, grouped by exclusive flag",
            x="Creation Rate - Throughput (snapshots completed/second)",
            y="Snapshot P95 Duration Latency (ms)",
        )
    )
    p.save(f"{outdir}/latency_vs_creation_rate.png")
    print(f"Saved: {outdir}/latency_vs_creation_rate.png")


def analyze_snapshot_duration_by_repository(df, outdir):
    if "snapshot.duration_ms" not in df.columns:
        print("No snapshot creation events found in the log")
        return

    # Filter for snapshot events AND valid repository paths
    snapshots = df[
        (df["snapshot.duration_ms"].notna())
        & (df["grpc.request.glProjectPath"].notna())
    ]

    # Get repository counts
    repo_counts = snapshots["grpc.request.glProjectPath"].value_counts()

    # Plot :: Overlapping histograms of snapshot duration by repository, log scale on X axis
    p = (
        ggplot(
            snapshots, aes(x="snapshot.duration_ms", fill="grpc.request.glProjectPath")
        )
        + geom_histogram(bins=30, alpha=0.5, position="identity")
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
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

def analyze_snapshot_by_files_dirs(df, outdir):
    if "snapshot.duration_ms" not in df.columns:
        print("No snapshot creation events found in the log")
        return

    # Filter for snapshot events AND valid repository paths
    snapshots = df[
        (df["snapshot.duration_ms"].notna())
        & (df["grpc.request.glProjectPath"].notna())
    ]
    
    relevant_cols = ['snapshot.directory_count', 'snapshot.file_count', 'snapshot.duration_ms']
    
    # Plot :: Scatter plot: dirs (x) vs files (y), colored by duration
    p = (
        ggplot(snapshots, aes(x='snapshot.directory_count', y='snapshot.file_count', color='snapshot.duration_ms'))
        + geom_point(size=3, alpha=0.6)
        + scale_color_gradient2(
            low=custom_colors[0],   # yellow for fast
            mid=custom_colors[3],   # pink for medium
            high=custom_colors[-1],  # indigo for slow
            midpoint=snapshots['snapshot.duration_ms'].median(),
            name='Duration (ms)'
        )
        + theme_seaborn(
        style="darkgrid", context="notebook", font="sans-serif", font_scale=1)
        + theme(
            figure_size=(12, 10),
            dpi=200,
            legend_position='right'
        )
        + labs(
            title="Snapshot Duration By Files X Directories",
            subtitle="Each dot represents a snapshot operation",
            x="Directory Count",
            y="File Count"
        )
        + facet_wrap("grpc.request.glRepository", ncol=1)
    )
    
    p.save(f"{outdir}/snapshot_files_dirs_duration.png")
    print(f"\nSaved: {outdir}/snapshot_files_dirs_duration.png")

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
    analyze_snapshot_by_files_dirs(df, output_directory)
