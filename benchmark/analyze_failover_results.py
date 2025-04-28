#!/usr/bin/env python3
import os
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
from datetime import datetime
import seaborn as sns
import argparse
from pathlib import Path

def parse_events_file(events_path):
    """Parse the events.log file to extract failover timing events"""
    events = {}
    try:
        with open(events_path, 'r') as f:
            lines = f.readlines()

        for line in lines:
            # Record basic metadata
            if "Benchmark start time:" in line:
                events["benchmark_start"] = line.split("Benchmark start time:")[1].strip()
            elif "Failover type:" in line:
                events["failover_type"] = line.split("Failover type:")[1].strip()

            # Coordinator failover events
            elif "Coordinator failover triggered at" in line:
                match = re.search(r"timestamp: (\d+)", line)
                if match:
                    events["coordinator_failover_timestamp"] = int(match.group(1))
            elif "Primary coordinator restored at" in line:
                match = re.search(r"timestamp: (\d+)", line)
                if match:
                    events["coordinator_recovery_timestamp"] = int(match.group(1))
            elif "Coordinator failover occurred after" in line:
                match = re.search(r"after (\d+) seconds", line)
                if match:
                    events["coordinator_failover_delay_seconds"] = int(match.group(1))
            elif "Coordinator recovery delay:" in line:
                match = re.search(r"delay: (\d+) seconds", line)
                if match:
                    events["coordinator_recovery_duration_seconds"] = int(match.group(1))

            # Worker failover events
            elif "Worker" in line and "failover triggered at" in line:
                match = re.search(r"timestamp: (\d+)", line)
                if match:
                    events["worker_failover_timestamp"] = int(match.group(1))
                worker_match = re.search(r"Worker(\d+) failover triggered", line)
                if worker_match:
                    events["worker_failed"] = int(worker_match.group(1))
            elif "Worker" in line and "restored at" in line:
                match = re.search(r"timestamp: (\d+)", line)
                if match:
                    events["worker_recovery_timestamp"] = int(match.group(1))
            elif "Worker failover occurred after" in line:
                match = re.search(r"after (\d+) seconds", line)
                if match:
                    events["worker_failover_delay_seconds"] = int(match.group(1))
            elif "Worker recovery delay:" in line:
                match = re.search(r"delay: (\d+) seconds", line)
                if match:
                    events["worker_recovery_duration_seconds"] = int(match.group(1))

            # Sequence information
            elif "Sequence delay between failovers:" in line:
                match = re.search(r"delay: (\d+) seconds", line)
                if match:
                    events["sequence_delay_seconds"] = int(match.group(1))
    except Exception as e:
        print(f"Error parsing events file: {e}")
    return events

def parse_locust_log(log_path):
    """Parse the Locust log file to extract request data over time"""
    data = []

    try:
        with open(log_path, 'r') as f:
            for line in f:
                # Extract timestamp and request data
                if "Name" not in line and "|" in line:
                    parts = line.split("|")
                    if len(parts) >= 7:
                        try:
                            # Extract timestamp (first part)
                            timestamp_str = parts[0].strip()
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")

                            # Extract request type and name
                            name_part = parts[2].strip()
                            req_type = parts[1].strip()

                            # Extract response time
                            resp_time = float(parts[4].strip())

                            # Success or failure
                            status = "Success" if "OK" in line else "Failure"

                            data.append({
                                "timestamp": timestamp,
                                "unix_time": timestamp.timestamp(),
                                "request_name": name_part,
                                "request_type": req_type,
                                "response_time": resp_time,
                                "status": status
                            })
                        except Exception as e:
                            continue  # Skip malformed lines
    except Exception as e:
        print(f"Error parsing Locust log: {e}")

    return pd.DataFrame(data) if data else pd.DataFrame()

def analyze_metrics_files(metrics_path):
    """Extract metrics from the Locust metrics JSON files"""
    metrics_data = {}

    try:
        with open(metrics_path, 'r') as f:
            metrics_data = json.load(f)
    except Exception as e:
        print(f"Error loading metrics file {metrics_path}: {e}")

    return metrics_data

def generate_failover_report(results_dir):
    """Generate comprehensive report on failover performance"""
    results_dir = Path(results_dir)
    print(f"Analyzing failover benchmark results in {results_dir}...")

    # Find the events log
    events_path = results_dir / "events.log"
    events = parse_events_file(events_path)

    # Find Locust logs
    log_files = list(results_dir.glob("locust_*.log"))
    if not log_files:
        print("No Locust log files found.")
        return

    # Parse the first log file found
    df = parse_locust_log(log_files[0])
    if df.empty:
        print("No valid request data found in logs.")
        return

    # Find metrics JSON files
    metrics_files = list(results_dir.glob("locust_metrics_*.json"))
    metrics_data = {}
    if metrics_files:
        metrics_data = analyze_metrics_files(metrics_files[0])

    # Determine which failover events occurred based on the events log
    failover_type = events.get("failover_type", "unknown")
    coord_failover = "coordinator_failover_timestamp" in events
    worker_failover = "worker_failover_timestamp" in events

    # Set up the figure with subplots based on failover events
    if coord_failover and worker_failover:
        fig, axs = plt.subplots(5, 1, figsize=(14, 24),
                              gridspec_kw={'height_ratios': [3, 3, 2, 2, 2]})
    else:
        fig, axs = plt.subplots(4, 1, figsize=(14, 20),
                              gridspec_kw={'height_ratios': [3, 2, 2, 2]})

    # Get the failover and recovery timestamps
    coord_failover_time = events.get("coordinator_failover_timestamp", None)
    coord_recovery_time = events.get("coordinator_recovery_timestamp", None)
    worker_failover_time = events.get("worker_failover_timestamp", None)
    worker_recovery_time = events.get("worker_recovery_timestamp", None)

    # Convert timestamp to seconds relative to start for better plotting
    if not df.empty and df["unix_time"].min() > 0:
        df["relative_time"] = df["unix_time"] - df["unix_time"].min()

        # Calculate relative times for failover events
        base_time = df["unix_time"].min()
        if coord_failover_time:
            coord_failover_relative = coord_failover_time - base_time
            coord_recovery_relative = coord_recovery_time - base_time if coord_recovery_time else None

        if worker_failover_time:
            worker_failover_relative = worker_failover_time - base_time
            worker_recovery_relative = worker_recovery_time - base_time if worker_recovery_time else None

    # 1. Response Time Over Time with Failover Markers - focus on all events
    if not df.empty:
        ax_idx = 0  # First subplot
        sns.scatterplot(
            x="relative_time",
            y="response_time",
            hue="status",
            style="request_type",
            alpha=0.7,
            data=df,
            ax=axs[ax_idx]
        )

        # Add coordinator failover event lines if applicable
        if coord_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=coord_failover_relative, color='r', linestyle='--',
                          label='Coordinator Failover')
            if coord_recovery_time:
                axs[ax_idx].axvline(x=coord_recovery_relative, color='g', linestyle='--',
                              label='Coordinator Restored')

        # Add worker failover event lines if applicable
        if worker_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=worker_failover_relative, color='orange', linestyle='-.',
                          label=f'Worker{events.get("worker_failed", "")} Failover')
            if worker_recovery_time:
                axs[ax_idx].axvline(x=worker_recovery_relative, color='lime', linestyle='-.',
                              label=f'Worker{events.get("worker_failed", "")} Restored')

        axs[ax_idx].set_title("Response Time During Failover Events", fontsize=16)
        axs[ax_idx].set_xlabel("Time (seconds)", fontsize=12)
        axs[ax_idx].set_ylabel("Response Time (ms)", fontsize=12)
        axs[ax_idx].set_yscale('log')  # Log scale for better visualization of spikes
        axs[ax_idx].legend(fontsize=10)
        axs[ax_idx].grid(True, which="both", linestyle="--", linewidth=0.5)

        # If we have both types of failover, add a second response time plot focused on each event
        if coord_failover and worker_failover:
            ax_idx += 1

            # Create focused plots around each failover event
            # We'll plot two separate views, one focused on each failover event
            sns.scatterplot(
                x="relative_time",
                y="response_time",
                hue="request_type",
                style="status",
                alpha=0.7,
                data=df,
                ax=axs[ax_idx]
            )

            # Add coordinator failover event lines with clear labels
            if coord_failover_time:
                axs[ax_idx].axvline(x=coord_failover_relative, color='r', linestyle='--',
                              linewidth=2, label='Coordinator Failover')
                if coord_recovery_time:
                    axs[ax_idx].axvline(x=coord_recovery_relative, color='g', linestyle='--',
                                  linewidth=2, label='Coordinator Restored')

            # Add worker failover event lines with different style
            if worker_failover_time:
                axs[ax_idx].axvline(x=worker_failover_relative, color='orange', linestyle='-.',
                              linewidth=2, label=f'Worker{events.get("worker_failed", "")} Failover')
                if worker_recovery_time:
                    axs[ax_idx].axvline(x=worker_recovery_relative, color='lime', linestyle='-.',
                                  linewidth=2, label=f'Worker{events.get("worker_failed", "")} Restored')

            # Add annotations to clarify what's happening
            if coord_failover_time and worker_failover_time:
                mid_y = np.mean([axs[ax_idx].get_ylim()[0], axs[ax_idx].get_ylim()[1]])
                axs[ax_idx].annotate('Coordinator\nFailover',
                                xy=(coord_failover_relative, mid_y),
                                xytext=(coord_failover_relative+5, mid_y*1.5),
                                arrowprops=dict(facecolor='red', shrink=0.05, width=2))
                axs[ax_idx].annotate(f'Worker{events.get("worker_failed", "")}\nFailover',
                                xy=(worker_failover_relative, mid_y),
                                xytext=(worker_failover_relative+5, mid_y*0.7),
                                arrowprops=dict(facecolor='orange', shrink=0.05, width=2))

            axs[ax_idx].set_title("Response Time Comparison: Coordinator vs Worker Failover", fontsize=16)
            axs[ax_idx].set_xlabel("Time (seconds)", fontsize=12)
            axs[ax_idx].set_ylabel("Response Time (ms)", fontsize=12)
            axs[ax_idx].set_yscale('log')
            axs[ax_idx].legend(fontsize=10)
            axs[ax_idx].grid(True, which="both", linestyle="--", linewidth=0.5)

        ax_idx += 1  # Increment for the next plot

    # 2. Request Rate Over Time
    if not df.empty:
        # Calculate request rate (requests per second)
        df["timestamp_second"] = df["relative_time"].apply(lambda x: int(x))
        requests_per_second = df.groupby("timestamp_second").size().reset_index(name="request_count")

        axs[ax_idx].plot(requests_per_second["timestamp_second"], requests_per_second["request_count"],
                   marker="o", linestyle="-", alpha=0.7, markersize=4)

        # Add coordinator failover event lines if applicable
        if coord_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=coord_failover_relative, color='r', linestyle='--',
                          label='Coordinator Failover')
            if coord_recovery_time:
                axs[ax_idx].axvline(x=coord_recovery_relative, color='g', linestyle='--',
                              label='Coordinator Restored')

        # Add worker failover event lines if applicable
        if worker_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=worker_failover_relative, color='orange', linestyle='-.',
                          label=f'Worker{events.get("worker_failed", "")} Failover')
            if worker_recovery_time:
                axs[ax_idx].axvline(x=worker_recovery_relative, color='lime', linestyle='-.',
                              label=f'Worker{events.get("worker_failed", "")} Restored')

        axs[ax_idx].set_title("Request Rate During Failover", fontsize=16)
        axs[ax_idx].set_xlabel("Time (seconds)", fontsize=12)
        axs[ax_idx].set_ylabel("Requests per Second", fontsize=12)
        axs[ax_idx].grid(True, linestyle="--", linewidth=0.5)
        axs[ax_idx].legend(fontsize=10)

        ax_idx += 1  # Increment for the next plot

    # 3. Error Rate Over Time
    if not df.empty:
        # Calculate error rate per second
        df["is_error"] = df["status"] != "Success"
        error_rate = df.groupby("timestamp_second")["is_error"].mean().reset_index(name="error_rate")
        error_rate["error_percentage"] = error_rate["error_rate"] * 100

        axs[ax_idx].plot(error_rate["timestamp_second"], error_rate["error_percentage"],
                   color='red', marker="o", linestyle="-", alpha=0.7, markersize=4)

        # Add coordinator failover event lines if applicable
        if coord_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=coord_failover_relative, color='r', linestyle='--',
                          label='Coordinator Failover')
            if coord_recovery_time:
                axs[ax_idx].axvline(x=coord_recovery_relative, color='g', linestyle='--',
                              label='Coordinator Restored')

        # Add worker failover event lines if applicable
        if worker_failover_time and df["unix_time"].min() > 0:
            axs[ax_idx].axvline(x=worker_failover_relative, color='orange', linestyle='-.',
                          label=f'Worker{events.get("worker_failed", "")} Failover')
            if worker_recovery_time:
                axs[ax_idx].axvline(x=worker_recovery_relative, color='lime', linestyle='-.',
                              label=f'Worker{events.get("worker_failed", "")} Restored')

        axs[ax_idx].set_title("Error Rate During Failover", fontsize=16)
        axs[ax_idx].set_xlabel("Time (seconds)", fontsize=12)
        axs[ax_idx].set_ylabel("Error Percentage (%)", fontsize=12)
        axs[ax_idx].set_ylim(bottom=0, top=max(100, error_rate["error_percentage"].max() * 1.1))
        axs[ax_idx].grid(True, linestyle="--", linewidth=0.5)
        axs[ax_idx].legend(fontsize=10)

        ax_idx += 1  # Increment for the next plot

    # 4. Response Time Distribution by Phase
    if not df.empty:
        # Define phases based on observed failover events
        df["phase"] = "Normal Operation"

        if coord_failover_time and coord_recovery_time:
            # Mark coordinator failover period
            df.loc[(df["relative_time"] >= coord_failover_relative) &
                  (df["relative_time"] < coord_recovery_relative), "phase"] = "During Coordinator Failover"

        if worker_failover_time and worker_recovery_time:
            # Mark worker failover period
            df.loc[(df["relative_time"] >= worker_failover_relative) &
                  (df["relative_time"] < worker_recovery_relative), "phase"] = "During Worker Failover"

            # Handle overlapping periods
            if coord_failover_time and worker_failover_time:
                overlap_start = max(coord_failover_relative, worker_failover_relative)
                overlap_end = min(coord_recovery_relative or float('inf'),
                                worker_recovery_relative or float('inf'))
                if overlap_end > overlap_start:
                    df.loc[(df["relative_time"] >= overlap_start) &
                          (df["relative_time"] < overlap_end), "phase"] = "During Both Failovers"

        # Add recovery phases
        if coord_recovery_time:
            df.loc[df["relative_time"] >= coord_recovery_time, "phase"] = "After Recovery"

        # Create boxplot for different phases
        order = ["Normal Operation", "During Coordinator Failover",
                "During Worker Failover", "During Both Failovers", "After Recovery"]
        order = [phase for phase in order if phase in df["phase"].unique()]

        sns.boxplot(x="phase", y="response_time", data=df, ax=axs[ax_idx], order=order)
        axs[ax_idx].set_title("Response Time Distribution by Phase", fontsize=16)
        axs[ax_idx].set_xlabel("Failover Phase", fontsize=12)
        axs[ax_idx].set_ylabel("Response Time (ms)", fontsize=12)
        axs[ax_idx].set_yscale('log')
        axs[ax_idx].set_xticklabels(axs[ax_idx].get_xticklabels(), rotation=45, ha="right")
        axs[ax_idx].grid(True, linestyle="--", linewidth=0.5)

    # Add overall title with summary information
    title_text = "Failover Benchmark Analysis\n"
    if events:
        title_text += f"Failover Type: {failover_type}\n"
        if "coordinator_failover_delay_seconds" in events:
            title_text += f"Coordinator failover occurred after {events['coordinator_failover_delay_seconds']} seconds\n"
        if "worker_failover_delay_seconds" in events:
            title_text += f"Worker{events.get('worker_failed', '')} failover occurred after {events['worker_failover_delay_seconds']} seconds\n"

    plt.suptitle(title_text, fontsize=18)
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    # Save the figure
    output_path = results_dir / "failover_analysis.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Failover analysis chart saved to {output_path}")

    # Generate summary statistics
    if not df.empty:
        # Calculate summary statistics by phase
        phases = df["phase"].unique()
        summary = {}
        error_rates = {}

        for phase in phases:
            phase_data = df[df["phase"] == phase]
            summary[phase] = phase_data["response_time"].describe()
            error_rates[phase] = phase_data["is_error"].mean() * 100

        # Save summary to file
        with open(results_dir / "failover_summary.txt", 'w') as f:
            f.write("FAILOVER BENCHMARK ANALYSIS SUMMARY\n")
            f.write("===================================\n\n")

            f.write("TEST CONFIGURATION:\n")
            f.write(f"- Failover type: {failover_type}\n")
            for event, value in events.items():
                if event.endswith("_seconds") or event.endswith("_timestamp") or event == "worker_failed":
                    f.write(f"- {event}: {value}\n")

            f.write("\nRESPONSE TIME STATISTICS (ms) BY PHASE:\n")
            for phase, stats in summary.items():
                f.write(f"\n{phase}:\n")
                f.write(f"  Count: {stats['count']:.0f}\n")
                f.write(f"  Mean: {stats['mean']:.2f}\n")
                f.write(f"  Median: {stats['50%']:.2f}\n")
                f.write(f"  95th percentile: {stats['75%'] + 1.96*(stats['75%'] - stats['25%']):.2f}\n")
                f.write(f"  Max: {stats['max']:.2f}\n")

            f.write("\nERROR RATES BY PHASE:\n")
            for phase, rate in error_rates.items():
                f.write(f"- {phase}: {rate:.2f}%\n")

            f.write("\nIMPACT ANALYSIS:\n")

            # Impact of coordinator failover
            if "Normal Operation" in summary and "During Coordinator Failover" in summary:
                coord_impact = (summary["During Coordinator Failover"]["mean"] /
                               summary["Normal Operation"]["mean"]) - 1
                f.write(f"- Response time increase during coordinator failover: {coord_impact * 100:.2f}%\n")

                error_impact = error_rates.get("During Coordinator Failover", 0) - error_rates.get("Normal Operation", 0)
                f.write(f"- Error rate increase during coordinator failover: {error_impact:.2f} percentage points\n")

            # Impact of worker failover
            if "Normal Operation" in summary and "During Worker Failover" in summary:
                worker_impact = (summary["During Worker Failover"]["mean"] /
                                summary["Normal Operation"]["mean"]) - 1
                f.write(f"- Response time increase during worker failover: {worker_impact * 100:.2f}%\n")

                error_impact = error_rates.get("During Worker Failover", 0) - error_rates.get("Normal Operation", 0)
                f.write(f"- Error rate increase during worker failover: {error_impact:.2f} percentage points\n")

            # Recovery comparison
            if "Normal Operation" in summary and "After Recovery" in summary:
                recovery_ratio = summary["After Recovery"]["mean"] / summary["Normal Operation"]["mean"]
                f.write(f"- Post-recovery vs normal operation response time ratio: {recovery_ratio:.2f}x\n")

            # Comparative analysis of coordinator vs worker failover if both were tested
            if "During Coordinator Failover" in summary and "During Worker Failover" in summary:
                comp_ratio = summary["During Coordinator Failover"]["mean"] / summary["During Worker Failover"]["mean"]
                f.write(f"\nCOMPARATIVE ANALYSIS:\n")
                if comp_ratio > 1:
                    f.write(f"- Coordinator failover had {comp_ratio:.2f}x higher impact on response time than worker failover\n")
                else:
                    f.write(f"- Worker failover had {1/comp_ratio:.2f}x higher impact on response time than coordinator failover\n")

                error_comp = error_rates.get("During Coordinator Failover", 0) - error_rates.get("During Worker Failover", 0)
                if error_comp > 0:
                    f.write(f"- Coordinator failover had {abs(error_comp):.2f} percentage points higher error rate than worker failover\n")
                else:
                    f.write(f"- Worker failover had {abs(error_comp):.2f} percentage points higher error rate than coordinator failover\n")

        print(f"Failover summary report saved to {results_dir / 'failover_summary.txt'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze failover benchmark results")
    parser.add_argument("results_dir", type=str, help="Path to the failover benchmark results directory")
    args = parser.parse_args()

    if not os.path.isdir(args.results_dir):
        print(f"Error: {args.results_dir} is not a valid directory")
        sys.exit(1)

    generate_failover_report(args.results_dir)