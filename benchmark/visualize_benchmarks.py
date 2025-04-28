#!/usr/bin/env python3
"""
PostgreSQL/Citus Benchmark Visualization Tool

A tool for visualizing and comparing benchmark results from latency_monitor.py
and locust_benchmark.py output JSON files.
"""

import os
import json
import glob
import argparse
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from matplotlib.ticker import MaxNLocator

# Colors for consistent visualization
COLORS = {
    'read': '#1f77b4',        # Blue
    'write': '#ff7f0e',       # Orange
    'spatial': '#2ca02c',     # Green
    'total': '#9467bd',       # Purple
    'p95': '#d62728',         # Red
    'avg': '#8c564b',         # Brown
    'min': '#e377c2',         # Pink
    'max': '#7f7f7f',         # Gray
    'baseline': '#17becf',    # Light blue
    'current': '#bcbd22',     # Yellow-green
}

class BenchmarkVisualizer:
    """Visualize benchmark results from Citus PostgreSQL benchmarks"""

    def __init__(self, data_dir="benchmark_results", output_dir="benchmark_results/graphs"):
        """Initialize the visualizer with directory paths"""
        self.data_dir = data_dir
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Data containers
        self.locust_results = []
        self.latency_monitor_results = []
        self.result_files = {}

        # Load the benchmark results
        self.load_data()

    def load_data(self):
        """Load benchmark result data from JSON files"""
        print(f"Scanning for benchmark results in {self.data_dir}")

        # Find all JSON result files
        json_pattern = os.path.join(self.data_dir, "**", "*.json")
        json_files = glob.glob(json_pattern, recursive=True)

        # Process each file
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Determine the file type
                if "locust_metrics" in os.path.basename(file_path):
                    # This is a Locust benchmark result
                    data['file_path'] = file_path
                    data['timestamp_dt'] = datetime.fromisoformat(data['timestamp'])
                    self.locust_results.append(data)
                    self.result_files[file_path] = "locust"

                elif "latency_monitor" in os.path.basename(file_path):
                    # This is a latency monitor result
                    data['file_path'] = file_path
                    data['timestamp_dt'] = datetime.fromisoformat(data['timestamp'])
                    self.latency_monitor_results.append(data)
                    self.result_files[file_path] = "latency_monitor"

            except Exception as e:
                print(f"Error loading {file_path}: {e}")

        # Sort results by timestamp
        self.locust_results.sort(key=lambda x: x['timestamp_dt'])
        self.latency_monitor_results.sort(key=lambda x: x['timestamp_dt'])

        print(f"Loaded {len(self.locust_results)} Locust benchmark results")
        print(f"Loaded {len(self.latency_monitor_results)} latency monitor results")

    def create_latency_trend_plot(self, results, title_prefix, filename_prefix, metric_key='latency_ms'):
        """Create a plot showing latency trends over time"""
        if not results:
            print(f"No data available for {title_prefix} latency trend plot")
            return

        # Create figure
        plt.figure(figsize=(12, 8))

        # Extract timestamps and metrics
        timestamps = [r['timestamp_dt'] for r in results]
        avg_latencies = [r[metric_key]['avg'] for r in results]
        p95_latencies = [r[metric_key]['p95'] for r in results]
        min_latencies = [r[metric_key]['min'] for r in results]
        max_latencies = [r[metric_key]['max'] for r in results]

        # Plot the data
        plt.plot(timestamps, avg_latencies, 'o-', color=COLORS['avg'], label='Average Latency')
        plt.plot(timestamps, p95_latencies, 's-', color=COLORS['p95'], label='p95 Latency')
        plt.plot(timestamps, min_latencies, '^-', color=COLORS['min'], label='Min Latency', alpha=0.7)
        plt.plot(timestamps, max_latencies, 'v-', color=COLORS['max'], label='Max Latency', alpha=0.7)

        # Format the plot
        plt.title(f"{title_prefix} Latency Trend")
        plt.xlabel("Benchmark Date")
        plt.ylabel("Latency (ms)")
        plt.legend()
        plt.grid(True, alpha=0.3)

        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        plt.xticks(rotation=45)

        # Use log scale if range is large
        if max(max_latencies) / max(min_latencies) > 100:
            plt.yscale('log')

        plt.tight_layout()

        # Save the plot
        filename = os.path.join(self.output_dir, f"{filename_prefix}_latency_trend.png")
        plt.savefig(filename)
        print(f"Saved latency trend plot to {filename}")

        plt.close()

    def create_throughput_trend_plot(self, results, title_prefix, filename_prefix):
        """Create a plot showing throughput trends over time"""
        if not results:
            print(f"No data available for {title_prefix} throughput trend plot")
            return

        # Create figure
        plt.figure(figsize=(12, 8))

        # Extract timestamps and metrics
        timestamps = [r['timestamp_dt'] for r in results]

        # Check if we're dealing with Locust or latency_monitor results
        if 'throughput_ops_sec' in results[0]:
            # Locust results
            total_throughput = [r['throughput_ops_sec']['total'] for r in results]
            read_throughput = [r['throughput_ops_sec']['read'] for r in results]
            write_throughput = [r['throughput_ops_sec']['write'] for r in results]

            # Plot the data
            plt.plot(timestamps, total_throughput, 'o-', color=COLORS['total'], label='Total')
            plt.plot(timestamps, read_throughput, 's-', color=COLORS['read'], label='Read')
            plt.plot(timestamps, write_throughput, '^-', color=COLORS['write'], label='Write')

        else:
            # latency_monitor results
            throughput = [r['throughput_qps'] for r in results]
            query_types = [r['query_type'] for r in results]

            # Create a dataframe for easier grouping
            df = pd.DataFrame({
                'timestamp': timestamps,
                'throughput': throughput,
                'query_type': query_types
            })

            # Group by query type and plot
            for query_type in df['query_type'].unique():
                subset = df[df['query_type'] == query_type]
                plt.plot(subset['timestamp'], subset['throughput'], 'o-',
                         label=query_type, alpha=0.7)

            # Also plot the overall average line
            plt.plot(timestamps, throughput, 'o-', color=COLORS['total'],
                     label='All Query Types', alpha=1.0)

        # Format the plot
        plt.title(f"{title_prefix} Throughput Trend")
        plt.xlabel("Benchmark Date")
        plt.ylabel("Throughput (queries per second)")
        plt.legend()
        plt.grid(True, alpha=0.3)

        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        plt.xticks(rotation=45)

        plt.tight_layout()

        # Save the plot
        filename = os.path.join(self.output_dir, f"{filename_prefix}_throughput_trend.png")
        plt.savefig(filename)
        print(f"Saved throughput trend plot to {filename}")

        plt.close()

    def create_comparison_plot(self, baseline_file, target_file):
        """Create a comparison between two benchmark results"""
        if baseline_file not in self.result_files or target_file not in self.result_files:
            print("One or both of the specified files were not found in the loaded results")
            return

        # Load the data
        with open(baseline_file, 'r') as f:
            baseline = json.load(f)

        with open(target_file, 'r') as f:
            target = json.load(f)

        # Determine the file type
        baseline_type = self.result_files[baseline_file]
        target_type = self.result_files[target_file]

        if baseline_type != target_type:
            print("Cannot compare results of different types")
            return

        # Create figure
        fig = plt.figure(figsize=(14, 10))

        if baseline_type == "locust":
            # Locust comparison
            self._create_locust_comparison(baseline, target, fig)
        else:
            # Latency monitor comparison
            self._create_latency_monitor_comparison(baseline, target, fig)

        # Save the plot
        baseline_name = os.path.basename(baseline_file).replace(".json", "")
        target_name = os.path.basename(target_file).replace(".json", "")
        filename = os.path.join(self.output_dir, f"compare_{baseline_name}_vs_{target_name}.png")
        plt.savefig(filename)
        print(f"Saved comparison plot to {filename}")

        plt.close()

    def _create_locust_comparison(self, baseline, target, fig):
        """Create a comparison plot for Locust benchmark results"""
        # Create a grid of subplots
        gs = gridspec.GridSpec(2, 2)

        # Latency comparison (Avg and p95)
        ax1 = fig.add_subplot(gs[0, 0])
        self._plot_metric_comparison(ax1, baseline, target,
                                    "read_latency_ms", ["avg", "p95"],
                                    "Read Latency (ms)")

        # Write latency comparison
        ax2 = fig.add_subplot(gs[0, 1])
        self._plot_metric_comparison(ax2, baseline, target,
                                    "write_latency_ms", ["avg", "p95"],
                                    "Write Latency (ms)")

        # Throughput comparison
        ax3 = fig.add_subplot(gs[1, 0])
        self._plot_metric_comparison(ax3, baseline, target,
                                    "throughput_ops_sec", ["total", "read", "write"],
                                    "Throughput (ops/sec)")

        # Operation count comparison
        ax4 = fig.add_subplot(gs[1, 1])
        self._plot_metric_comparison(ax4, baseline, target,
                                    "operations", ["total", "read", "write"],
                                    "Operation Counts")

        # Add title and timestamps
        baseline_time = baseline["timestamp_dt"] if "timestamp_dt" in baseline else datetime.fromisoformat(baseline["timestamp"])
        target_time = target["timestamp_dt"] if "timestamp_dt" in target else datetime.fromisoformat(target["timestamp"])
        fig.suptitle(f"Benchmark Comparison\nBaseline: {baseline_time.strftime('%Y-%m-%d %H:%M')}, Target: {target_time.strftime('%Y-%m-%d %H:%M')}")

        plt.tight_layout()

    def _create_latency_monitor_comparison(self, baseline, target, fig):
        """Create a comparison plot for latency monitor results"""
        # Create a grid of subplots
        gs = gridspec.GridSpec(2, 2)

        # Latency comparison
        ax1 = fig.add_subplot(gs[0, 0])
        metrics = ["avg", "p95", "min", "max"]
        self._plot_basic_comparison(ax1, baseline, target,
                                  "latency_ms", metrics,
                                  "Latency (ms)")

        # Throughput comparison
        ax2 = fig.add_subplot(gs[0, 1])
        self._plot_basic_scalar_comparison(ax2, baseline, target,
                                         "throughput_qps",
                                         "Throughput (queries/sec)")

        # Query counts
        ax3 = fig.add_subplot(gs[1, 0])
        self._plot_basic_scalar_comparison(ax3, baseline, target,
                                         "total_queries",
                                         "Total Queries")

        # Client count comparison
        ax4 = fig.add_subplot(gs[1, 1])
        self._plot_basic_scalar_comparison(ax4, baseline, target,
                                         "client_count",
                                         "Client Count")

        # Add title and timestamps
        baseline_time = baseline["timestamp_dt"] if "timestamp_dt" in baseline else datetime.fromisoformat(baseline["timestamp"])
        target_time = target["timestamp_dt"] if "timestamp_dt" in target else datetime.fromisoformat(target["timestamp"])

        subtitle = f"Baseline: {baseline['query_type']} ({baseline_time.strftime('%Y-%m-%d %H:%M')}) vs Target: {target['query_type']} ({target_time.strftime('%Y-%m-%d %H:%M')})"
        fig.suptitle(f"Latency Monitor Comparison\n{subtitle}")

        plt.tight_layout()

    def _plot_metric_comparison(self, ax, baseline, target, metric_group, metrics, ylabel):
        """Plot a comparison of metrics between baseline and target"""
        x = np.arange(len(metrics))
        width = 0.35

        # Extract values
        baseline_values = [baseline[metric_group][m] for m in metrics if m in baseline[metric_group]]
        target_values = [target[metric_group][m] for m in metrics if m in target[metric_group]]

        # Ensure both arrays have the same length by finding common metrics
        common_metrics = [m for m in metrics if m in baseline[metric_group] and m in target[metric_group]]
        if not common_metrics:
            print(f"Warning: No common metrics found in {metric_group}")
            return

        baseline_values = [baseline[metric_group][m] for m in common_metrics]
        target_values = [target[metric_group][m] for m in common_metrics]

        # Calculate percent change
        pct_change = [(target_values[i] - baseline_values[i]) / baseline_values[i] * 100
                      if baseline_values[i] > 0 else 0 for i in range(len(common_metrics))]

        # Create bars
        rects1 = ax.bar(x - width/2, baseline_values, width, label='Baseline', color=COLORS['baseline'])
        rects2 = ax.bar(x + width/2, target_values, width, label='Target', color=COLORS['current'])

        # Add labels and formatting
        ax.set_ylabel(ylabel)
        ax.set_title(f"{metric_group.replace('_', ' ').title()}")
        ax.set_xticks(x)
        ax.set_xticklabels(common_metrics)
        ax.legend()

        # Add value annotations
        self._autolabel(ax, rects1, pct_change, True)
        self._autolabel(ax, rects2, pct_change, False)

    def _plot_basic_comparison(self, ax, baseline, target, metric_group, metrics, ylabel):
        """Plot a basic comparison of metrics between baseline and target"""
        x = np.arange(len(metrics))
        width = 0.35

        # Find common metrics that exist in both baseline and target
        common_metrics = [m for m in metrics if m in baseline[metric_group] and m in target[metric_group]]

        if not common_metrics:
            print(f"Warning: No common metrics found in {metric_group}")
            return

        # Extract values using only common metrics
        baseline_values = [baseline[metric_group][m] for m in common_metrics]
        target_values = [target[metric_group][m] for m in common_metrics]

        # Calculate percent change
        pct_change = [(target_values[i] - baseline_values[i]) / baseline_values[i] * 100
                      if baseline_values[i] > 0 else 0 for i in range(len(common_metrics))]

        # Adjust x range for the actual number of metrics we're plotting
        x = np.arange(len(common_metrics))

        # Create bars
        rects1 = ax.bar(x - width/2, baseline_values, width, label='Baseline', color=COLORS['baseline'])
        rects2 = ax.bar(x + width/2, target_values, width, label='Target', color=COLORS['current'])

        # Add labels and formatting
        ax.set_ylabel(ylabel)
        ax.set_title(f"{metric_group.replace('_', ' ').title()}")
        ax.set_xticks(x)
        ax.set_xticklabels(common_metrics)
        ax.legend()

        # Add value annotations
        self._autolabel(ax, rects1, pct_change, True)
        self._autolabel(ax, rects2, pct_change, False)

    def _plot_basic_scalar_comparison(self, ax, baseline, target, metric, ylabel):
        """Plot a comparison of a scalar metric between baseline and target"""
        x = np.arange(1)
        width = 0.35

        # Extract values
        baseline_value = baseline[metric]
        target_value = target[metric]

        # Calculate percent change
        pct_change = [(target_value - baseline_value) / baseline_value * 100
                      if baseline_value > 0 else 0]

        # Create bars
        rects1 = ax.bar(x - width/2, [baseline_value], width, label='Baseline', color=COLORS['baseline'])
        rects2 = ax.bar(x + width/2, [target_value], width, label='Target', color=COLORS['current'])

        # Add labels and formatting
        ax.set_ylabel(ylabel)
        ax.set_title(f"{metric.replace('_', ' ').title()}")
        ax.set_xticks(x)
        ax.set_xticklabels([metric.replace('_', ' ').title()])
        ax.legend()

        # Add value annotations
        self._autolabel(ax, rects1, pct_change, True)
        self._autolabel(ax, rects2, pct_change, False)

    def _autolabel(self, ax, rects, pct_change, is_baseline):
        """Attach a text label above each bar showing its value and percent change"""
        for i, rect in enumerate(rects):
            height = rect.get_height()
            if height > 0:  # Don't label zero values
                if is_baseline:
                    ax.text(rect.get_x() + rect.get_width()/2., height * 1.01,
                            f'{height:.2f}', ha='center', va='bottom', fontsize=9)
                else:
                    # For target, show percent change
                    if pct_change[i] > 0:
                        color = 'red'  # Higher is worse for latencies
                        sign = '+'
                    else:
                        color = 'green'  # Lower is better for latencies
                        sign = ''

                    # Adjust for throughput metrics where higher is better
                    if "throughput" in ax.get_ylabel().lower() or "queries" in ax.get_ylabel().lower():
                        if color == 'red':
                            color = 'green'
                        else:
                            color = 'red'

                    ax.text(rect.get_x() + rect.get_width()/2., height * 1.01,
                            f'{height:.2f}\n{sign}{pct_change[i]:.1f}%',
                            ha='center', va='bottom', fontsize=9,
                            color=color)

    def create_query_type_comparison(self, results_subset=None):
        """Create a comparison between different query types"""
        if not results_subset:
            results = self.latency_monitor_results
        else:
            results = results_subset

        if not results:
            print("No latency monitor results available for query type comparison")
            return

        # Group results by query type
        query_types = {}
        for result in results:
            query_type = result.get('query_type')
            if query_type and query_type != 'all':
                if query_type not in query_types:
                    query_types[query_type] = []
                query_types[query_type].append(result)

        if not query_types:
            print("No valid query types found in results")
            return

        # Create figure
        plt.figure(figsize=(14, 10))
        gs = gridspec.GridSpec(2, 1, height_ratios=[2, 1])

        # Latency comparison
        ax1 = plt.subplot(gs[0])

        width = 0.2
        x = np.arange(3)  # avg, p95, max
        metrics = ['avg', 'p95', 'max']

        for i, (query_type, query_results) in enumerate(query_types.items()):
            # Calculate average metrics across all results for this query type
            avg_metrics = {
                'avg': np.mean([r['latency_ms']['avg'] for r in query_results]),
                'p95': np.mean([r['latency_ms']['p95'] for r in query_results]),
                'max': np.mean([r['latency_ms']['max'] for r in query_results])
            }

            # Plot bars
            latencies = [avg_metrics[m] for m in metrics]
            ax1.bar(x + (i - len(query_types)/2 + 0.5) * width, latencies,
                   width, label=query_type)

        ax1.set_ylabel('Latency (ms)')
        ax1.set_title('Query Type Latency Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels(metrics)
        ax1.legend()

        # Throughput comparison
        ax2 = plt.subplot(gs[1])

        x = np.arange(len(query_types))

        query_type_names = list(query_types.keys())
        throughputs = [np.mean([r['throughput_qps'] for r in query_types[qt]]) for qt in query_type_names]

        ax2.bar(x, throughputs, 0.6)
        ax2.set_ylabel('Throughput (queries/sec)')
        ax2.set_title('Query Type Throughput Comparison')
        ax2.set_xticks(x)
        ax2.set_xticklabels(query_type_names)

        # Add value annotations
        for i, v in enumerate(throughputs):
            ax2.text(i, v * 1.01, f'{v:.2f}', ha='center', va='bottom')

        plt.tight_layout()

        # Save the plot
        filename = os.path.join(self.output_dir, "query_type_comparison.png")
        plt.savefig(filename)
        print(f"Saved query type comparison plot to {filename}")

        plt.close()

    def generate_basic_reports(self):
        """Generate a set of basic visualization reports"""
        print("Generating basic benchmark visualization reports...")

        # Generate Locust benchmark trends
        if self.locust_results:
            self.create_latency_trend_plot(
                self.locust_results,
                "Locust Read",
                "locust_read",
                "read_latency_ms"
            )

            self.create_latency_trend_plot(
                self.locust_results,
                "Locust Write",
                "locust_write",
                "write_latency_ms"
            )

            self.create_throughput_trend_plot(
                self.locust_results,
                "Locust",
                "locust"
            )

        # Generate latency monitor trends
        if self.latency_monitor_results:
            self.create_latency_trend_plot(
                self.latency_monitor_results,
                "Latency Monitor",
                "latency_monitor",
                "latency_ms"
            )

            self.create_throughput_trend_plot(
                self.latency_monitor_results,
                "Latency Monitor",
                "latency_monitor"
            )

            self.create_query_type_comparison()

        print(f"Basic reports generated in {self.output_dir}")

    def generate_html_report(self):
        """Generate an HTML report with all visualizations"""
        if not self.locust_results and not self.latency_monitor_results:
            print("No benchmark results available for HTML report")
            return

        # First generate all individual plots
        self.generate_basic_reports()

        # Create HTML content
        html = """<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PostgreSQL/Citus Benchmark Report</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    margin: 0;
                    padding: 20px;
                    color: #333;
                }
                h1, h2, h3 {
                    color: #0056b3;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                }
                .section {
                    margin-bottom: 30px;
                    border-bottom: 1px solid #eee;
                    padding-bottom: 20px;
                }
                .graph {
                    margin: 20px 0;
                    text-align: center;
                }
                .graph img {
                    max-width: 100%;
                    height: auto;
                    border: 1px solid #ddd;
                    border-radius: 4px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                th, td {
                    padding: 12px 15px;
                    border: 1px solid #ddd;
                    text-align: left;
                }
                th {
                    background-color: #f8f9fa;
                }
                tr:nth-child(even) {
                    background-color: #f2f2f2;
                }
                .timestamp {
                    color: #666;
                    font-size: 0.9em;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>PostgreSQL/Citus Benchmark Report</h1>
                <p class="timestamp">Generated on %s</p>
        """ % datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Add Locust benchmarks section if we have results
        if self.locust_results:
            html += """
                <div class="section">
                    <h2>Locust Benchmark Results</h2>
                    <p>Results from distributed load testing using Locust</p>

                    <h3>Latency Trends</h3>
                    <div class="graph">
                        <img src="locust_read_latency_trend.png" alt="Read Latency Trend">
                        <p>Read operation latency trends over time</p>
                    </div>

                    <div class="graph">
                        <img src="locust_write_latency_trend.png" alt="Write Latency Trend">
                        <p>Write operation latency trends over time</p>
                    </div>

                    <h3>Throughput Trends</h3>
                    <div class="graph">
                        <img src="locust_throughput_trend.png" alt="Throughput Trend">
                        <p>Operation throughput trends over time</p>
                    </div>

                    <h3>Summary of Recent Benchmarks</h3>
                    <table>
                        <tr>
                            <th>Date</th>
                            <th>Read Avg (ms)</th>
                            <th>Read p95 (ms)</th>
                            <th>Write Avg (ms)</th>
                            <th>Write p95 (ms)</th>
                            <th>Throughput (ops/s)</th>
                        </tr>
            """

            # Add the most recent 5 benchmark results
            for result in self.locust_results[-5:]:
                timestamp = result['timestamp_dt'].strftime('%Y-%m-%d %H:%M')
                read_avg = result['read_latency_ms']['avg']
                read_p95 = result['read_latency_ms']['p95']
                write_avg = result['write_latency_ms']['avg']
                write_p95 = result['write_latency_ms']['p95']
                throughput = result['throughput_ops_sec']['total']

                html += f"""
                        <tr>
                            <td>{timestamp}</td>
                            <td>{read_avg:.2f}</td>
                            <td>{read_p95:.2f}</td>
                            <td>{write_avg:.2f}</td>
                            <td>{write_p95:.2f}</td>
                            <td>{throughput:.2f}</td>
                        </tr>
                """

            html += """
                    </table>
                </div>
            """

        # Add latency monitor section if we have results
        if self.latency_monitor_results:
            html += """
                <div class="section">
                    <h2>Latency Monitor Results</h2>
                    <p>Results from real-time latency monitoring</p>

                    <h3>Latency Trends</h3>
                    <div class="graph">
                        <img src="latency_monitor_latency_trend.png" alt="Latency Trend">
                        <p>Query latency trends over time</p>
                    </div>

                    <h3>Throughput Trends</h3>
                    <div class="graph">
                        <img src="latency_monitor_throughput_trend.png" alt="Throughput Trend">
                        <p>Query throughput trends over time</p>
                    </div>

                    <h3>Query Type Comparison</h3>
                    <div class="graph">
                        <img src="query_type_comparison.png" alt="Query Type Comparison">
                        <p>Comparison of different query types</p>
                    </div>

                    <h3>Summary of Recent Benchmarks</h3>
                    <table>
                        <tr>
                            <th>Date</th>
                            <th>Query Type</th>
                            <th>Avg Latency (ms)</th>
                            <th>p95 Latency (ms)</th>
                            <th>Throughput (qps)</th>
                            <th>Client Count</th>
                        </tr>
            """

            # Add the most recent 5 benchmark results
            for result in self.latency_monitor_results[-5:]:
                timestamp = result['timestamp_dt'].strftime('%Y-%m-%d %H:%M')
                query_type = result['query_type']
                avg_latency = result['latency_ms']['avg']
                p95_latency = result['latency_ms']['p95']
                throughput = result['throughput_qps']
                client_count = result['client_count']

                html += f"""
                        <tr>
                            <td>{timestamp}</td>
                            <td>{query_type}</td>
                            <td>{avg_latency:.2f}</td>
                            <td>{p95_latency:.2f}</td>
                            <td>{throughput:.2f}</td>
                            <td>{client_count}</td>
                        </tr>
                """

            html += """
                    </table>
                </div>
            """

        # Close HTML
        html += """
            </div>
        </body>
        </html>
        """

        # Save HTML report
        report_path = os.path.join(self.output_dir, "benchmark_report.html")
        with open(report_path, 'w') as f:
            f.write(html)

        print(f"HTML report generated at {report_path}")


def main():
    """Command line entry point"""
    parser = argparse.ArgumentParser(description="PostgreSQL/Citus Benchmark Visualizer")
    parser.add_argument("--data-dir", default="benchmark_results",
                      help="Directory containing benchmark result JSON files (default: benchmark_results)")
    parser.add_argument("--output-dir", default="benchmark_results/graphs",
                      help="Directory where visualization output will be saved (default: benchmark_results/graphs)")
    parser.add_argument("--compare", nargs=2, metavar=("BASELINE", "TARGET"),
                      help="Compare two specific benchmark result files")
    parser.add_argument("--html", action="store_true",
                      help="Generate an HTML report with all visualizations")

    args = parser.parse_args()

    # Create the visualizer
    visualizer = BenchmarkVisualizer(args.data_dir, args.output_dir)

    # Generate basic reports if no specific action is requested
    if not args.compare and not args.html:
        visualizer.generate_basic_reports()

    # Generate comparison if requested
    if args.compare:
        # Check if the files exist
        for file_path in args.compare:
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                return

        visualizer.create_comparison_plot(args.compare[0], args.compare[1])

    # Generate HTML report if requested
    if args.html:
        visualizer.generate_html_report()


if __name__ == "__main__":
    main()