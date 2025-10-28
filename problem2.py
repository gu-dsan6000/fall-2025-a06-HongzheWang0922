#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis

Analyze cluster usage patterns to understand which clusters are most heavily used.
Extract cluster IDs, application IDs, and application start/end times.

Outputs:
1. data/output/problem2_timeline.csv - Timeline data
2. data/output/problem2_cluster_summary.csv - Cluster summary
3. data/output/problem2_stats.txt - Statistics
4. data/output/problem2_bar_chart.png - Applications per cluster bar chart
5. data/output/problem2_density_plot.png - Job duration density plot
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, min as spark_min, max as spark_max,
    input_file_name, to_timestamp, unix_timestamp, lit
)
import time
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def create_spark_session(master_url=None):
    """Create Spark session for cluster or local execution."""
    
    builder = (
        SparkSession.builder
        .appName("Problem2_ClusterUsageAnalysis")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    if master_url:
        # Cluster mode
        builder = builder.master(master_url)
        builder = (builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        )
    else:
        # Local mode
        builder = builder.master("local[*]")
    
    return builder.getOrCreate()


def analyze_cluster_usage(spark, data_path):
    """
    Analyze cluster usage patterns.
    
    Args:
        spark: SparkSession
        data_path: Path to log files
    
    Returns:
        tuple: (timeline_df, cluster_summary_df, stats_dict)
    """
    
    print("\n" + "="*60)
    print("PROBLEM 2: CLUSTER USAGE ANALYSIS")
    print("="*60)
    print("\n⚠️  NOTE: This analysis takes 10-20 minutes on the cluster")
    
    start_time = time.time()
    
    # Step 1: Read all log files
    print("\nStep 1: Reading log files...")
    logs_df = spark.read.text(data_path)
    
    # Add file path information
    logs_df = logs_df.withColumn("file_path", input_file_name())
    total_lines = logs_df.count()
    print(f"✓ Loaded {total_lines:,} log lines from files")
    
    # Step 2: Extract application and cluster IDs from file paths
    print("\nStep 2: Extracting application and cluster IDs...")
    
    # Extract application_ID from path: application_TIMESTAMP_JOBID
    logs_df = logs_df.withColumn(
        "application_id",
        regexp_extract("file_path", r"application_(\d+_\d+)", 0)
    )
    
    # Extract cluster timestamp (first part of application ID)
    logs_df = logs_df.withColumn(
        "cluster_id",
        regexp_extract("application_id", r"application_(\d+)_", 1)
    )
    
    # Filter out rows without valid IDs
    apps_df = logs_df.filter(
        (col("application_id") != "") & (col("cluster_id") != "")
    ).select("application_id", "cluster_id", "value").cache()
    
    print(f"✓ Extracted application and cluster IDs")
    
    # Step 3: Extract timestamps from log entries
    print("\nStep 3: Extracting timestamps from log entries...")
    
    # Pattern: YY/MM/DD HH:MM:SS
    apps_with_time = apps_df.withColumn(
        "timestamp_str",
        regexp_extract("value", r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", 1)
    ).filter(col("timestamp_str") != "")
    
    # Convert to timestamp
    apps_with_time = apps_with_time.withColumn(
        "timestamp",
        to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss")
    )
    
    print(f"✓ Extracted timestamps")
    
    # Step 4: Calculate application start and end times
    print("\nStep 4: Calculating application timelines...")
    
    timeline_df = (apps_with_time
        .groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
    )
    
    # Calculate duration in seconds
    timeline_df = timeline_df.withColumn(
        "duration_seconds",
        unix_timestamp("end_time") - unix_timestamp("start_time")
    )
    
    # Sort by start time
    timeline_df = timeline_df.orderBy("start_time")
    
    timeline_count = timeline_df.count()
    print(f"✓ Calculated timelines for {timeline_count} applications")
    
    print("\nSample timeline data:")
    timeline_df.show(10, truncate=False)
    
    # Step 5: Aggregate by cluster
    print("\nStep 5: Aggregating cluster statistics...")
    
    cluster_summary_df = (timeline_df
        .groupBy("cluster_id")
        .agg(
            count("application_id").alias("num_applications"),
            spark_min("start_time").alias("first_app_start"),
            spark_max("end_time").alias("last_app_end"),
            spark_min("duration_seconds").alias("min_duration"),
            spark_max("duration_seconds").alias("max_duration")
        )
        .orderBy(col("num_applications").desc())
    )
    
    print("\nCluster summary:")
    cluster_summary_df.show(20, truncate=False)
    
    # Step 6: Calculate statistics
    print("\nStep 6: Calculating statistics...")
    
    unique_clusters = cluster_summary_df.count()
    total_apps = timeline_df.count()
    
    # Get top cluster info
    top_cluster = cluster_summary_df.first()
    
    stats = {
        'unique_clusters': unique_clusters,
        'total_applications': total_apps,
        'top_cluster_id': top_cluster['cluster_id'],
        'top_cluster_apps': top_cluster['num_applications']
    }
    
    execution_time = time.time() - start_time
    print(f"\n✓ Analysis completed in {execution_time:.2f} seconds ({execution_time/60:.1f} minutes)")
    
    apps_df.unpersist()
    
    return timeline_df, cluster_summary_df, stats


def create_visualizations(cluster_summary_pd, timeline_pd, output_dir="data/output"):
    """Create visualizations using Seaborn."""
    
    print("\n" + "="*60)
    print("CREATING VISUALIZATIONS")
    print("="*60)
    
    # Set style
    sns.set_style("whitegrid")
    sns.set_palette("husl")
    
    # 1. Bar chart: Applications per cluster
    print("\n1. Creating bar chart (applications per cluster)...")
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Sort by number of applications and take top 20
    top_clusters = cluster_summary_pd.nlargest(20, 'num_applications')
    
    sns.barplot(
        data=top_clusters,
        x='cluster_id',
        y='num_applications',
        ax=ax
    )
    
    ax.set_xlabel('Cluster ID', fontsize=12)
    ax.set_ylabel('Number of Applications', fontsize=12)
    ax.set_title('Top 20 Clusters by Number of Applications', fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    bar_chart_file = f"{output_dir}/problem2_bar_chart.png"
    plt.savefig(bar_chart_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Saved {bar_chart_file}")
    
    # 2. Density plot: Job duration distribution for largest cluster
    print("\n2. Creating density plot (job duration distribution)...")
    
    # Get the largest cluster
    largest_cluster_id = cluster_summary_pd.loc[
        cluster_summary_pd['num_applications'].idxmax(), 'cluster_id'
    ]
    
    # Filter timeline data for largest cluster
    largest_cluster_data = timeline_pd[
        timeline_pd['cluster_id'] == largest_cluster_id
    ]
    
    # Convert duration to minutes for better readability
    largest_cluster_data['duration_minutes'] = largest_cluster_data['duration_seconds'] / 60
    
    # Filter out extreme outliers for better visualization (optional)
    # Keep only durations up to 99th percentile
    duration_99th = largest_cluster_data['duration_minutes'].quantile(0.99)
    plot_data = largest_cluster_data[
        largest_cluster_data['duration_minutes'] <= duration_99th
    ]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Use log scale if data is highly skewed
    if plot_data['duration_minutes'].max() / plot_data['duration_minutes'].min() > 100:
        use_log = True
        ax.set_xscale('log')
    else:
        use_log = False
    
    sns.kdeplot(
        data=plot_data,
        x='duration_minutes',
        fill=True,
        ax=ax,
        linewidth=2
    )
    
    ax.set_xlabel('Job Duration (minutes)' + (' - Log Scale' if use_log else ''), fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title(
        f'Job Duration Distribution for Cluster {largest_cluster_id}\n'
        f'({len(largest_cluster_data)} applications)',
        fontsize=14,
        fontweight='bold'
    )
    
    # Add median line
    median_duration = plot_data['duration_minutes'].median()
    ax.axvline(median_duration, color='red', linestyle='--', linewidth=2, label=f'Median: {median_duration:.1f} min')
    ax.legend()
    
    plt.tight_layout()
    density_plot_file = f"{output_dir}/problem2_density_plot.png"
    plt.savefig(density_plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Saved {density_plot_file}")
    
    print("\n✓ Visualizations created successfully")


def save_outputs(timeline_df, cluster_summary_df, stats, output_dir="data/output"):
    """Save analysis outputs."""
    
    print("\n" + "="*60)
    print("SAVING OUTPUTS")
    print("="*60)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to Pandas
    timeline_pd = timeline_df.toPandas()
    cluster_summary_pd = cluster_summary_df.toPandas()
    
    # 1. Save timeline CSV
    timeline_file = f"{output_dir}/problem2_timeline.csv"
    print(f"\n1. Saving timeline to {timeline_file}...")
    timeline_pd.to_csv(timeline_file, index=False)
    print(f"✓ Saved {timeline_file}")
    
    # 2. Save cluster summary CSV
    summary_file = f"{output_dir}/problem2_cluster_summary.csv"
    print(f"\n2. Saving cluster summary to {summary_file}...")
    cluster_summary_pd.to_csv(summary_file, index=False)
    print(f"✓ Saved {summary_file}")
    
    # 3. Create visualizations
    create_visualizations(cluster_summary_pd, timeline_pd, output_dir)
    
    # 4. Save statistics text file
    stats_file = f"{output_dir}/problem2_stats.txt"
    print(f"\n3. Saving statistics to {stats_file}...")
    
    with open(stats_file, 'w') as f:
        f.write("="*60 + "\n")
        f.write("PROBLEM 2: CLUSTER USAGE ANALYSIS SUMMARY\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Total unique clusters: {stats['unique_clusters']}\n")
        f.write(f"Total applications analyzed: {stats['total_applications']}\n")
        f.write(f"Most active cluster: {stats['top_cluster_id']}\n")
        f.write(f"Applications on most active cluster: {stats['top_cluster_apps']}\n\n")
        
        f.write("Top 10 Clusters by Application Count:\n")
        f.write("-"*60 + "\n")
        
        top_10 = cluster_summary_pd.nlargest(10, 'num_applications')
        for idx, row in top_10.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")
        
        f.write("\n" + "="*60 + "\n")
    
    print(f"✓ Saved {stats_file}")
    
    print("\n" + "="*60)
    print("✓ ALL OUTPUTS SAVED SUCCESSFULLY")
    print("="*60)


def main():
    """Main function."""
    
    print("\n" + "="*70)
    print("SPARK LOG ANALYSIS - PROBLEM 2")
    print("="*70)
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
        mode = "CLUSTER"
        bucket = os.getenv("SPARK_LOGS_BUCKET", "s3://hw530-assignment-spark-cluster-logs")
        data_path = f"{bucket}/data/application_*/container_*/*.log"
    else:
        master_url = None
        mode = "LOCAL"
        data_path = "data/sample/application_*/container_*/*.log"
    
    print(f"\nExecution Mode: {mode}")
    print(f"Data Path: {data_path}")
    if master_url:
        print(f"Master URL: {master_url}")
    
    # Create Spark session
    print("\nCreating Spark session...")
    spark = create_spark_session(master_url)
    print("✓ Spark session created")
    
    try:
        # Run analysis
        timeline_df, cluster_summary_df, stats = analyze_cluster_usage(spark, data_path)
        
        # Save outputs
        save_outputs(timeline_df, cluster_summary_df, stats)
        
        print("\n" + "="*70)
        print("✓ PROBLEM 2 COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())