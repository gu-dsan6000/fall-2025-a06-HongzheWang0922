#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis

Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all Spark log files.

Outputs:
1. data/output/problem1_counts.csv - Log level counts
2. data/output/problem1_sample.csv - 10 random sample log entries
3. data/output/problem1_summary.txt - Summary statistics
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, lit
import time


def create_spark_session(master_url=None):
    """Create Spark session for cluster or local execution."""
    
    builder = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    if master_url:
        # Cluster mode
        builder = builder.master(master_url)
        # S3 configuration for cluster
        builder = (builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        )
    else:
        # Local mode
        builder = builder.master("local[*]")
    
    return builder.getOrCreate()


def analyze_log_levels(spark, data_path):
    """
    Analyze log level distribution in Spark logs.
    
    Args:
        spark: SparkSession
        data_path: Path to log files (local or S3)
    
    Returns:
        tuple: (counts_df, sample_df, stats_dict)
    """
    
    print("\n" + "="*60)
    print("PROBLEM 1: LOG LEVEL DISTRIBUTION ANALYSIS")
    print("="*60)
    
    start_time = time.time()
    
    # Step 1: Read all log files
    print("\nStep 1: Reading log files...")
    logs_df = spark.read.text(data_path)
    total_lines = logs_df.count()
    print(f"✓ Loaded {total_lines:,} total log lines")
    
    # Step 2: Extract log levels using regex
    print("\nStep 2: Extracting log levels...")
    # Pattern to match log levels: INFO, WARN, ERROR, DEBUG
    logs_parsed = logs_df.select(
        col("value").alias("log_entry"),
        regexp_extract(col("value"), r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias("log_level")
    )
    
    # Filter out lines without log levels
    logs_with_levels = logs_parsed.filter(col("log_level") != "")
    lines_with_levels = logs_with_levels.count()
    print(f"✓ Found {lines_with_levels:,} lines with log levels")
    
    # Step 3: Count log levels
    print("\nStep 3: Counting log levels...")
    counts_df = (logs_with_levels
        .groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    
    print("\nLog level counts:")
    counts_df.show()
    
    # Step 4: Get random sample
    print("\nStep 4: Generating random sample...")
    sample_df = logs_with_levels.sample(False, 0.01).limit(10)
    
    # Step 5: Calculate statistics
    print("\nStep 5: Calculating statistics...")
    unique_levels = counts_df.count()
    
    # Collect counts for summary
    counts_list = counts_df.collect()
    
    stats = {
        'total_lines': total_lines,
        'lines_with_levels': lines_with_levels,
        'unique_levels': unique_levels,
        'counts': {row['log_level']: row['count'] for row in counts_list}
    }
    
    execution_time = time.time() - start_time
    print(f"\n✓ Analysis completed in {execution_time:.2f} seconds")
    
    return counts_df, sample_df, stats


def save_outputs(counts_df, sample_df, stats, output_dir="data/output"):
    """Save analysis outputs to CSV and text files."""
    
    print("\n" + "="*60)
    print("SAVING OUTPUTS")
    print("="*60)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Save counts to CSV
    counts_file = f"{output_dir}/problem1_counts.csv"
    print(f"\n1. Saving counts to {counts_file}...")
    counts_df.toPandas().to_csv(counts_file, index=False)
    print(f"✓ Saved {counts_file}")
    
    # 2. Save sample to CSV
    sample_file = f"{output_dir}/problem1_sample.csv"
    print(f"\n2. Saving sample to {sample_file}...")
    sample_df.toPandas().to_csv(sample_file, index=False)
    print(f"✓ Saved {sample_file}")
    
    # 3. Save summary to text file
    summary_file = f"{output_dir}/problem1_summary.txt"
    print(f"\n3. Saving summary to {summary_file}...")
    
    with open(summary_file, 'w') as f:
        f.write("="*60 + "\n")
        f.write("PROBLEM 1: LOG LEVEL DISTRIBUTION SUMMARY\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Total log lines processed: {stats['total_lines']:,}\n")
        f.write(f"Total lines with log levels: {stats['lines_with_levels']:,}\n")
        f.write(f"Unique log levels found: {stats['unique_levels']}\n\n")
        
        f.write("Log level distribution:\n")
        f.write("-"*60 + "\n")
        
        total = stats['lines_with_levels']
        for level, count in sorted(stats['counts'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total) * 100
            f.write(f"  {level:8s}: {count:12,} ({percentage:6.2f}%)\n")
        
        f.write("\n" + "="*60 + "\n")
    
    print(f"✓ Saved {summary_file}")
    
    print("\n" + "="*60)
    print("✓ ALL OUTPUTS SAVED SUCCESSFULLY")
    print("="*60)


def main():
    """Main function."""
    
    print("\n" + "="*70)
    print("SPARK LOG ANALYSIS - PROBLEM 1")
    print("="*70)
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
        mode = "CLUSTER"
        # Use S3 path for cluster
        bucket = os.getenv("SPARK_LOGS_BUCKET", "s3://hw530-assignment-spark-cluster-logs")
        data_path = f"{bucket}/data/application_*/container_*/*.log"
    else:
        master_url = None
        mode = "LOCAL"
        # Use local path for testing
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
        counts_df, sample_df, stats = analyze_log_levels(spark, data_path)
        
        # Save outputs
        save_outputs(counts_df, sample_df, stats)
        
        print("\n" + "="*70)
        print("✓ PROBLEM 1 COMPLETED SUCCESSFULLY!")
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