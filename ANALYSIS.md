# Spark Log Analysis Report

**Name:** Hongzhe Wang  
**NetID:** hw530  
**Date:** October 28, 2025

---

## Overview

This project analyzed approximately 2.8 GB of Spark cluster logs from 194 applications running between 2015-2017. I set up a 4-node Spark cluster on AWS and used PySpark to process over 33 million log lines. The analysis focused on understanding log level distributions and cluster usage patterns.

**Main Findings:**
- Processed 33.2 million log lines, with 27.4 million containing identifiable log levels
- Found 6 unique clusters with one dominant cluster handling 93% of all workload
- Log entries are overwhelmingly INFO level (99.92%), with very few errors or warnings

---

## Problem 1: Log Level Distribution

### Approach

I read all log files from S3 using PySpark's recursive file lookup, then used regex to extract log levels (INFO, WARN, ERROR, DEBUG) from each line. The pattern `r'\b(INFO|WARN|ERROR|DEBUG)\b'` matched the log level keywords in the standard Spark log format.

The main challenge was getting the S3 path configuration right. Initially I tried wildcards like `s3a://bucket/data/*/*/*/*.log` which didn't work. The solution was to use `recursiveFileLookup=true` with just the base directory path.

### Results

Out of 33.2 million total log lines, 27.4 million (82.5%) contained structured log levels:

| Log Level | Count       | Percentage |
|-----------|-------------|------------|
| INFO      | 27,389,482  | 99.92%     |
| ERROR     | 11,259      | 0.04%      |
| WARN      | 9,595       | 0.04%      |

### Analysis

The overwhelming dominance of INFO messages (99.92%) is typical for production Spark jobs with detailed logging enabled. The low error rate (0.04%) suggests the applications ran reliably with minimal failures. Interestingly, no DEBUG level logs were found, which makes sense for production environments where verbose logging is disabled for performance.

About 17.5% of log lines didn't match the structured format - these were likely stack traces, data dumps, or custom application output.

**Execution:** Problem 1 took about 6 minutes to run on the cluster, processing at roughly 92,000 lines per second.

---

## Problem 2: Cluster Usage Analysis

### Approach

This was more complex than Problem 1. I had to:
1. Extract application and cluster IDs from file paths using regex
2. Parse timestamps from log entries (format: `YY/MM/DD HH:MM:SS`)
3. Calculate start/end times for each application
4. Aggregate by cluster to see usage patterns
5. Generate visualizations with Seaborn

The timestamp parsing was tricky because some entries had different formats. I filtered out entries without valid timestamps rather than failing the entire job.

### Results

Found 6 clusters with very uneven distribution:

| Cluster ID      | Applications | Duration Range    |
|-----------------|--------------|-------------------|
| 1485248649253   | 181 (93%)    | 11s to 15.5 hours |
| 1472621869829   | 8            | 10s to 33s        |
| 1448006111297   | 2            | 42 min            |
| Others          | 3 total      | Various           |

### Analysis

The most striking finding is that one cluster (1485248649253) handled 181 out of 194 applications (93.3%). This was clearly the main production cluster, active for about 6 months from January to July 2017. The other clusters were likely used for testing or one-off analyses.

The duration range on the main cluster was huge - from 11 seconds to over 15 hours. This suggests a mix of workload types: quick ad-hoc queries and long-running batch jobs. The density plot showed most jobs were relatively short (under an hour), with a long tail of occasional multi-hour jobs.

From an operations perspective, this concentration on a single cluster could be concerning. If that cluster went down, 93% of the workload would be impacted. It also might indicate resource contention during peak usage.

**Execution:** Problem 2 took about 8 minutes, slower than Problem 1 due to the multiple regex extractions and aggregations.

---

## Technical Setup

### Cluster Configuration

- **Hardware:** 4x t3.large instances (1 master + 3 workers)
- **Resources:** 8 vCPU total, 32 GB RAM, 400 GB storage
- **Spark:** Version 4.0.1 with 4GB executor memory, 2 cores per executor
- **Data:** 2.8 GB on S3 (s3a://hw530-assignment-spark-cluster-logs)

I used the provided automation script to set up the cluster, which took about 10-15 minutes. The script handled all the configuration including security groups, SSH keys, and Spark installation.

### Key Implementation Details

**S3 Configuration:**
```python
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.aws.credentials.provider", 
       "com.amazonaws.auth.InstanceProfileCredentialsProvider")
```

**Recursive File Reading:**
```python
logs_df = spark.read.option("recursiveFileLookup", "true").text(data_path)
```

Both scripts follow the same structure: create Spark session, read data, process, save outputs. I kept the code modular with separate functions for session creation, analysis, and saving results.

---

## Challenges Faced

### 1. S3 Path Wildcards
**Problem:** Wildcard patterns in S3 paths kept failing with "Path does not exist" errors.  
**Solution:** Used `recursiveFileLookup=true` with directory-level paths instead of file-level wildcards.

### 2. Disk Space
**Problem:** My control EC2 (8GB disk) ran out of space during setup.  
**Solution:** Moved all work to the Master node which has 100GB, and used S3 as primary data source.

### 3. IAM Permissions
**Problem:** Workers couldn't access S3 initially.  
**Solution:** Verified the EMR_EC2_DefaultRole was attached and configured InstanceProfileCredentialsProvider.

### 4. Dependency Installation
**Problem:** PySpark wasn't installed on Master node.  
**Solution:** Used `uv` package manager which made installation fast and easy.

---

## Performance

### Execution Times

| Problem | Time    | Throughput         |
|---------|---------|-------------------|
| 1       | 6 min   | ~92K lines/sec    |
| 2       | 8 min   | ~69K lines/sec    |

Problem 2 was slower due to multiple regex operations and timestamp conversions. Both ran smoothly with no failures or stragglers.

### Cluster vs Local

Testing on sample data (170K lines) locally took about 15-20 seconds. Running the full dataset (33M lines) on the cluster took 6-8 minutes. That's roughly a 40-50x speedup, which makes sense with 6 cores across 3 workers vs 1 core locally.

### Cost

Total AWS cost for this assignment: approximately **$0.10**
- 4x t3.large @ $0.33/hour
- ~20 minutes total runtime (including setup)

Pretty cost-effective for processing 33 million records!

---

## What I Learned

**Technical Skills:**
- Setting up and managing a Spark cluster on AWS
- Working with S3 and handling IAM permissions
- Writing efficient PySpark code with proper optimizations (caching, adaptive execution)
- Debugging distributed jobs using Spark Web UI

**Practical Insights:**
- Always test with sample data first - saved me hours of debugging
- S3 and HDFS have different semantics, especially around path handling
- The Spark Web UI is incredibly useful for understanding what's actually happening
- Proper error handling and logging makes debugging much easier

**Surprises:**
- How imbalanced the cluster usage was (93% on one cluster)
- How fast Spark can process millions of records
- How cheap cloud computing is when you clean up properly

---

## Future Improvements

If I had more time, I'd explore:
1. **Error pattern analysis** - dig into what those 11,259 ERROR messages were about
2. **Time-based analysis** - look at usage patterns by hour/day/week
3. **Performance optimization** - some of those 15-hour jobs could probably be optimized
4. **User analysis** - extract user info to see who's using the cluster most

---

## Conclusion

This project successfully demonstrated processing large-scale log data using Spark. I processed 33.2 million log entries and gained insights into both log patterns and cluster usage.

The key finding about cluster concentration (93% on one cluster) would be valuable for capacity planning and reliability improvements. The low error rate (0.04%) confirms the systems were generally healthy.

From a learning perspective, this hands-on experience with Spark, AWS, and distributed computing was invaluable. Understanding how to set up, configure, and optimize a Spark cluster is a practical skill that extends beyond just this assignment.

**Total Time Spent:** 
- Cluster setup: 15 minutes (mostly automated)
- Development and testing: 2 hours
- Running on cluster: 15 minutes
- Analysis and report: 1 hour

**Deliverables:**
- 2 Python scripts (problem1.py, problem2.py)
- 8 output files (3 for Problem 1, 5 for Problem 2)
- This analysis report

---

## Appendix: Code Snippets

### Log Level Extraction
```python
logs_parsed = logs_df.select(
    col("value").alias("log_entry"),
    regexp_extract(col("value"), r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias("log_level")
).filter(col("log_level") != "")
```

### Application ID Extraction
```python
logs_df = logs_df.withColumn(
    "application_id",
    regexp_extract("file_path", r"application_(\d+_\d+)", 0)
).withColumn(
    "cluster_id",
    regexp_extract("application_id", r"application_(\d+)_", 1)
)
```

### Duration Calculation
```python
timeline_df = timeline_df.withColumn(
    "duration_seconds",
    unix_timestamp("end_time") - unix_timestamp("start_time")
)
```

---

**End of Report**
