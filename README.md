# Description


# Overall Flow Diagram
[Rosbag Files]
      │
      ▼
[Data Extraction Module]
      │
      ▼
[Raw Data for Topics]
      │
      ▼
[Resampling & Synchronization Module]
      │
      ▼
[Synchronized DataFrame @ 100Hz]
      │
      ▼
[Data Validation Module] → [Validation Report]
      │ (if valid)
      ▼
[Data Packaging Module]
      │
      ▼
[Packaged Data in Delta Lake/Parquet]
      │
      ├─────────────────────────────────────┐
      ▼                                     ▼
[Terminal-based Analytics App]  [Upload to Azure/Datalake]
      │
      ▼
[Performance Indicators & Telemetry Graphs]
