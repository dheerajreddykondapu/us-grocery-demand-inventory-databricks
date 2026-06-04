# Architecture

```text
Manual Trigger
   ↓
AWS Lambda
   ↓
Amazon S3 Raw Zone
   ↓
Databricks Job / Workflow
   ↓
Bronze Delta Tables
   ↓
Silver Cleaned + AI-Enriched Tables
   ↓
Gold Business KPI Tables
   ↓
Databricks SQL Dashboard
```

## AWS Layer

AWS Lambda extracts the Instacart dataset from Kaggle and writes raw CSV files to Amazon S3 using ingestion-date partitioning.

## Databricks Layer

Databricks reads raw files from S3, creates Bronze Delta tables, builds cleaned Silver tables, enriches product metadata with Databricks AI Functions and fallback rules, and creates Gold KPI tables.

## Dashboard Layer

Databricks SQL reads Gold tables for business reporting and visualization.
