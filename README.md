# Retail Data Platform on AWS

Submission for the Senior Cloud Data Engineer (AWS Data Platforms) technical assessment.
An S3 data lake (raw / processed / curated), a PySpark ETL for AWS Glue, a Spark Structured
Streaming pipeline for Kinesis/Kafka, a star schema with advanced SQL, Terraform modules and a
Jenkins CI/CD pipeline.

![Architecture](architecture/architecture.png)

## Repository structure

| Path | What it is | Assessment section |
| --- | --- | --- |
| `architecture/` | Architecture, star-schema and CI/CD diagrams + `ARCHITECTURE.md` (layers, failure points, scaling, security boundaries) | A, D, G |
| `pyspark/transactions_etl.py` | Glue/PySpark ETL: DQ rules, quarantine, dedup, date normalisation, aggregations, incremental watermark | B, C |
| `pyspark/streaming/` | Event producer + Spark Structured Streaming job (watermark, dedup, windowed aggregates, DLQ, checkpoints) | E |
| `sql/01_star_schema_ddl.sql` | Fact_Transactions, Dim_Customer (SCD2), Dim_Date, Dim_Region with PK/FK | D |
| `sql/02_load_star_schema.sql` | Loads the star schema from the processed layer | D |
| `sql/03_advanced_queries.sql` | The five analytical queries | H |
| `terraform/modules/` | Reusable modules, each with `main.tf`, `variables.tf`, `outputs.tf`: `s3_data_lake`, `iam_glue_role`, `glue_job`, `monitoring` | B, F, I |
| `terraform/live/` | Root module + `envs/<env>.tfvars` + `backend/<env>.hcl` | F |
| `cicd/Jenkinsfile` | Validate → plan → apply dev → smoke test → plan prod → approval → apply prod, with rollback by tag | G |
| `samples/` | Sample-data generator + `transactions_sample.csv` (one day of raw data) | C |
| `tests/` | SQL/star-schema tests (DuckDB) and streaming guarantees test | C, D, E, H |

## Setup

Requires Python 3.10–3.12, Java 17, Terraform ≥ 1.10, AWS CLI v2.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Execution steps

### Locally (no AWS account needed)

```bash
python samples/generate_sample_data.py --days 120 --rows-per-day 300 --end-date 2026-09-30

python pyspark/transactions_etl.py --local \
  --source_path samples/data/raw/transactions \
  --processed_path samples/data/processed/transactions \
  --curated_path samples/data/curated \
  --quarantine_path samples/data/quarantine/transactions \
  --state_path samples/data/_state/transactions_etl

python tests/run_sql_tests.py                 # builds the star schema, runs H1..H5
bash tests/test_streaming_late_and_dupes.sh   # streaming: dedup, late events, DLQ, restart
ruff check .
```

Expected: 37,440 rows read, ~906 quarantined, ~1,400 duplicates removed, 35,134 unique
transactions; `All SQL assertions passed.`; streaming test prints `PASS`.

### On AWS (dev)

```bash
cd terraform/live
# set your state bucket in backend/dev.hcl first
terraform init -backend-config=backend/dev.hcl
terraform plan  -var-file=envs/dev.tfvars -out=dev.tfplan
terraform apply dev.tfplan

aws s3 sync ../../samples/data/raw/ s3://$(terraform output -raw data_lake_bucket)/raw/
aws glue start-job-run --job-name $(terraform output -raw glue_job_name)

terraform destroy -var-file=envs/dev.tfvars   # when done
```

## Assumptions

- Daily batch files arrive as CSV, one folder per ingest date; `transaction_id` is unique per real transaction.
- The latest ingested version of a transaction is the correct one (corrections are re-sent, not deleted).
- Only `COMPLETED` transactions count as revenue; refunds and pending sales are excluded from aggregates.
- Amounts are in a single currency (INR); multi-currency would add an FX dimension.
- One product per transaction, so the fact grain is the transaction (a basket model would need a line-item fact).
- Events more than 2 hours late are reconciled by the nightly batch rather than the real-time aggregates.
- dev and prod live in separate AWS accounts; the sample run uses only dev.

## Trade-offs

| Decision | Chosen | Alternative | Why |
| --- | --- | --- | --- |
| Batch engine | AWS Glue | Amazon EMR | Serverless, per-second billing, no cluster ops for a daily job; EMR when jobs run 24×7 or need deep tuning |
| Incremental logic | Explicit watermark + partition merge | Glue Job Bookmarks / Apache Iceberg `MERGE` | Transparent and testable; Iceberg is the next step at higher volume |
| Bad data | Quarantine + 10% circuit breaker | Drop silently / fail on first bad row | Keeps good data flowing while making every rejected row traceable |
| File format | Parquet + Snappy | CSV / ORC / Avro | Columnar, compressed, schema-carrying; best fit for Athena and Spark |
| Partitioning | Raw by `ingest_date`, processed by `year/month/day` | Partition by region or customer | Arrival time for incremental pickup, business date for query pruning; avoids tiny files |
| Streaming | Kinesis + Spark Structured Streaming | MSK + Kafka Streams / Flink | Fully managed, IAM-native; code also supports MSK |
| Exactly-once | Dedup in watermark + idempotent file sink | Kafka transactions | Works across Kinesis and Kafka; simpler to operate |
| Warehouse | Redshift star schema | Athena-only | Fast BI joins and concurrency; Athena stays for ad-hoc queries |

## Key design decisions

- **Business key `transaction_id`**, latest `ingest_ts` wins — removes duplicates and applies corrections.
- **Bad rows are quarantined, not dropped**, with the rules they broke.
- **Idempotent reruns**: watermark advances only on success + dynamic partition overwrite.
- **Least privilege**: the Glue role reads `raw/`, writes only its output prefixes, uses one KMS key.
- **Plan/apply separation**: CI applies only the saved, reviewed plan; prod needs a named approver.

## External references and reusable components

Declare here everything you did not write from scratch (the assessment's candidate
declaration asks for this), for example: starter templates, AWS documentation examples,
AI-assisted code, and which parts you modified.
