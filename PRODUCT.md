# Product

<!-- impeccable:product-schema 1 -->

> Inferred from the repository and the redesign brief of 2026-11-14 (the user asked not to be questioned on details that can be judged from context). Facts marked *(inferred)* were not confirmed in an interview.

## Platform

web

## Users

- **Primary:** the course lecturer and examiners of IT4931 (Big Data Storage & Processing, SoICT – HUST) reviewing Group 3's project, usually on a laptop or a projected screen during a defense. *(inferred)*
- **Secondary:** the four group members, who run the pipeline locally or in Docker and use the dashboard to present and verify results.

## Product Purpose

A course project that implements an e-commerce data pipeline on a Lambda Architecture: HDFS + Parquet/Snappy storage tiers, Spark batch analytics (revenue, Top 10 products, order status, RFM K-Means), Spark Structured Streaming over Kafka clickstream (1-minute window sliding every 10 seconds, 10-minute watermark), and a SQL benchmark comparing raw CSV, unpartitioned Parquet and partitioned Parquet. The dashboard is the serving-layer view of all results. Success: an examiner can understand the architecture and verify each experimental result within minutes.

## Positioning

It shows one pipeline's own measured outputs end to end, from storage-tier sizes to benchmark execution plans, rather than a generic analytics demo.

## Operating Context

- Runs as a Streamlit app (`src/dashboard/app.py`) reading the serving layer through `ServingDatabaseClient` (PostgreSQL in Docker, SQLite fallback `data/serving_layer.db` locally).
- Data is produced by `scripts/run_local_demo.py` or the Docker jobs; the dashboard also has one action that generates a fresh clickstream batch for the speed layer.
- Interface language is Vietnamese with standard English technical terms.

## Capabilities and Constraints

- Four areas: system overview, batch analytics, real-time streaming, performance benchmark.
- Business logic, algorithms and database access must not change for presentation purposes.
- Stack: Python 3.11, Streamlit 1.35, Plotly 5.22; no new dependencies without need.
- Some metrics are estimates in the backend (benchmark bytes read, row-group scan volume); the UI must not present them as more precise than they are.

## Brand Commitments

- Project identity: "IT4931 · Nhóm 3", course IT4931, supervisor TS. Vũ Tuyết Trinh, members Phạm Văn Quyết (202416331), Vũ Việt Tiến (202416362), Nguyễn Xuân Hồng Quân (202416321), Đặng Minh Quân (202416323).
- Visual direction pinned by the user (2026-11-14): Apple iOS 26 Liquid Glass, premium SaaS analytics, light ice-blue ground; palette #F5F9FF / #FFFFFF / #2563EB / #60A5FA / #EAF4FF / #172554 / #172033 / #64748B; Inter.

## Evidence on Hand

- Real serving-layer tables: batch_revenue_summary, batch_top_products, batch_order_status_summary, batch_customer_segments, data_quality_report, streaming_window_metrics, streaming_category_metrics, benchmark_results, storage_comparison, rowgroup_experiments.
- No live Kafka / Spark / HDFS in local mode; connection status must be probed, never asserted.

## Product Principles

1. Every number on screen comes from the serving layer or configuration; nothing is hardcoded for looks.
2. Explain why a result happens (pruning, pushdown, watermark), not only what it is.
3. An examiner should find any result within two clicks.
4. Honest labelling of estimates and derived counts.
