# Trust & Safety – Real-Time Data Platform

## Overview

This project implements a mini real-time data platform designed to support Trust & Safety workflows at Resend. The system ingests high-volume event streams, processes them in real time, and produces queryable datasets and metrics that support abuse detection, monitoring, and enforcement decisions.

The design emphasizes:

* Real-time processing (not near real-time)
* Scalable, event-driven architecture
* Strong schema and data quality guarantees
* SQL-friendly analytics for Trust & Safety teams

---

## Architecture

**Components**

1. **Event Producer (TypeScript)**

   * Simulates high-volume Trust & Safety events
   * Publishes events to Amazon Kinesis Data Streams

2. **Streaming Pipeline**

   * AWS Lambda (TypeScript) consumes Kinesis events
   * Performs validation, enrichment, and aggregation
   * Writes:

     * Raw immutable events to S3 (Parquet)
     * Aggregated metrics to Redshift/Athena

3. **Analytics & Dashboard Layer**

   * SQL-accessible aggregation tables
   * Intended for exploration via Amazon QuickSight

---

## Event Schema

Events are immutable and strongly typed.

Core fields include:

* event_id
* event_type (enum)
* user_id
* ip_address
* email_domain
* timestamp
* metadata (campaign, provider, etc.)

Schema is enforced both at compile-time (TypeScript) and runtime (validation layer).

---

## Data Quality Strategy

* Strict schema validation at ingestion
* Invalid events routed to a dead-letter S3 bucket
* Metrics tracked:

  * Invalid event rate
  * Event lag
  * Volume by event type

This mirrors real-world Trust & Safety audit and appeal requirements.

---

## Aggregated Metrics

Example real-time outputs:

* Events per domain per minute
* Abuse flags per user (rolling window)
* Bounce and spam rates by campaign
* Top offending IPs/domains

These tables are designed for direct consumption by BI tools.

---

## Scalability Considerations

* Kinesis scales horizontally with shard count
* Lambda scales with stream throughput
* Immutable raw storage allows replay and backfill
* Stateless processing enables fault tolerance

---

## Tradeoffs & Future Improvements

* Could replace Lambda with Kinesis Data Analytics (SQL) for complex windowing
* Could add rule engine for automated enforcement
* Could introduce feature store for ML-based abuse detection

---

## Demo

See Loom video for walkthrough of:

* System design
* Code structure
* Streaming flow
* Example Trust & Safety queries
