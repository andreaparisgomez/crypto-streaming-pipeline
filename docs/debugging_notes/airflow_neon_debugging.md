# Airflow and Neon Debugging Notes

## Overview

This document summarises several debugging challenges encountered while integrating Apache Airflow with the operational PostgreSQL database and the Neon analytical warehouse.

The issues primarily involved:

- Docker container networking
- environment variable propagation
- PostgreSQL connection handling
- Airflow container isolation
- local-to-cloud warehouse orchestration

These debugging steps became an important part of understanding how orchestration infrastructure behaves in containerised environments.

---

## Initial Goal

The objective was to allow Airflow DAGs running inside Docker containers to:

1. read data from the local operational PostgreSQL database (`crypto_db`)
2. transform and aggregate analytical metrics
3. load analytical fact tables into the Neon warehouse

This required simultaneous access to:

- local PostgreSQL
- cloud-hosted Neon PostgreSQL
- Airflow Docker containers

---

# Issue 1 — Airflow Could Not Read Environment Variables

## Problem

The Airflow DAGs could not access the PostgreSQL credentials defined in the `.env` file.

Errors included:

```text
NoneType connection values
database authentication failures
missing environment variable values
```

The DAGs worked correctly outside Airflow but failed inside the Airflow containers.

---

## Root Cause

Although the `.env` file existed locally, the environment variables were not being propagated into the running Docker containers.

Updating the `.env` file alone was insufficient because the containers had already been initialised using older environment configurations.

---

## Solution

The Airflow containers needed to be fully recreated.

The following commands were used:

```bash
docker compose down

docker compose up -d
```

This forced Docker Compose to rebuild the Airflow container environment using the updated `.env` configuration.

After container recreation, the DAGs successfully loaded the PostgreSQL credentials.

---

# Issue 2 — localhost Failed Inside Docker Containers

## Problem

After environment variables loaded correctly, Airflow still could not connect to PostgreSQL.

Connection attempts using:

```text
localhost
```

failed from inside the containers.

---

## Root Cause

Inside Docker containers:

```text
localhost
```

refers to the container itself rather than the host machine.

The PostgreSQL server was running on the local host machine rather than inside the Airflow container network.

---

## Solution

The PostgreSQL host variable was updated from:

```env
POSTGRES_HOST=localhost
```

to:

```env
POSTGRES_HOST=host.docker.internal
```

This special Docker hostname allows containers to access services running on the host machine.

After updating the host value and restarting Airflow containers, the DAGs successfully connected to the local PostgreSQL database.

---

# Issue 3 — Neon Warehouse SSL Connections

## Problem

Initial Neon PostgreSQL connection attempts failed due to SSL requirements.

Neon requires secure SSL-enabled PostgreSQL connections by default.

---

## Root Cause

The PostgreSQL connection configuration did not initially include SSL parameters.

---

## Solution

The warehouse environment configuration was updated to include:

```env
WAREHOUSE_SSLMODE=require
```

Connection strings were updated accordingly:

```text
postgresql://USER:PASSWORD@HOST/neondb?sslmode=require
```

After enabling SSL mode, Airflow and local scripts successfully connected to the Neon warehouse.

---

# Issue 4 — Airflow DAGs Not Appearing

## Problem

Some DAG files were not visible inside the Airflow UI after new DAG creation.

---

## Root Cause

The DAG directory volume mapping was not updating automatically inside the running containers.

Airflow scheduler refresh behaviour inside Docker did not immediately detect newly added DAG files.

---

## Solution

The issue was resolved by:

```bash
docker compose restart
```

and verifying DAG availability inside containers using:

```bash
docker compose exec airflow-scheduler ls -l /opt/airflow/dags
```

This confirmed whether DAG files were correctly mounted inside the Airflow containers.

---

# Issue 5 — Warehouse Loading Separation

## Problem

Initially, the architecture attempted to treat the operational PostgreSQL database as both:

- streaming persistence layer
- analytical warehouse

This quickly became difficult to manage as the project expanded.

---

## Root Cause

Operational streaming storage and analytical warehousing have different requirements:

| Operational Storage | Analytical Warehouse |
|---|---|
| frequent inserts | analytical querying |
| near real-time persistence | historical aggregation |
| streaming outputs | dashboard consumption |
| transactional workloads | read-heavy workloads |

Mixing both responsibilities inside a single database complicated dashboard modelling and analytical workflows.

---

## Solution

The architecture was separated into:

### Operational Layer

Local PostgreSQL (`crypto_db`)

Responsible for:

- streaming persistence
- Kafka consumer outputs
- intermediate processing storage

### Analytical Warehouse Layer

Neon PostgreSQL warehouse

Responsible for:

- fact tables
- dimension tables
- dashboard views
- historical analytical querying

This separation significantly simplified dashboard development and dimensional modelling.

---

# Issue 6 — Airflow Scheduling vs Streaming Behaviour

## Problem

There was initial confusion regarding whether Airflow should orchestrate the streaming services themselves.

---

## Root Cause

Airflow is designed primarily for scheduled orchestration rather than managing continuously running streaming infrastructure.

The streaming services:

- Kafka producers
- Spark streaming jobs
- PostgreSQL consumers

operate continuously and independently.

---

## Solution

The architecture was redesigned so that:

### Streaming Infrastructure

Runs independently:

- Kafka
- Spark Structured Streaming
- PostgreSQL consumers

### Airflow

Operates downstream on persisted data:

- warehouse loading
- daily aggregations
- monitoring workflows
- analytical refresh tasks

This separation created a cleaner and more realistic modern data platform architecture.

---

# Lessons Learned

Several important engineering lessons emerged from these debugging sessions.

---

## Docker Networking Matters

Container networking behaves differently from local execution.

Understanding:

- `localhost`
- Docker bridge networking
- `host.docker.internal`
- volume mounting

became essential for orchestration debugging.

---

## Environment Variables Require Container Recreation

Updating `.env` files alone does not always update running containers.

In many cases, containers must be recreated for environment changes to propagate correctly.

---

## Operational and Analytical Layers Should Be Separated

Separating:

- streaming persistence
- analytical warehousing

greatly simplified:

- dashboard modelling
- analytical querying
- warehouse design
- orchestration logic

---

## Airflow Is Not a Streaming Engine

Airflow works best as:

- an orchestration layer
- a scheduling platform
- a monitoring framework

rather than as a manager for continuously running streaming infrastructure.

---

# Final Outcome

After resolving these issues, the final platform successfully supported:

- Kafka streaming ingestion
- Spark Structured Streaming
- operational PostgreSQL persistence
- Airflow orchestration
- Neon analytical warehousing
- dimensional modelling
- dashboard analytics
- warehouse loading workflows
- monitoring DAGs

The debugging process became a significant learning component of the project itself and helped shape the final architecture decisions.
