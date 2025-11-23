# Data Nexus — Big Data Ingestion & Credit-Based API Platform

Hi 👋, I’m **Mukul**, the developer behind **Data Nexus**.

This project demonstrates my capabilities in:

* **Big data ingestion** of heterogeneous datasets
* **Scalable storage** for analytical querying
* **API engineering** with authentication & credits
* **End-to-end architecture design** & deployment readiness

My goal was to build a solution that is not only fully functional, but also **production-aligned and cloud-compatible**, even while using completely **free** and **open-source** technologies.

---

## 🧠 What the system does

Data Nexus is capable of:

✔ Ingesting multi-format files: CSV, TSV, XLSX, JSONL, Parquet
✔ Detecting schema variations & normalizing data
✔ Deduplication via deterministic checksum
✔ High-performance querying via **ClickHouse**
✔ Secure REST API with:

* API-key authentication
* Credit-based access control
* Rate limiting
* Pagination, filters, metadata
  ✔ CSV export for bulk access

---

## ✔ Completed Modules

| Module                                    | Status      |
| ----------------------------------------- | ----------- |
| Project structure + architecture docs     | ✔ Done      |
| Docker Compose stack (API + DBs + worker) | ✔ Done      |
| Postgres schema (users, credits, logs)    | ✔ Done      |
| ClickHouse master schema                  | ✔ Done      |
| Fastify API + TypeScript backend          | ✔ Done      |
| Swagger documentation                     | ✔ Done      |
| API key authentication                    | ✔ Done      |
| Rate limiting (Redis)                     | ✔ Done      |
| Hybrid credit deduction system            | ✔ Done      |
| Query endpoints with filters + metadata   | ✔ Done      |
| CSV export route                          | ✔ Done      |
| Ingestion worker foundation               | ✔ Done      |
| File registry + checksum tracking         | In Progress |
| CSV → ClickHouse ingest path              | In Progress |
| Normalization + full dedupe pipeline      | Pending     |
| Admin credit panel or scripts             | Pending     |

> The ingestion path is running with initial CSV support — validation + Parquet bulk-load will extend it further.

---

## 🧰 Tech Stack I Used

| Layer                   | Technology           |
| ----------------------- | -------------------- |
| Language                | TypeScript (Node.js) |
| API Framework           | Fastify              |
| OLAP Database           | ClickHouse           |
| Metadata & Credits      | PostgreSQL           |
| Caching & Rate Limiting | Redis                |
| Object Storage          | MinIO                |
| Ingestion               | DuckDB + BullMQ      |
| Deployment              | Docker Compose       |

> The solution can seamlessly migrate to AWS/GCP later
> (S3 / RDS / ElastiCache / ClickHouse Cloud / GCS)

---

## 🔐 Credit System (Hybrid Model)

Credits are deducted dynamically based on:

`` Base Cost + (Rows Returned × Cost/Row) + (Compute Time × Cost/Sec) ``

Example error when credits run out:

`` json
{ "error": "Insufficient Credits" }
``

---

## 📡 API Endpoints (Implemented)

| Endpoint              | Purpose                                |
| --------------------- | -------------------------------------- |
| `GET /health`         | Service status                         |
| `GET /docs`           | Swagger UI                             |
| `GET /v1/records`     | List records with filters + pagination |
| `GET /v1/records/:id` | Single record lookup                   |
| `POST /v1/query`      | Advanced query using JSON body         |
| `GET /v1/export`      | CSV export option                      |

**Every response includes:**
`credits_used`, `response_time`, `total_records`, `pagination`, etc.

---

## 🏗 Architecture Snapshot

``
Raw Data → MinIO
         → Ingestion Worker (Node + DuckDB + BullMQ)
         → Clean Data → ClickHouse
         → Fastify API → (Auth + Credits + Filters + Pagination)
         → Clients
``

Supporting services:

* Redis for rate limiting & counters
* PostgreSQL for API keys & usage logs

---

## 📂 Project Structure

``
/docs           → Architecture & API docs
/src/api        → Fastify API backend
/src/ingestion  → Workers + DuckDB pipeline
/src/utils      → Hashing, config, helpers
/deployment     → Docker Compose & env templates
``

---

## 🧩 What’s Next (Planned Extensions)

| Priority | Feature                                         |
| -------- | ----------------------------------------------- |
| High     | Complete normalization & validation layer       |
| High     | Parquet export + stable bulk-load to ClickHouse |
| Medium   | Admin credit operations panel                   |
| Medium   | Meilisearch fuzzy search                        |
| Optional | Tests + performance benchmarks                  |
| Optional | Prometheus & Grafana dashboards                 |

---

## 💡 Reflection

This project helped me showcase:

* **Real-world workflow of a big-data system**
* Designing scalable infrastructure from scratch
* Working with unfamiliar data situations
* Combining SQL + JavaScript engineering skills
* Prioritizing execution under time constraints

I genuinely enjoyed building this and would love to expand on it further 🚀

---

## 👤 Developer

**Mukul**
India
