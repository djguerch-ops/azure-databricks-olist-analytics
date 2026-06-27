# Azure Databricks Olist Analytics Platform

[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![PySpark](https://img.shields.io/badge/PySpark-BigData-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-FF3621)](https://databricks.com/product/unity-catalog)
[![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-Storage-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

End-to-end Lakehouse platform built on Azure Databricks, ingesting and transforming **1.5M+ records** from the Brazilian Olist e-commerce dataset through a Medallion Architecture with Delta Lake, Unity Catalog, ADLS Gen2, and Auto Loader.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Azure Infrastructure](#azure-infrastructure)
- [Getting Started](#getting-started)
- [Medallion Architecture](#medallion-architecture)
- [Technology Stack](#technology-stack)
- [Data Volumes](#data-volumes)
- [Dashboard](#dashboard)
- [Auto Loader](#auto-loader)
- [Project Structure](#project-structure)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

## Overview

This project implements a production-grade Lakehouse Data Platform on Azure Databricks.

It ingests raw e-commerce data from the **Brazilian Olist dataset** (9 CSV files) stored in **Azure Data Lake Storage Gen2**, processes them through Bronze, Silver, and Gold layers using PySpark and Delta Lake, and exposes business-ready analytics through a **Databricks SQL Dashboard**.

The project also demonstrates **incremental ingestion** using **Auto Loader** (CloudFiles) with checkpoint management.

The dataset is sourced from Kaggle: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Architecture

[![Architecture](images/Architecture.png)](images/Architecture.png)

The platform follows a layered Lakehouse architecture:

- **Ingestion** — CSV files stored in ADLS Gen2 External Location, ingested via batch and Auto Loader
- **Bronze** — Raw data persisted as Delta tables in Unity Catalog, no transformation
- **Silver** — Cleaned, validated, deduplicated, and enriched data
- **Gold** — Business KPI aggregations ready for analytics
- **Serving** — Databricks SQL Dashboard

---

## Azure Infrastructure

[![Azure Resources](images/azure_resources.png)](images/azure_resources.png)

**Resource Group**: `rg-olist-databricks-dev`

| Resource | Type | Role |
|----------|------|------|
| Azure Databricks Workspace | Databricks | Processing & orchestration |
| Azure Data Lake Storage Gen2 | Storage | Raw file storage (landing zone) |
| Azure Databricks Access Connector | Managed Identity | Secure ADLS Gen2 access |

**Unity Catalog setup:**
- Storage Credential → links Databricks to ADLS Gen2 via Access Connector
- External Location → exposes the ADLS Gen2 container to Unity Catalog
- Catalog `olist` → schemas `bronze`, `silver`, `gold`

---

## Getting Started

### Prerequisites

- An **Azure account** with an active subscription
- **Azure Databricks workspace** (Premium tier for Unity Catalog)
- **Azure Data Lake Storage Gen2** account
- A cluster with **Databricks Runtime 13.0+**

### 1. Download the Dataset

Download the Olist dataset from Kaggle:

👉 **https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce**

Files needed:
- `olist_orders_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

### 2. Create Azure Infrastructure

In the Azure Portal, create the following resources in a resource group `rg-olist-databricks-dev`:

```
1. Azure Data Lake Storage Gen2
   → Create a container: raw-data/olist/

2. Azure Databricks Workspace (Premium tier)

3. Azure Databricks Access Connector
   → Assign role "Storage Blob Data Contributor" on the ADLS Gen2
```

### 3. Configure Unity Catalog

In your Databricks workspace:

```sql
-- Create storage credential (using Access Connector managed identity)
CREATE STORAGE CREDENTIAL olist_credential
USING AZUREMSI <access-connector-resource-id>;

-- Create external location pointing to ADLS Gen2
CREATE EXTERNAL LOCATION olist_raw_data
URL 'abfss://raw-data@<storage-account>.dfs.core.windows.net/olist/'
WITH (STORAGE CREDENTIAL olist_credential);

-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS olist;
CREATE SCHEMA IF NOT EXISTS olist.bronze;
CREATE SCHEMA IF NOT EXISTS olist.silver;
CREATE SCHEMA IF NOT EXISTS olist.gold;
```

### 4. Upload Data to ADLS Gen2

Upload the 9 CSV files to your ADLS Gen2 container:
```
abfss://raw-data@<storage-account>.dfs.core.windows.net/olist/
```

Use Azure Storage Explorer or the Azure Portal UI.

### 5. Clone the Repository

In your Databricks workspace:
1. Go to **Workspace** → **Repos** → **Add Repo**
2. Enter: `https://github.com/djguerch-ops/azure-databricks-olist-analytics`
3. Click **Create Repo**

### 6. Run the Notebooks in Order

| Step | Notebook | Description |
|------|----------|-------------|
| 1 | `01_bronze_ingestion.py` | Batch ingestion of all 9 CSV files into Bronze Delta tables |
| 2 | `02_silver_transformation_*.py` | One notebook per entity — data quality & enrichment |
| 3 | `03_gold_sales_analytics.py` | Business KPI aggregations |
| 4 | `04_autoloader_bronze_ingestion.py` | Incremental ingestion demo with Auto Loader |

---

## Medallion Architecture

### Bronze Layer

Raw ingestion of all 9 Olist CSV files from ADLS Gen2 into Delta tables in Unity Catalog.

- No transformation — raw data preserved as-is
- Metadata columns added: `ingestion_timestamp`
- Schema inference from CSV files
- Tables stored in `olist.bronze`

**Bronze tables:**

| Table | Description |
|-------|-------------|
| `olist.bronze.orders` | All customer orders |
| `olist.bronze.customers` | Customer information |
| `olist.bronze.products` | Product catalog |
| `olist.bronze.order_items` | Items per order |
| `olist.bronze.payments` | Payment methods and values |
| `olist.bronze.reviews` | Customer reviews and scores |
| `olist.bronze.sellers` | Seller information |
| `olist.bronze.geolocation` | Brazilian zip code geolocation |
| `olist.bronze.category_translation` | Portuguese to English category names |

### Silver Layer

Data quality rules applied per entity:

- Null handling and filtering
- Deduplication via `dropDuplicates()`
- Business validations (e.g. valid order statuses, positive amounts)
- Standardization of column types and naming conventions
- Data enrichment (e.g. joining category translations)
- Tables stored in `olist.silver`

**Silver tables:**

| Table | Key transformations |
|-------|-------------------|
| `olist.silver.orders` | Null filtering, timestamp standardization, status validation |
| `olist.silver.customers` | Deduplication, null handling |
| `olist.silver.products` | Category name translation (EN), null filtering |
| `olist.silver.order_items` | Price validation, freight validation |
| `olist.silver.payments` | Payment value validation, type standardization |
| `olist.silver.reviews` | Score validation (1-5), null handling |
| `olist.silver.sellers` | Deduplication, null handling |
| `olist.silver.category_translation` | Language standardization |

### Gold Layer

Business-oriented aggregations joining multiple Silver tables.

**Main table**: `olist.gold.sales_analytics`

**Metrics computed:**

| Metric | Description |
|--------|-------------|
| `total_revenue` | Sum of all order payment values |
| `total_orders` | Count of distinct orders |
| `total_customers` | Count of distinct customers |
| `total_products` | Count of distinct products sold |
| `avg_item_price` | Average price per order item |
| `total_freight_cost` | Total freight cost across all orders |
| `avg_review_score` | Average customer review score |

---

## Technology Stack

| Technology | Role |
|------------|------|
| Azure Databricks | Distributed processing & orchestration |
| Apache Spark / PySpark | Data transformation at scale |
| Delta Lake | ACID transactions & time travel |
| Unity Catalog | Data governance, access control & lineage |
| ADLS Gen2 | Raw file storage (landing zone) |
| Auto Loader (CloudFiles) | Incremental file ingestion |
| Databricks SQL | Interactive querying & dashboards |

---

## Data Volumes

| Table | Layer | Rows |
|-------|-------|------|
| orders | Bronze/Silver | 99,441 |
| customers | Bronze/Silver | 99,441 |
| products | Bronze/Silver | 32,951 |
| order_items | Bronze/Silver | 112,650 |
| payments | Bronze/Silver | 103,886 |
| reviews | Bronze/Silver | 98,410 |
| sellers | Bronze/Silver | 3,095 |
| geolocation | Bronze/Silver | 1,000,163 |
| **Total** | | **~1,550,037** |

---

## Dashboard

[![Dashboard](images/dashboard.png)](images/dashboard.png)

The Gold layer feeds a **Databricks SQL Dashboard** with the following KPIs:

| KPI | Description |
|-----|-------------|
| Revenue by Month | Monthly revenue evolution across the dataset period |
| Top Categories | Revenue generated per product category (EN) |
| Customer Activity | New customer acquisition by month |
| Sales Analytics | Global business performance metrics (revenue, orders, avg price) |

---

## Auto Loader

The project includes an **Auto Loader** implementation for incremental ingestion of new order files.

**How it works:**
- New CSV files land in ADLS Gen2
- Auto Loader detects new files using checkpoint tracking
- Only new files are processed — no reprocessing of existing data
- Results written to Bronze Delta tables incrementally

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", schema_path) \
    .load(adls_raw_path) \
    .writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("olist.bronze.orders")
```

**Demo performed:** the orders dataset was split into multiple files and ingested incrementally — only newly arrived files were processed at each run.

---

## Project Structure

```
azure-databricks-olist-analytics/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation_orders.py
│   ├── 02_silver_transformation_customers.py
│   ├── 02_silver_transformation_products.py
│   ├── 02_silver_transformation_items.py
│   ├── 02_silver_transformation_payments.py
│   ├── 02_silver_transformation_review.py
│   ├── 02_silver_transformation_seller.py
│   ├── 02_silver_transformation_category_translation.py
│   ├── 03_gold_sales_analytics.py
│   └── 04_autoloader_bronze_ingestion.py
│
├── images/
│   ├── Architecture.png
│   ├── azure_resources.png
│   ├── unity_catalog.png
│   └── dashboard.png
│
├── docs/
├── LICENSE
└── README.md
```

---

## Future Improvements

- [ ] Power BI integration via DirectQuery on Gold layer
- [ ] CI/CD pipeline with GitHub Actions + Databricks Asset Bundles
- [ ] dbt integration for SQL-based Gold transformations
- [ ] Terraform deployment for Azure infrastructure as code
- [ ] Real-time streaming with Databricks Structured Streaming
- [ ] Data quality framework with Databricks Data Quality
- [ ] End-to-end monitoring with Databricks Lakehouse Monitoring

---

## Author

**Djamel Guerchouche**
Data Engineer

Specialized in cloud-native data platforms, distributed processing, and Lakehouse architecture.

- 🔗 [LinkedIn](https://www.linkedin.com/in/djamel-guerchouche-863559b6/)
- 🐙 [GitHub](https://github.com/djguerch-ops)

**Core expertise:**
Azure · Databricks · Apache Spark · Delta Lake · Unity Catalog · ADLS Gen2 · Python

---

*Built with ❤️ using Azure Databricks, PySpark, Delta Lake and ADLS Gen2.*
