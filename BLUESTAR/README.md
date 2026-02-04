# BLUESTAR Travel Data Pipeline

High-performance ETL pipeline for processing Blue Star travel booking data.

## 📂 Component Structure

- **`load_excels.py`**:
  Initial data ingestion script. Scans the configured directory for `.xlsx` files, parses date columns (`BillDate`, `FltDate1-4`), and loads them into the `BLUE_STAR` table in DuckDB.
- **`bluestar.py`**:
  The core processing engine. It performs:
  - **Data Cleaning**: Uses standard regex to normalize flight numbers and validate passenger names.
  - **Unpivoting**: Transforms the wide segment-based schema into a long format for route analysis.
  - **Route Reconstruction**: Groups individual flights back into logical trips using a **24-hour stopover threshold**.
  - **Deduplication**: Ensures identical flight segments are consolidated per passenger/PNR.
  - **Batch Processing**: Efficiently processes millions of rows in configurable chunks (default: 100,000) with DuckDB optimization.

## 🚀 Getting Started

### 1. Ingest Raw Data

Ensure your Excel files are placed in the directory defined by `EXCEL_DIR` in `load_excels.py`, then run:

```bash
python BLUESTAR/load_excels.py
```

### 2. Run ETL Pipeline

Execute the main script to process the data and populate the target tables:

```bash
python BLUESTAR/bluestar.py
```

## ⚙️ Configuration

Key settings in `bluestar.py`:

- `BATCH_SIZE`: Memory tuning for DuckDB transactions.
- `THREADS`: Parallel execution threads.
- `MEMORY_LIMIT`: DuckDB memory cap.
