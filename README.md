# AGENCY Travel Data Pipeline

A high-performance, modular ETL (Extract, Transform, Load) system for processing diverse travel booking datasets into a unified analytical format using [DuckDB](https://duckdb.org/) and [Pandas](https://pandas.pydata.org/).

## 🚀 Overview

This repository contains a suite of specialized ETL pipelines designed to ingest raw airline/agency data (Excel, CSV, etc.), clean it, and reconstruct complex passenger journeys. The system is architecture for speed and memory efficiency, capable of handling millions of records on consumer hardware.

### Key Features

- **Dataset-Specific Logic**: Custom cleaning and trip reconstruction rules for each data source.
- **Unified Destination**: All pipelines output to a shared, high-speed DuckDB instance.
- **Route Stitching**: Automatically combines individual flight segments into continuous passenger trips based on time thresholds (e.g., 24-36 hour stopover rules).
- **Batch Processing**: Memory-optimized execution using configurable batch sizes and memory limits.

## 📂 Project Structure

Each dataset has its own directory containing loader utilities and the main ETL script:

- **[`BLUESTAR/`](BLUESTAR/README.md)**: Processing for Blue Star dataset.
- **`TA/`**: Processing for Travel Agency dataset.
- **`TBO/`**: Processing for TBO dataset.
- **`TRIPJACK/`**: Processing for Tripjack dataset.
- **`TRUST/`**: Processing for TRUST dataset.

## 🛠️ Technologies

- **Python 3.10+**
- **DuckDB**: Fast analytical database for SQL-based transformations.
- **Pandas**: Efficient data manipulation and unpivoting.

## 🏃 Usage

To run a specific pipeline, navigate to the component directory or run the script directly:

1. **Load Data** (Example for BLUESTAR):

   ```bash
   python BLUESTAR/load_excels.py
   ```

2. **Process Pipeline**:
   ```bash
   python BLUESTAR/bluestar.py
   ```

## ⚙️ Configuration

Shared configuration patterns across scripts:

- `THREADS`: CPU threads for DuckDB.
- `MEMORY_LIMIT`: RAM allocation for the database.
- `BATCH_SIZE`: Number of rows processed per iteration.

## 📄 License

[MIT](LICENSE)
