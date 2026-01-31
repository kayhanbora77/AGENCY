# TRUST Travel Data Pipeline

A high-performance ETL (Extract, Transform, Load) pipeline for processing flight booking data using [DuckDB](https://duckdb.org/) and [Pandas](https://pandas.pydata.org/).

## 🚀 Overview

This project is designed to ingest raw travel data (from Excel or other sources), clean and normalize it, and restructure flight segments into complete passenger journeys. It handles complex logic such as:

- **Flight Normalization**: Standardizing flight numbers and dates.
- **Route Reconstruction**: Stitching together individual flight legs into continuous trips based on time thresholds (e.g., 36-hour stopover rule).
- **Data Deduplication**: Removing duplicate entries to ensure data integrity.
- **Batch Processing**: Efficiently handling large datasets with memory-optimized DuckDB configurations.

## 📂 Project Structure

- **`TRUST/`**
  - `trust_travel.py`: Main ETL script for the TRUST dataset. Handles route logic and target table population.
  - `load_excels.py`: Utility to bulk load raw Excel data files into DuckDB for processing.
- **`TBO/`**
  - `tbo3.py`: ETL logic specific to the TBO dataset, featuring advanced unpivoting and trip sequence detection.

## 🛠️ Technologies

- **Python 3.10+**
- **DuckDB**: Embedded SQL OLAP database for fast analytical queries.
- **Pandas**: Data manipulation and DataFrame handling.

## ⚙️ Configuration

Key configuration parameters (found in scripts):

- `DATABASE_DIR`: Path to the DuckDB database file.
- `BATCH_SIZE`: Number of rows processed per batch to manage memory.
- `THREADS`: CPU threads allocated for DuckDB execution.
- `MEMORY_LIMIT`: Hard memory limit for DuckDB to prevent OOM errors.

## 🏃 Usage

1. **Load Data**:
   Ensure your raw Excel files are in the configured directory and run:

   ```bash
   python TRUST/load_excels.py
   ```

2. **Run Pipeline**:
   Execute the main processing script to clean and transform the data:
   ```bash
   python TRUST/trust_travel.py
   ```

## 📄 License

[MIT](LICENSE)
