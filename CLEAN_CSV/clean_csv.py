import pandas as pd
import numpy as np


def clean_csv(input_file, output_file):
    # 1. Load the CSV file into a DataFrame
    # Use the 'sep' parameter if your delimiter isn't a comma (e.g., ';')
    df = pd.read_csv(input_file)

    # 2. Inspect the data (optional but recommended)
    print("Original data shape:", df.shape)
    print("Missing values check:\n", df.isnull().sum())
    print("Duplicate rows check:", df.duplicated().sum())

    # 3. Handle Missing Values
    # Option A: Drop rows with any missing values
    df_cleaned = df.dropna(how="any")

    # Option B: Fill missing values with a specific value (e.g., mean for numbers, 'N/A' for text)
    # df_cleaned = df.fillna(df.mean(numeric_only=True))
    # df_cleaned['text_column'] = df_cleaned['text_column'].fillna('N/A')

    # 4. Remove Duplicate Rows
    df_cleaned = df_cleaned.drop_duplicates()

    # 5. Data Transformation (Example: convert a date column to datetime objects)
    # df_cleaned['date_column'] = pd.to_datetime(df_cleaned['date_column'], errors='coerce')

    # 6. Save the cleaned data to a new CSV file
    # 'index=False' prevents pandas from writing the DataFrame index as an extra column
    df_cleaned.to_csv(output_file, index=False)

    print(f"\nCleaned data saved to {output_file}")
    print("Cleaned data shape:", df_cleaned.shape)


def main():
    clean_csv("messy_data.csv", "cleaned_data.csv")


if __name__ == "__main__":
    main()
