import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os

def read_data(file_paths):
    """
    Reads the CSV files and returns a dictionary of DataFrames.
    """
    dataframes = {}
    for dataset_name, path in file_paths.items():
        dataframes[dataset_name] = pd.read_csv(path, encoding="latin1")
    return dataframes
    
file_paths = {
    "sitios_contaminados": "raw_data/Sitios_contaminados_geoportal.csv",
    "sitios_remediados": "raw_data/Sitios_remediados_geoportal.csv"
    }

dataframes = read_data(file_paths)


def transform_contaminated(df):
    """
    Transforms the 'sitios contaminados' (contaminated zones) DataFrame.
    """
    # Rename columns
    df = df.rename(columns={
        "No. de registro": "Registro",
        "Año de identificación como sitio contaminado": "Año de identificación",
        "Modalidad del sitio contaminado": "Modalidad",
        "Responsable de la contaminación": "Responsable",
        "Cuenta con programa de remediación aprobado": "Programa de remediación aprobado"
    })

    # Convert data types
    df["Año de identificación"] = pd.to_datetime(df["Año de identificación"])

    # Drop unnecesary columns
    df = df.drop(columns=["x", "y"], errors="ignore")

    # Remove duplicate rows considering only the remaining columns
    df = df.drop_duplicates()

    # Drop rows with any column having NA/null data.
    df = df.dropna()

    return df


def transform_remediated(df):
    """
    Transforms the 'sitios remediados' (remediated zones) DataFrame.
    """
    # Rename columns
    df = df.rename(columns={
        "No. de registro": "Registro",
        "Año de identificación como sitio contaminado": "Año de identificación",
        "Modalidad del sitio contaminado": "Modalidad",
        "Responsable de la contaminación": "Responsable",
        "Responsable técnico de la remediación": "Responsable de la remediación"
    })

    # Convert data types
    df["Año de identificación"] = pd.to_datetime(df["Año de identificación"])
    df["Año de conclusión de la remediación"] = pd.to_datetime(df["Año de conclusión de la remediación"])

    # Drop unnecesary columns
    df = df.drop(columns=["x", "y"], errors="ignore")

    # Remove duplicate rows considering only the remaining columns
    df = df.drop_duplicates()

    # Drop rows with any column having NA/null data.
    df = df.dropna()

    return df


def concat_data(dfs):
    """
    Concatenates multiple DataFrames into one.
    """
    combined_df = pd.concat(dfs, ignore_index=True)

    # Remove duplicate columns
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

    return combined_df

# Apply transformations
transformed_contaminated = transform_contaminated(dataframes["sitios_contaminados"])
transformed_remediated = transform_remediated(dataframes["sitios_remediados"])

# Concatenate DataFrames
df_combined = concat_data([transformed_contaminated, transformed_remediated])


def plot_year_distribution(df, output_dir):
    """
    Plots the distribution of records by year.
    """
    df["Año de identificación"].dt.year.value_counts().sort_index().plot(kind='bar', figsize=(10, 6))
    plt.title("Distribution of Records by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Records")
    plt.grid(True)
    
    # Save plot to file
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(os.path.join(output_dir, "year_distribution.png"))
    plt.close()

# Save the DataFrame to a CSV file
def load_processed_data(df, output_path):
    """
    Saves the processed DataFrame to a CSV file.
    """
    df.to_csv(output_path, index=False)

# Perform plots
plot_year_distribution(df_combined, "dataviz")

# Save combined data
load_processed_data(df_combined, "processed_data/combined_data.csv")
