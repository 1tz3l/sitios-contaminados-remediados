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


def extract_year_from_date(df, column_name):
    """
    Extracts the year from a column that may contain either dates or just years,
    and ensures it is in YYYY format as an integer.
    """
    df[column_name] = df[column_name].apply(lambda x: str(x).split("-")[0] if pd.notnull(x) else np.nan)
    
    # Convert to numeric and then to an integer to remove any decimals
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype('Int64')
    
    return df


def transform_contaminated(df):
    """
    Transforms 'sitios contaminados' (contaminated zones) DataFrame.
    """
    # Rename columns
    df = df.rename(columns={
        "No. de registro": "Registro",
        "Año de identificación como sitio contaminado": "Año de identificación",
        "Modalidad del sitio contaminado": "Modalidad",
        "Responsable de la contaminación": "Responsable",
        "Cuenta con programa de remediación aprobado": "Programa de remediación aprobado"
    })

    df = extract_year_from_date(df, "Año de identificación")
    return df


def transform_remediated(df):
    """
    Transforms 'sitios remediados' (remediated zones) DataFrame.
    """
    # Rename columns
    df = df.rename(columns={
        "No. de registro": "Registro",
        "Año de identificación como sitio contaminado": "Año de identificación",
        "Modalidad del sitio contaminado": "Modalidad",
        "Responsable de la contaminación": "Responsable",
        "Responsable técnico de la remediación": "Responsable de la remediación",
        "Clasificación de técnica o proceso de tratamiento": "Técnica",
        "Técnica o proceso de tratamiento": "Proceso de tratamiento",
        "Año de conclusión de la remediación": "Año de conclusión"
    })

    df = extract_year_from_date(df, "Año de identificación")
    df = extract_year_from_date(df, "Año de conclusión")
    return df


def merge_datasets(df1, df2):
    """
    Merges two DataFrames and handles column conflicts.
    """
    # Drop conflicting columns in df2
    drop_columns = ["Modalidad", "Responsable", "Estado", "Municipio", "Tipo de evento", 
                    "Actividad del responsable de la contaminación SCIAN 2023 - Clase",
                    "Actividad del responsable de la contaminación SCIAN 2023 - Subsector",
                    "Contaminante (específico)", "Contaminante (genérico)", "x", "y", "Año de identificación"]
    
    df2 = df2.drop(columns=[col for col in drop_columns if col in df2.columns], errors='ignore')

    # Perform outer merge on 'Registro' and 'Ubicación'
    df_merged = pd.merge(df1, df2, on=["Registro", "Ubicación"], how="outer")

    # Ensure that unique columns from df2 are not dropped and reorder them if necessary
    unique_columns_df2 = ["Técnica", "Proceso de tratamiento", "Año de conclusión"]
    for col in unique_columns_df2:
        if col not in df_merged.columns:
            df_merged[col] = df2[col]

    # Reorder columns if needed
    if 'Año de identificación' in df_merged.columns and 'Año de conclusión' in df_merged.columns:
        cols = df_merged.columns.tolist()
        cols.insert(cols.index('Año de conclusión') + 1, cols.pop(cols.index('Año de identificación')))
        df_merged = df_merged[cols]

    return df_merged

def handle_missing_columns(df):
    """
    Ensures that essential columns exist in the DataFrame, filling with NaNs if necessary.
    """
    required_columns = [
        "Técnica", "Responsable de la remediación", 
        "Proceso de tratamiento", "Año de conclusión", 
        "Área de suelo remediado m²", "Volumen de suelo remediado m³"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan
    return df


def concat_data(dfs):
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    return combined_df


def plot_year_distribution(df, output_dir):
    df["Año de identificación"].value_counts().sort_index().plot(kind='bar', figsize=(10, 6))
    plt.title("Distribution of Records by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Records")
    plt.grid(True)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(os.path.join(output_dir, "year_distribution.png"))
    plt.close()

def load_processed_data(df, output_path):
    df.to_csv(output_path, index=False)


# Main execution
file_paths = {
    "sitios_contaminados": "raw_data/Sitios_contaminados_geoportal.csv",
    "sitios_remediados": "raw_data/Sitios_remediados_geoportal.csv"
}

dataframes = read_data(file_paths)

transformed_contaminated = transform_contaminated(dataframes["sitios_contaminados"])
transformed_remediated = transform_remediated(dataframes["sitios_remediados"])

df_merged = merge_datasets(transformed_contaminated, transformed_remediated)
df_merged = handle_missing_columns(df_merged)


# Perform plots
plot_year_distribution(df_merged, "dataviz")


# Save combined data
load_processed_data(df_merged, "processed_data/combined_data.csv")