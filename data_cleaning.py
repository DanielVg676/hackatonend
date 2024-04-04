import pandas as pd
import numpy as np
from scipy.stats import zscore

def replace_inf_with_nan(df):
    """
    Reemplaza valores infinitos con NaN en un DataFrame.
    """
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

def fill_na(df, numeric_features, categorical_features):
    """
    Llena los valores NaN en características numéricas con la mediana y en características categóricas con la moda.
    """
    for column in numeric_features:
        if column in df.columns:
            df[column] = df[column].fillna(df[column].median())
        else:
            print(f"Advertencia: La columna numérica '{column}' no existe en el DataFrame.")
    for column in categorical_features:
        if column in df.columns:
            mode_value = df[column].mode()
            if not mode_value.empty:
                df[column] = df[column].fillna(mode_value[0])
            else:
                df[column] = df[column].fillna("Unknown")
        else:
            print(f"Advertencia: La columna categórica '{column}' no existe en el DataFrame.")

def remove_outliers(df, numeric_features):
    """
    Identifica y trata valores atípicos en características numéricas utilizando el método Z-Score.
    """
    for column in numeric_features:
        if column in df.columns:
            # Evita calcular el z-score para columnas con un único valor único (std=0)
            if df[column].std() > 0:
                df[column] = np.where(np.abs(zscore(df[column])) > 3, df[column].median(), df[column])
        else:
            print(f"Advertencia: La columna numérica '{column}' para la eliminación de atípicos no existe en el DataFrame.")

def clean_data(df, numeric_features, categorical_features):
    """
    Función agregadora que aplica todas las funciones de limpieza.
    """
    if df.empty:
        print("Error: El DataFrame de entrada está vacío. La limpieza de datos no puede proceder.")
        return
    
    replace_inf_with_nan(df)
    fill_na(df, numeric_features, categorical_features)
    remove_outliers(df, numeric_features)

# Ejemplo de uso
# Asegúrate de definir 'df', 'numeric_features' y 'categorical_features' antes de llamar a clean_data.
# clean_data(df, numeric_features, categorical_features)
