import pandas as pd
import numpy as np

def calculate_iat_statistics(df):
    """
    Calcula las estadísticas de los intervalos entre llegadas (IAT) para cada flujo en el DataFrame.
    
    Parámetros:
    - df (pandas.DataFrame): DataFrame que contiene los datos de flujo con columnas 'flow_id' y 'timestamp'.
    
    Retorna:
    - Un DataFrame con las estadísticas de IAT (media, desviación estándar, máximo, mínimo) por flujo.
    """
    try:
        # Verifica que las columnas necesarias existan en el DataFrame
        if 'flow_id' not in df.columns or 'timestamp' not in df.columns:
            raise ValueError("El DataFrame debe contener las columnas 'flow_id' y 'timestamp'.")

        # Intenta convertir 'timestamp' al formato datetime si no está ya en ese formato
        if not np.issubdtype(df['timestamp'].dtype, np.datetime64):
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            except ValueError as e:
                raise ValueError("No se pudo convertir 'timestamp' a datetime. Verifica el formato de la fecha.") from e

        # Ordena el DataFrame por 'flow_id' y 'timestamp'
        df = df.sort_values(by=['flow_id', 'timestamp'])

        # Calcula el IAT
        df['iat'] = df.groupby('flow_id')['timestamp'].diff().dt.total_seconds().fillna(0)

        # Agrupa por 'flow_id' y calcula las estadísticas
        iat_stats = df.groupby('flow_id')['iat'].agg(['mean', 'std', 'max', 'min']).reset_index()
        iat_stats.columns = ['flow_id', 'flow_iat_mean', 'flow_iat_std', 'flow_iat_max', 'flow_iat_min']

        return iat_stats
    except ValueError as ve:
        print(f"Error de valor: {ve}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error específico de valor
    except Exception as e:
        print(f"Error inesperado al calcular estadísticas de IAT: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error inesperado

