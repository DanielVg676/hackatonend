import numpy as np
import pandas as pd  # Asegúrate de importar pandas para manejar DataFrames

def add_packet_direction(df):
    """
    Agrega una columna 'direction' al DataFrame basada en una lógica definida.
    Por simplicidad, asumiremos un criterio simple basado en puertos de destino.
    
    Parámetros:
    - df (pd.DataFrame): DataFrame que contiene los datos de los flujos.

    Retorna:
    - df (pd.DataFrame): DataFrame con la columna 'direction' agregada.
    """
    try:
        # Verifica si 'dest_port' existe en el DataFrame
        if 'dest_port' not in df.columns:
            raise ValueError("La columna 'dest_port' no se encuentra en el DataFrame.")

        # Verifica que 'dest_port' no contenga valores NaN, lo cual podría indicar datos faltantes
        if df['dest_port'].isnull().any():
            raise ValueError("La columna 'dest_port' contiene valores NaN.")

        # Definir la lógica de dirección aquí. Por ejemplo:
        df['direction'] = np.where(df['dest_port'] < 1024, 'forward', 'backward')
        return df

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        # Podrías optar por retornar el DataFrame sin modificar si se prefiere manejar el error sin interrupción
        return df
    except Exception as e:
        print(f"Error inesperado al agregar la dirección del paquete: {e}")
        # Considera si retornar el DataFrame sin modificar o manejar de otra manera
        return df
