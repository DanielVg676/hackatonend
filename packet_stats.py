import pandas as pd
import numpy as np

def calculate_packet_stats(df):
    """
    Calcula estadísticas detalladas de longitud de paquetes para cada flujo en el DataFrame.
    
    Args:
    df (pd.DataFrame): DataFrame que contiene las longitudes de los paquetes y el flow_id.
    
    Returns:
    pd.DataFrame: DataFrame con las estadísticas agregadas por flow_id.
    """
    try:
        # Verificar que el DataFrame contenga las columnas necesarias
        required_columns = {'flow_id', 'packet_length', 'direction'}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"El DataFrame no contiene las columnas requeridas: {required_columns - set(df.columns)}")
        
        # Asegurarse de que 'packet_length' es numérico
        if not np.issubdtype(df['packet_length'].dtype, np.number):
            raise TypeError("La columna 'packet_length' debe ser de tipo numérico.")
        
        # Preparar DataFrame para estadísticas generales y por dirección
        packet_stats = df.groupby(['flow_id', 'direction'])['packet_length'].agg(
            total_length='sum',
            max_length='max',
            min_length='min',
            mean_length='mean',
            std_length='std'
        ).reset_index()
        
        # Rellenar los valores faltantes de std con 0
        packet_stats['std_length'] = packet_stats['std_length'].fillna(0)

        
        # Pivotear el DataFrame para tener columnas separadas por dirección
        direction_stats = packet_stats.pivot(index='flow_id', columns='direction', values=['total_length', 'max_length', 'min_length', 'mean_length', 'std_length'])
        
        # Aplanar el MultiIndex de columnas y renombrar para claridad
        direction_stats.columns = ['_'.join(col).strip() for col in direction_stats.columns.values]
        direction_stats.reset_index(inplace=True)
        
        # Calcular estadísticas generales de longitud de paquete
        general_stats = df.groupby('flow_id')['packet_length'].agg(
            min_packet_length='min',
            max_packet_length='max',
            mean_packet_length='mean',
            std_packet_length='std',
            var_packet_length='var'
        ).reset_index()
        
        # Combinar estadísticas de dirección y generales en un solo DataFrame
        combined_stats = pd.merge(direction_stats, general_stats, on='flow_id', how='outer')
        
        # Rellenar los valores faltantes con 0, asumiendo que la ausencia de paquetes implica longitud 0
        combined_stats.fillna(0, inplace=True)
        
        return combined_stats
    except ValueError as ve:
        print(f"Error de valor: {ve}")
        return pd.DataFrame()  # Devuelve un DataFrame vacío en caso de error
    except TypeError as te:
        print(f"Error de tipo: {te}")
        return pd.DataFrame()  # Devuelve un DataFrame vacío en caso de error
    except Exception as e:
        print(f"Error inesperado al calcular estadísticas de paquetes: {e}")
        return pd.DataFrame()  # Devuelve un DataFrame vacío para manejar cualquier otro error inesperado
