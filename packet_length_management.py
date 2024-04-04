import pandas as pd
import numpy as np
import logging

# Configuración inicial del logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_packet_length(df):
    if 'packet_length' not in df.columns:
        logging.info("'packet_length' no se encuentra en el DataFrame. Intentando calcular...")
        
        # Verifica si las columnas para el cálculo alternativo existen
        required_columns_for_calculation = ['flow.bytes_toserver', 'flow.bytes_toclient', 'total_fwd_packets', 'total_bwd_packets']
        missing_columns = set(required_columns_for_calculation) - set(df.columns)
        if missing_columns:
            logging.error(f"No se pueden calcular 'packet_length' porque faltan las columnas requeridas: {missing_columns}")
            raise ValueError(f"Faltan columnas requeridas para calcular 'packet_length': {missing_columns}")

        # Estimación de 'packet_length'
        df['total_bytes'] = df['flow.bytes_toserver'] + df['flow.bytes_toclient']
        df['total_packets'] = df['total_fwd_packets'] + df['total_bwd_packets']
        df['packet_length'] = df.apply(lambda row: row['total_bytes'] / row['total_packets'] if row['total_packets'] > 0 else 0, axis=1)
        logging.info("La columna 'packet_length' ha sido calculada como una estimación basada en el flujo de bytes.")
    else:
        logging.info("'packet_length' ya existe en el DataFrame.")
    return df

def calculate_packet_length_stats(df):
    """
    Calcula estadísticas detalladas de longitud de paquetes para cada flujo en el DataFrame.
    """
    try:
        df = ensure_packet_length(df)
        
        required_columns = {'flow_id', 'packet_length'}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"El DataFrame no contiene las columnas requeridas: {required_columns - set(df.columns)}")
        
        logging.debug("Columnas antes de la agregación: %s", df.columns.tolist())
        
        packet_length_stats = df.groupby('flow_id')['packet_length'].agg(['mean', 'max', 'min', 'std']).reset_index()
        packet_length_stats.rename(columns={'mean': 'mean_packet_length', 'max': 'max_packet_length', 'min': 'min_packet_length', 'std': 'std_packet_length'}, inplace=True)
        
        logging.debug("Columnas después de la agregación y antes de la fusión: %s", packet_length_stats.columns.tolist())
        
        df = df.merge(packet_length_stats, on='flow_id', how='left')
        
        logging.debug("Columnas después de la fusión: %s", df.columns.tolist())
        
        if 'std_packet_length' not in df.columns:
            logging.error("'std_packet_length' no se encuentra en el DataFrame después de la fusión.")
        
        df['var_packet_length'] = df['std_packet_length'] ** 2
        
        return df

    except ValueError as ve:
        logging.error(f"Error de valor: {ve}")
        return pd.DataFrame()
    except TypeError as te:
        logging.error(f"Error de tipo: {te}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return pd.DataFrame()