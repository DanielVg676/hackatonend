import pandas as pd
import numpy as np
import logging

# Configuración inicial del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_required_columns(df, required_columns):
    """
    Verifica que el DataFrame contenga todas las columnas necesarias.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_message = f"Missing required columns: {', '.join(missing_columns)}"
        logging.error(error_message)
        raise ValueError(error_message)

def calculate_packet_rates(df):
    """
    Calcula las tasas de paquetes por segundo.
    """
    try:
        required_columns = ['total_fwd_packets', 'total_bwd_packets', 'flow_duration']
        check_required_columns(df, required_columns)

        # Realiza los cálculos asumiendo que las columnas necesarias existen
        df['fwd_packets_s'] = np.where(df['flow_duration'] > 0, df['total_fwd_packets'] / df['flow_duration'], 0)
        df['bwd_packets_s'] = np.where(df['flow_duration'] > 0, df['total_bwd_packets'] / df['flow_duration'], 0)

        # Limpieza de datos para evitar NaN o infinitos
        df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)

        logging.info("Packet rates calculated successfully.")
    except ValueError as ve:
        logging.error(f"Error calculating packet rates: {ve}")
        # Decide cómo quieres manejar este error. Podrías devolver el df original o un df vacío.
        raise

    return df

def calculate_header_lengths(df, tcp_header_length=20):
    """
    Calcula las longitudes totales de las cabeceras para flujos en ambas direcciones.
    """
    try:
        required_columns = ['total_fwd_packets', 'total_bwd_packets']
        check_required_columns(df, required_columns)

        df['fwd_header_length_total'] = df['total_fwd_packets'] * tcp_header_length
        df['bwd_header_length_total'] = df['total_bwd_packets'] * tcp_header_length
    except ValueError as ve:
        logging.error(f"Error calculating header lengths: {ve}")
        raise

    return df


def calculate_flags(df):
    """
    Calcula banderas a partir de los valores de 'tcp.flags'.
    """
    try:
        if 'tcp.flags' not in df.columns:
            raise ValueError("'tcp.flags' column is missing.")

        # Intenta convertir 'tcp.flags' a enteros
        df['tcp.flags'] = df['tcp.flags'].fillna(0).astype(int)
        # Continúa con el procesamiento después de la conversión exitosa
        df['fwd_psh_flags'] = np.where(df['tcp.flags'] & 0x08, 1, 0)
        df['bwd_psh_flags'] = df['fwd_psh_flags']
        df['fwd_urg_flags'] = np.where(df['tcp.flags'] & 0x20, 1, 0)
        df['bwd_urg_flags'] = df['fwd_urg_flags']
    except ValueError as ve:
        logging.error(f"Error calculating flags: {ve}")
        raise

    return df



def calculate_active_idle_times(df):


    try:
        required_columns = ['timestamp', 'flow_id']
        check_required_columns(df, required_columns)
        # Asumiendo que df tiene una columna 'timestamp' con las marcas de tiempo de los eventos/paquetes
        # y una columna 'flow_id' para identificar cada flujo
        
        # Ordenar por 'flow_id' y 'timestamp' para asegurar que los paquetes están en orden
        df.sort_values(by=['flow_id', 'timestamp'], inplace=True)
        
        # Calcular la diferencia de tiempo entre paquetes consecutivos dentro del mismo flujo
        df['time_diff'] = df.groupby('flow_id')['timestamp'].diff()
        
        # Definir un umbral para identificar tiempos inactivos (por ejemplo, 5 segundos)
        idle_threshold = 5
        
        # Todo tiempo_diff mayor que el umbral se considera un inicio de tiempo inactivo
        df['is_idle'] = df['time_diff'] > idle_threshold
        
        # Para calcular el tiempo activo, consideramos el tiempo hasta el primer tiempo inactivo
        # Esto es simplificado y debe ajustarse según la definición exacta de "activo" en tus datos
        df['active_time'] = np.where(df['is_idle'], 0, df['time_diff'])
        
        # Calcular tiempos activos e inactivos totales por flujo
        active_idle_stats = df.groupby('flow_id').agg(
            active_mean=('active_time', 'mean'),
            active_std=('active_time', 'std'),
            active_max=('active_time', 'max'),
            active_min=('active_time', 'min'),
            # Para calcular los tiempos inactivos, puedes sumar todos los 'time_diff' marcados como idle
            # Ajusta estos cálculos según tus necesidades específicas
            idle_total=('time_diff', lambda x: x[x > idle_threshold].sum()),
        ).reset_index()
        
        # Fusionar las estadísticas calculadas de vuelta al DataFrame original
        df = df.merge(active_idle_stats, on='flow_id', how='left')
        
        # Limpieza final: eliminar columnas temporales y llenar NaN con 0
        df.drop(columns=['time_diff', 'is_idle'], inplace=True)
        df.fillna(0, inplace=True)
        df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)

        logging.info("Active and idle times calculated successfully.")
    except ValueError as ve:
        logging.error(f"Error calculating active and idle times: {ve}")
        raise

    
    return df

def calculate_tcp_advanced_stats(df):
    required_columns = [
        'total_fwd_packets', 'total_bwd_packets', 'flow_duration',
        'flow.pkts_toserver', 'flow.pkts_toclient', 'flow.bytes_toserver',
        'flow.bytes_toclient', 'tcp.flags', 'timestamp', 'flow_id'
    ]
    check_required_columns(df, required_columns)
    
    df = calculate_packet_rates(df)
    df = calculate_header_lengths(df)
    df = calculate_flags(df)
    
    # Llamar a calculate_active_idle_times para calcular estadísticas de tiempo activo e inactivo
    df = calculate_active_idle_times(df)
    
    # Asegura que todas las nuevas columnas tengan valores válidos para evitar NaN o infinitos
    df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
    
    return df