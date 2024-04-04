# flow_metrics.py

import pandas as pd

def calculate_down_up_ratio(df):
    df['down_up_ratio'] = df['total_bwd_packets'] / df['total_fwd_packets']
    return df

def calculate_average_packet_size(df):
    df['average_packet_size'] = (df['flow.bytes_toserver'] + df['flow.bytes_toclient']) / (df['total_fwd_packets'] + df['total_bwd_packets'])
    return df

def calculate_segment_size_avg(df):
    df['fwd_segment_size_avg'] = df['total_bytes_toserver'] / df['total_fwd_packets']
    df['bwd_segment_size_avg'] = df['total_bytes_toclient'] / df['total_bwd_packets']
    return df

# Esta función y las siguientes serían similares en estructura a las anteriores.
def calculate_bulk_stats(df):
    """
    Calcula estadísticas relacionadas con operaciones "bulk" en el tráfico de red.
    """

    # Definir umbrales para operaciones 'bulk'
    BULK_THRESHOLD_BYTES = 1000  # Supongamos 1000 bytes como umbral
    BULK_THRESHOLD_PACKETS = 10  # Supongamos 10 paquetes como umbral

    # Identificar flujos 'bulk'
    is_bulk_fwd = (df['total_bytes_toserver'] > BULK_THRESHOLD_BYTES) & (df['total_fwd_packets'] > BULK_THRESHOLD_PACKETS)
    is_bulk_bwd = (df['total_bytes_toclient'] > BULK_THRESHOLD_BYTES) & (df['total_bwd_packets'] > BULK_THRESHOLD_PACKETS)

    # Calcular estadísticas promedio para operaciones 'bulk'
    df['fwd_bytes_bulk_avg'] = df.loc[is_bulk_fwd, 'total_bytes_toserver'] / df.loc[is_bulk_fwd, 'total_fwd_packets']
    df['bwd_bytes_bulk_avg'] = df.loc[is_bulk_bwd, 'total_bytes_toclient'] / df.loc[is_bulk_bwd, 'total_bwd_packets']

    df['fwd_packet_bulk_avg'] = df.loc[is_bulk_fwd, 'total_fwd_packets']
    df['bwd_packet_bulk_avg'] = df.loc[is_bulk_bwd, 'total_bwd_packets']

    # Para calcular tasas 'bulk', necesitaríamos definir cómo medir estas tasas en tu contexto específico
    # Por ahora, podemos calcular un promedio de bytes y paquetes en operaciones 'bulk' como un ejemplo
    df['fwd_bulk_rate_avg'] = df['fwd_bytes_bulk_avg'] / df['flow_duration']
    df['bwd_bulk_rate_avg'] = df['bwd_bytes_bulk_avg'] / df['flow_duration']

    # Subflow Fwd/Bwd Packets y Bytes se pueden calcular asumiendo que cada "bulk" representa un subflow.
    # Aquí, simplemente reutilizaremos los totales como una aproximación.
    df['subflow_fwd_packets'] = df['total_fwd_packets']
    df['subflow_fwd_bytes'] = df['total_bytes_toserver']
    df['subflow_bwd_packets'] = df['total_bwd_packets']
    df['subflow_bwd_bytes'] = df['total_bytes_toclient']

    # Inicialización de ventanas y datos activos
    # Los valores para las ventanas iniciales y paquetes de datos activos necesitan definiciones claras.
    # Estos son placeholders para ilustrar cómo podrías expandir las métricas.
    df['fwd_init_win_bytes'] = df['fwd_segment_size_avg']
    df['bwd_init_win_bytes'] = df['bwd_segment_size_avg']

    # Contar paquetes de datos activos como aquellos con la bandera PSH en fwd
    df['fwd_act_data_pkts'] = df['tcp_flag_PSH_count'] * (df['total_fwd_packets'] / df['total_fwd_packets'].sum())

    # Calcular el tamaño mínimo del segmento en envío como el tamaño mínimo del paquete en flujos con datos enviados
    df['fwd_seg_size_min'] = df['min_packet_length'] * (df['total_fwd_packets'] > 0)

    # Llenar los valores faltantes con 0 para mantener la consistencia
    df.fillna(0, inplace=True)

    return df


# Nueva función que agrupa todas las métricas
def calculate_all_metrics(df):
    df = calculate_down_up_ratio(df)
    df = calculate_average_packet_size(df)
    df = calculate_segment_size_avg(df)
    # Incluir llamadas a otras funciones de cálculo aquí
    df = calculate_bulk_stats(df)
    # Agregar más llamadas según sea necesario
    return df

# Ejemplo de cómo se utilizaría este módulo
if __name__ == "__main__":
    # Cargar tu DataFrame aquí, por ejemplo:
    # df = pd.read_csv('tu_archivo_de_datos.csv')
    # df = calculate_all_metrics(df)
    # Ahora df tiene todas las nuevas columnas con las métricas calculadas
    pass
