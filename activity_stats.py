
import pandas as pd


def calculate_activity_stats(df):
    # Supongamos que 'flow_iat_mean', 'flow_iat_std', 'flow_iat_max', 'flow_iat_min' representan
    # estadísticas generales del tiempo entre llegadas (IAT) para cada flujo.
    
    # Calcular estadísticas de períodos activos basados en IAT.
    # Un enfoque simplificado es considerar períodos con tiempos IAT por debajo de un umbral como "activos".
    # Aquí, se utilizan las estadísticas IAT existentes como proxy para la actividad.
    df['active_mean'] = df['flow_iat_mean']
    df['active_std'] = df['flow_iat_std']
    df['active_max'] = df['flow_iat_max']
    df['active_min'] = df['flow_iat_min']
    
    # Para calcular estadísticas de inactividad, se podrían necesitar marcas de tiempo específicas de inicio/final
    # de actividad, o identificar grandes IAT como períodos inactivos.
    # Como simplificación, se podría calcular un promedio de los mayores IAT como inactividad.
    # NOTA: Este método es muy simplificado y podría no reflejar precisamente la inactividad real sin datos más específicos.
    idle_threshold = df['flow_iat_mean'] + df['flow_iat_std']  # Definir umbral de inactividad como ejemplo
    df['idle_mean'] = idle_threshold
    df['idle_std'] = df['flow_iat_std']  # Esta es una simplificación; idealmente se calcularía basado en periodos inactivos reales
    df['idle_max'] = df['flow_iat_max']  # Suponer que el IAT más largo puede ser un buen indicador del máximo periodo inactivo
    df['idle_min'] = df['flow_iat_min']  # Suponer que el IAT más corto por encima del umbral indica el mínimo periodo inactivo
    
    return df
