import pandas as pd
import numpy as np
import json
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Asumimos que las importaciones de módulos personalizados son correctas
# Asegúrate de manejar las excepciones dentro de estas funciones también
from iat_calculations import calculate_iat_statistics
from tcp_flags_count import count_tcp_flags_vectorized
from packet_length_stats import calculate_packet_length_stats as calculate_basic_packet_length_stats
from tcp_advanced_stats import calculate_tcp_advanced_stats
from packet_direction import add_packet_direction
from packet_stats import calculate_packet_stats
#from packet_length_management import calculate_packet_length_stats as calculate_and_ensure_packet_length_stats

from packet_stats_calculations import calculate_basic_packet_stats, ensure_and_calculate_packet_stats


from data_cleaning import clean_data

def preprocesar_datos_y_ajustar_columnas(df_preprocesado, numeric_features_updated, categorical_features_updated):
    # Eliminar de las listas las columnas que no están presentes en el DataFrame
    numeric_features_present = [col for col in numeric_features_updated if col in df_preprocesado.columns]
    categorical_features_present = [col for col in categorical_features_updated if col in df_preprocesado.columns]

    try:
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features_present),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features_present)
            ],
            remainder='drop'  # Descarta las columnas no especificadas
        )
        
        X_preprocessed = preprocessor.fit_transform(df_preprocesado)
        
        columns_transformed = (
            preprocessor.named_transformers_['num'].get_feature_names_out(numeric_features_present).tolist() +
            preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_features_present).tolist()
        )
        
        df_preprocessed = pd.DataFrame(X_preprocessed, columns=columns_transformed)
        
    except Exception as e:
        logging.error(f"Error inesperado durante el preprocesamiento: {e}")
        raise

    return df_preprocessed


def verify_data_cleanliness(df, numeric_features):
    try:
        if df.empty:
            print("El DataFrame está vacío. No se puede verificar la limpieza.")
            return
        
        missing_features = [feature for feature in numeric_features if feature not in df.columns]
        if missing_features:
            print(f"Advertencia: Las siguientes características numéricas no se encontraron en el DataFrame: {missing_features}")
            # Ajusta la lista de características numéricas para incluir solo las que están presentes
            numeric_features = [feature for feature in numeric_features if feature in df.columns]
        
        print("Verificación de limpieza de datos:")
        
        # Conteo de valores NaN en el DataFrame
        nan_counts = df.isna().sum()
        print("\nConteo de NaN por columna:\n", nan_counts[nan_counts > 0])  # Muestra solo columnas con NaN
        
        # Conteo de valores infinitos en columnas numéricas
        inf_counts = np.isinf(df.select_dtypes(include=['float64', 'int64'])).sum()
        print("\nConteo de infinitos por columna:\n", inf_counts[inf_counts > 0])  # Muestra solo columnas con infinitos
        
        if numeric_features:
            print("\nResumen estadístico de características numéricas:")
            print(df[numeric_features].describe())
        else:
            print("No hay características numéricas especificadas o presentes para el resumen estadístico.")
        
    except KeyError as ke:
        logging.error(f"Error de clave al verificar la limpieza de datos: {ke}")
    except Exception as e:
        logging.error(f"Error inesperado al verificar la limpieza de datos: {e}")


def review_transformed_data(df_preprocessed):
    try:
        print("\nRevisión de los datos transformados/preprocesados:")
        print(df_preprocessed.head())
        print("\nResumen estadístico de las características preprocesadas:")
        print(df_preprocessed.describe())
    except Exception as e:
        logging.error(f"Error al revisar datos transformados: {e}")

def preprocesar_datos(df):
    try:
        logging.info("Calculando estadísticas IAT...")
        iat_stats = calculate_iat_statistics(df)
        df = df.merge(iat_stats, on='flow_id', how='left')
        logging.info(f"DataFrame después de calcular estadísticas IAT y hacer merge: {df.columns}")

        try:
            logging.info("Procesando banderas TCP...")
            df['tcp.flags'] = df.get('tcp.flags', pd.Series([None] * df.shape[0]))
            df = count_tcp_flags_vectorized(df)
            logging.info(f"DataFrame después de procesar banderas TCP: {df.columns}")
        except Exception as e:
            logging.critical(f"Fallo crítico durante el procesamiento de banderas TCP: {e}")
            raise SystemExit("Fallo crítico durante el procesamiento de datos.")

        logging.info("Convirtiendo timestamp a UNIX time...")
        df['timestamp'] = pd.to_datetime(df['timestamp']).astype('int64') // 10**9
        logging.info(f"DataFrame después de convertir timestamp a UNIX time: {df.columns}")

        logging.info("Calculando duración de flujo y otros totales...")
        grouped = df.groupby('flow_id')
        df['flow_duration'] = grouped['timestamp'].transform('max') - grouped['timestamp'].transform('min')
        df['total_fwd_packets'] = grouped['flow.pkts_toserver'].transform('sum')
        df['total_bwd_packets'] = grouped['flow.pkts_toclient'].transform('sum')
        df['total_bytes_toserver'] = grouped['flow.bytes_toserver'].transform('sum')
        df['total_bytes_toclient'] = grouped['flow.bytes_toclient'].transform('sum')
        logging.info(f"DataFrame después de calcular duración de flujo y otros totales: {df.columns}")

        logging.info("Asegurando y calculando estadísticas básicas de longitud de paquete...")
        df = ensure_and_calculate_packet_stats(df)
        logging.info(f"DataFrame después de asegurar y calcular estadísticas básicas de longitud de paquete: {df.columns}")


        logging.info("Calculando estadísticas de longitud de paquete...")
        packet_stats = calculate_basic_packet_stats(df)

        if not packet_stats.empty:
            # Debugging: imprimir las columnas de ambos DataFrames antes del merge
            logging.debug(f"Columnas en df antes del merge: {df.columns.tolist()}")
            logging.debug(f"Columnas en packet_stats antes del merge: {packet_stats.columns.tolist()}")

            # Preparar una lista de columnas en packet_stats que ya existen en df
            overlapping_columns = [col for col in packet_stats.columns if col in df.columns and col != 'flow_id']
            if overlapping_columns:
                # Añadir sufijos únicamente a las columnas en packet_stats que se solapan, excepto 'flow_id'
                rename_mapping = {col: f"{col}_stats" for col in overlapping_columns}
                packet_stats.rename(columns=rename_mapping, inplace=True)
                logging.debug(f"Columnas en packet_stats después de renombrar para evitar solapamientos: {packet_stats.columns.tolist()}")

            # Realizar el merge
            df = df.merge(packet_stats, on='flow_id', how='left')
            logging.info("Estadísticas de longitud de paquete calculadas y fusionadas con éxito.")
        else:
            logging.warning("La función calculate_basic_packet_length_stats devolvió un DataFrame vacío.")

        # Debugging: imprimir las columnas de df después del merge para verificar cambios
        logging.info(f"DataFrame después de añadir estadísticas de longitud de paquete: {df.columns}")


        logging.info("Calculando estadísticas avanzadas de TCP y dirección de paquete...")
        # Aquí imprimimos el DataFrame antes de pasarlo a calculate_tcp_advanced_stats
        logging.info(f"DataFrame antes de calcular estadísticas avanzadas de TCP: {df.columns}")
        df = calculate_tcp_advanced_stats(df)
        logging.info(f"DataFrame después de calcular estadísticas avanzadas de TCP: {df.columns}")

        # Agregar la dirección de los paquetes al DataFrame
        df = add_packet_direction(df)

        # Calcular las estadísticas de los paquetes
        packet_stats_df = calculate_packet_stats(df)

        # Realizar el merge, utilizando sufijos explícitos para manejar posibles columnas duplicadas
        # Esto te ayuda a identificar y diferenciar las columnas de cada DataFrame original después del merge
        df_merged = pd.merge(df, packet_stats_df, on='flow_id', how='left', suffixes=('', '_stats'))


        return df_merged

    except Exception as e:
        logging.error(f"Error durante el preprocesamiento de datos: {e}")
        raise


try:
    # Intenta cargar los datos, captura y maneja errores comunes
    with open('../../../var/log/suricata/eve.json', 'r') as file:
        lines = file.readlines()
        data = [json.loads(line) for line in lines if json.loads(line).get('event_type', '') in ['flow', 'http', 'dns', 'tls']]
    df = pd.json_normalize(data)
    print(df.head())
except FileNotFoundError:
    logging.error("Archivo JSON no encontrado.")
    raise SystemExit("Fallo crítico: Archivo de datos no encontrado.")
except json.JSONDecodeError:
    logging.error("Error al decodificar el archivo JSON.")
    raise SystemExit("Fallo crítico: Formato de archivo JSON inválido.")
except Exception as e:
    logging.error(f"Error inesperado al cargar datos: {e}")
    raise SystemExit("Fallo crítico: Error inesperado al cargar datos.")

# Preprocesamiento de datos con manejo de errores
try:
    df_preprocesado = preprocesar_datos(df)
except Exception as e:
    logging.critical("Fallo crítico durante el preprocesamiento. El programa terminará.")
    raise SystemExit(e)

# Selección y definición de características
# Actualización de características numéricas basadas en los pasos de preprocesamiento observados
numeric_features_updated = [
    'src_port', 'dest_port', 'flow.pkts_toserver', 'flow.pkts_toclient', 
    'flow.bytes_toserver', 'flow.bytes_toclient', 'flow_duration', 
    'total_fwd_packets', 'total_bwd_packets', 'flow_iat_mean', 'flow_iat_std', 
    'flow_iat_max', 'flow_iat_min', 'total_fwd_length', 'max_fwd_length', 
    'min_fwd_length', 'mean_fwd_length', 'std_fwd_length', 'total_bwd_length', 
    'max_bwd_length', 'min_bwd_length', 'mean_bwd_length', 'std_bwd_length', 
    'min_packet_length', 'max_packet_length', 'mean_packet_length', 
    'std_packet_length', 'var_packet_length', 'fwd_psh_flags', 'bwd_psh_flags', 
    'fwd_urg_flags', 'bwd_urg_flags', 'active_mean', 'active_std', 'active_max', 
    'active_min', 'idle_total', 'tcp_flag_FIN_count', 'tcp_flag_SYN_count', 
    'tcp_flag_RST_count', 'tcp_flag_PSH_count', 'tcp_flag_ACK_count', 
    'tcp_flag_URG_count', 'tcp_flag_ECE_count', 'tcp_flag_CWR_count'
]

# Verificación y posible actualización de características categóricas
categorical_features_updated = [
    'proto', 'tcp.flags', 'direction'  # Asumiendo que 'tcp.flags' es relevante y 'direction' fue calculada o relevante
]


# Función de limpieza de datos actualizada para incluir características actualizadas
clean_data(df_preprocesado, numeric_features_updated, categorical_features_updated)

df_preprocesado = preprocesar_datos_y_ajustar_columnas(df_preprocesado, numeric_features_updated, categorical_features_updated)


# Verificación de la limpieza de los datos
""" verify_data_cleanliness(df_preprocesado, numeric_features_updated)




try:
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features_updated),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features_updated)
        ],
        remainder='drop'  # Descarta las columnas no especificadas
    )
    
    # Verificar si todas las columnas especificadas existen en el DataFrame
    missing_num = [col for col in numeric_features_updated if col not in df_preprocesado.columns]
    missing_cat = [col for col in categorical_features_updated if col not in df_preprocesado.columns]
    
    if missing_num or missing_cat:
        raise ValueError(f"Columnas numéricas faltantes: {missing_num}, Columnas categóricas faltantes: {missing_cat}")
    
    X_preprocessed = preprocessor.fit_transform(df_preprocesado)
    
    columns_transformed = (
        preprocessor.named_transformers_['num'].get_feature_names_out(numeric_features_updated).tolist() +
        preprocessor.named_transformers_['cat'].get_feature_names_out(categorical_features_updated).tolist()
    )
    
    df_preprocessed = pd.DataFrame(X_preprocessed, columns=columns_transformed)
    
except ValueError as ve:
    logging.error(f"Error de valor durante el preprocesamiento: {ve}")
    raise SystemExit("Fallo crítico durante el preprocesamiento.")
except Exception as e:
    logging.error(f"Error inesperado durante el preprocesamiento: {e}")
    raise SystemExit("Fallo crítico inesperado durante el preprocesamiento.")
 """
# Revisión de los datos transformados/preprocesados
try:
    review_transformed_data(df_preprocesado)
    print("\nTotal de columnas obtenidas al final:", len(df_preprocesado.columns))
    print("\nColumnas obtenidas al final:\n", df_preprocesado.columns.tolist())
    
    
    # Información del DataFrame
    print("\nInformación del DataFrame al final del preprocesamiento:")
    print(df_preprocesado.info())

    # Estadísticas descriptivas
    print(df_preprocesado.describe())

    # Asegúrate de que 'df_preprocesado' sea el DataFrame final tras el preprocesamiento
    print("\nResumen estadístico enfocado en la duración del flujo y otras columnas seleccionadas:")
    print(df_preprocesado[['src_port', 'dest_port', 'flow.pkts_toserver', 'flow.pkts_toclient', 'flow_duration']].describe())


    # Asegúrate de reemplazar `df_preprocesado` con el nombre correcto de tu DataFrame final.

except Exception as e:
    logging.error(f"Error durante la revisión de los datos transformados/preprocesados: {e}")
    # Este error podría no ser crítico pero requiere revisión