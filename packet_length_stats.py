import pandas as pd
import logging

# Configuración inicial del logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_packet_length_stats(df):
    try:
        if df.empty:
            logging.warning("El DataFrame de entrada está vacío. Retornando un DataFrame vacío.")
            return pd.DataFrame()

        # Verifica la presencia de columnas requeridas
        required_columns = {'flow_id', 'packet_length'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            logging.error(f"El DataFrame no contiene las columnas requeridas: {missing_columns}")
            raise ValueError(f"El DataFrame no contiene las columnas requeridas: {missing_columns}")

        if not pd.api.types.is_numeric_dtype(df['packet_length']):
            logging.error("La columna 'packet_length' debe ser de tipo numérico.")
            raise TypeError("La columna 'packet_length' debe ser de tipo numérico.")

        # Calcula estadísticas de longitud de paquete
        logging.debug("Realizando agregación para las estadísticas de longitud de paquete.")
        packet_length_stats = df.groupby('flow_id')['packet_length'].agg(['mean', 'max', 'min', 'std']).reset_index()
        packet_length_stats['std'] = packet_length_stats['std'].fillna(0)  # Asegurar que no haya NaNs
        
        # Renombrar columnas de estadísticas para claridad y evitar conflictos
        new_column_names = {
            'mean': 'mean_packet_length_stats', 
            'max': 'max_packet_length_stats', 
            'min': 'min_packet_length_stats', 
            'std': 'std_packet_length_stats'
        }
        packet_length_stats.rename(columns=new_column_names, inplace=True)

        logging.debug(f"Estadísticas de longitud de paquete calculadas: {packet_length_stats.head()}")

        # Merge las estadísticas calculadas con el DataFrame original
        df = pd.merge(df, packet_length_stats, on='flow_id', how='left')

        logging.info(f"DataFrame después de la fusión: {df.columns.tolist()}")

        # Calcula varianza de longitud de paquete como nueva columna
        df['var_packet_length_stats'] = df['std_packet_length_stats'] ** 2

        logging.info("Estadísticas de longitud de paquete calculadas y fusionadas correctamente.")
        return df

    except KeyError as ke:
        logging.error(f"Error de clave: {ke}")
        return pd.DataFrame()
    except ValueError as ve:
        logging.error(f"Error de valor: {ve}")
        return pd.DataFrame()
    except TypeError as te:
        logging.error(f"Error de tipo: {te}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        return pd.DataFrame()
