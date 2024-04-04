import pandas as pd
import numpy as np
import logging

def convert_tcp_flags_to_numeric(df):
    if 'tcp.flags' in df.columns:
        df['tcp.flags'] = pd.to_numeric(df['tcp.flags'].apply(lambda x: int(x, 16) if pd.notnull(x) else np.nan), errors='coerce')
        df['tcp.flags'] = df['tcp.flags'].fillna(0).astype(int)
    else:
        logging.error("'tcp.flags' no se encuentra en el DataFrame.")
    return df



def count_tcp_flags_vectorized(df):
    try:
        if 'tcp.flags' not in df.columns:
            raise ValueError("El DataFrame no contiene la columna requerida 'tcp.flags'.")
        
        # Intenta convertir 'tcp.flags' a numérico, en caso de ser necesario
        df = convert_tcp_flags_to_numeric(df)
        
        if not np.issubdtype(df['tcp.flags'].dtype, np.number):
            raise TypeError("Incluso después de la conversión, la columna 'tcp.flags' no es de tipo numérico.")

        flags = {
            'FIN': 0x01,
            'SYN': 0x02,
            'RST': 0x04,
            'PSH': 0x08,
            'ACK': 0x10,
            'URG': 0x20,
            'ECE': 0x40,
            'CWR': 0x80,
        }

        for flag, value in flags.items():
            df[f'tcp_flag_{flag}_count'] = ((df['tcp.flags'] & value) > 0).astype(int)

        return df

    except ValueError as ve:
        print(f"Error de valor: {ve}")
        return df
    except TypeError as te:
        print(f"Error de tipo: {te}")
        return df
    except Exception as e:
        print(f"Error inesperado al contar las banderas TCP: {e}")
        return df
