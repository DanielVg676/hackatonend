"""
Créditos y Agradecimientos:

Este código utiliza el conjunto de datos CICDarknet2022 para fines de análisis de tráfico Darknet. Agradecemos a los autores del conjunto de datos y al siguiente artículo por hacer disponible este recurso para la comunidad de investigación:

Arash Habibi Lashkari, Gurdip Kaur, y Abir Rahali, "DIDarknet: A Contemporary Approach to Detect and Characterize the Darknet Traffic using Deep Image Learning", presentado en la 10th International Conference on Communication and Network Security, Tokio, Japón, noviembre de 2022.

El conjunto de datos CICDarknet2022 está licenciado bajo términos que permiten su redistribución, republicación y espejo en cualquier forma, bajo la condición de incluir la cita apropiada al conjunto de datos y al artículo mencionado.
"""


import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder, MinMaxScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
import os

archivo_csv = 'DarknetFinal.csv'
if not os.path.isfile(archivo_csv):
    raise FileNotFoundError(f"El archivo {archivo_csv} no se encontró.")

try:
    dataframe = pd.read_csv(archivo_csv)
    
    # Reemplaza infinitos por NaNs para una correcta imputación
    dataframe.replace([np.inf, -np.inf], np.nan, inplace=True)

    if 'Label' not in dataframe.columns:
        raise ValueError("La columna 'Label' no se encuentra en el DataFrame.")

    columns_to_drop = ['Label', 'Flow ID', 'Timestamp', 'Src IP', 'Dst IP', 'Label.1']

    y = dataframe['Label']
    X = dataframe.drop(columns=columns_to_drop, errors='ignore')

    y, _ = pd.factorize(y)

    categorical_features = X.select_dtypes(include=['object']).columns.tolist()
    numerical_features = X.select_dtypes(exclude=['object']).columns.tolist()

    categorical_transformer = OneHotEncoder(handle_unknown='ignore')
    numerical_imputer = SimpleImputer(strategy='mean')

    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', categorical_transformer, categorical_features),
            ('num', numerical_imputer, numerical_features)
        ],
        remainder='passthrough')

    # Asegúrate de escalar después de la imputación
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('scaler', StandardScaler(with_mean=False)),  # O usa MinMaxScaler si es más apropiado
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    pipeline.fit(X_train, y_train)

    rf_accuracy = pipeline.score(X_test, y_test)
    print(f"Accuracy of Random Forest: {rf_accuracy}")

except FileNotFoundError as e:
    print(e)
except ValueError as e:
    print(e)
except Exception as e:
    print(f"Se produjo un error inesperado: {e}")
