from typing import List, Tuple
import pandas as pd

def extract_usernames(user_list):
    # user_list corresponde a una lista de diccionarios
    # Se crea una lista extrayendo solo el "username" de cada diccionario
    out = [user['username'] for user in user_list]
    
    return out

def q3_time(file_path: str) -> List[Tuple[str, int]]:

    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)

    # Extracción de columna "mentionedUsers"
    df_mentioned = df['mentionedUsers']
    # Eliminar nulos
    df_mentioned = df_mentioned.dropna()

    # Aplicación de función para extraer usernames
    df_mentioned = df_mentioned.apply(extract_usernames)
    # "Abrir" cada fila, creando una fila nueva por cada elemento en la lista (valor de la fila)
    df_mentioned = df_mentioned.explode()

    # Contar y ordenar por apariciones de cada username y extraer los 10 mayores
    mentioned_max = df_mentioned.value_counts()[0:10]

    # Ordenar serie de pandas como lista de tuplas
    out = list(zip(mentioned_max, mentioned_max.index))

    return out