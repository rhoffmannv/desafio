from typing import List, Tuple
import pandas as pd
import emoji
from collections import Counter
import polars as pl


def q2_time(file_path: str) -> List[Tuple[str, int]]:    

    # Crear LazyFrame para leer el JSON
    lf_emojis = pl.scan_ndjson(file_path)
    
    # Selección de columnas a usar
    lf_emojis = lf_emojis.select(
        # Extraccion de lista de emojis de cada tweet
        pl.col("content").map_batches(extract_emojis_list).alias("emojis_lists")
    ).filter(
        # Filtrado de tweets sin emojis
        pl.col("emojis_lists").list.len() > 0
    )
    
    # Materialización del LazyFrame a DataFrame
    df_emojis_lists = lf_emojis.collect()
    
    # "Abrir" filas creando una fila nueva por cada emoji en la lista (Se renombra column a "emojis")
    df_emojis = df_emojis_lists.get_column("emojis_lists").explode().alias("emojis")

    # Se genera dataframe con conteo por cada emoji ordenado de mayor a menor y se extraen los 10 emojis con mayor conteo
    emojis_counts = df_emojis.value_counts(sort=True)[0:10]

    # Se crea lista de tuplas de salida
    out = list(zip(emojis_counts["emojis"], emojis_counts["count"]))
    
    return out



def q2_time_pandas(file_path: str) -> List[Tuple[str, int]]:

    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)

    # Contador para emojis
    counter_emojis = Counter()
    
    # Iterando en columna "content"
    for content in df['content']:
        # Obtención de emojis en el contenido actual
        emojis_list = get_emojis_list(content)
        # Actualizar counter de emojis
        counter_emojis.update(emojis_list)

    # Extraccion de los 10 emojis mas usados
    out = counter_emojis.most_common(10)
    
    return out



def get_emojis_list(content):
    # Obtención de emojis en el contenido actual
    # Es una lista de diccionarios por cada emoji de la forma: {'match_start': 262, 'match_end': 263, 'emoji': '🚜'}
    emojis_match = emoji.emoji_list(content)
    # Agregar solo 'emoji' a lista
    emojis_list = [match['emoji'] for match in emojis_match]
    
    return emojis_list


def extract_emojis_list(batch):
    # Se aplica función "get_emojis_list" a cada item dentro del batch
    emojis_lists = [get_emojis_list(x) for x in batch]
    # Se devuelve la lista como una serie de Polars 
    return pl.Series(emojis_lists)