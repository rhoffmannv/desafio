from typing import List, Tuple
import pandas as pd
import emoji
from collections import Counter
import polars as pl


def q2_time(file_path: str) -> List[Tuple[str, int]]:    
    """
    Lee archivo JSON con tweets desde path y devuelve los top 10 emojis más usados,
    con su respectivo conteo.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[str, int]]
        Una lista de tuplas con el emoji y las veces que fue utilizado,
        en orden descendente según cantidad de apariciones.
    """
    
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
    """
    Lee archivo JSON con tweets desde path y devuelve los top 10 emojis más usados,
    con su respectivo conteo.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[str, int]]
        Una lista de tuplas con el emoji y las veces que fue utilizado,
        en orden descendente según cantidad de apariciones.
    """
    
    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)

    # Extracción de columna content
    df_content = df['content']

    # Aplicación de función para extraer lista de emojis de cada content
    df_emojis = df_content.apply(get_emojis_list)
    # "Abrir" cada fila, creando una fila nueva por cada emoji en la lista
    df_emojis = df_emojis.explode()

    # Contar y ordenar por apariciones de cada emoji y extraer los 10 mayores
    emojis_max = df_emojis.value_counts()[0:10]

    # Ordenar serie de pandas como lista de tuplas
    out = list(zip(emojis_max, emojis_max.index))
    
    return out



def get_emojis_list(content: str) -> List[str]:
    """
    Extrae emojis que aparecen en el texto.

    Parameters
    ----------
    content : str
        String con posibles emojis.

    Returns
    -------
    List[str]
        Lista con emojis que aparecen en el contenido.
    """
    
    # Obtención de lista de diccionarios por cada emoji de la forma: {'match_start': 262, 'match_end': 263, 'emoji': '🚜'}
    emojis_match = emoji.emoji_list(content)
    # Agregar solo 'emoji' a lista
    emojis_list = [match['emoji'] for match in emojis_match]
    
    return emojis_list


def extract_emojis_list(batch: pl.DataFrame) -> pl.Series:
    """
    Extrae lista de emojis en cada elemento de un Dataframe.

    Parameters
    ----------
    batch : pl.DataFrame
        Dataframe con texto en cada elemento.

    Returns
    -------
    pl.Series
        Serie de Polars con lista de emojis en cada elemento.
    """
    
    # Se aplica función "get_emojis_list" a cada item dentro del batch
    emojis_lists = [get_emojis_list(x) for x in batch]
    # Se devuelve la lista como una serie de Polars 
    return pl.Series(emojis_lists)