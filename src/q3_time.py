from typing import List, Tuple, Dict
import pandas as pd
import polars as pl


def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Lee archivo JSON con tweets desde path y devuelve el top 10 histórico de usuarios (username)
    más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[str, int]]
        Una lista de tuplas con el username y las veces que fue mencionado,
        en orden descendente según cantidad de menciones.
    """
    
    # Crear LazyFrame para leer el JSON
    lf_tweets = pl.scan_ndjson(file_path)
    
    # Selección de columnas a usar
    lf_tweets = lf_tweets.select(
        # Extraccion de lista de usernames de users mencionados de cada tweet
        pl.col("mentionedUsers").map_batches(extract_usernames).alias("mentioned_list")
    ).filter(
        # Filtrado de tweets sin menciones
        pl.col("mentioned_list").list.len() > 0
    )
    
    # Materialización del LazyFrame a DataFrame
    df_mentioned = lf_tweets.collect()

    # "Abrir" filas creando una fila nueva por cada username mencionado en la lista (Se renombra columna a "mentioned")
    df_mentioned = df_mentioned.get_column("mentioned_list").explode().alias("mentioned")

    # Generación de conteo por cada username mencionado ordenado de mayor a menor y extracción de los 10 mas mencionados
    mentioned_max = df_mentioned.value_counts(sort=True)[0:10]

    # Se crea lista de tuplas de salida
    out = list(zip(mentioned_max["mentioned"], mentioned_max["count"]))
    
    return out



def q3_time_pandas(file_path: str) -> List[Tuple[str, int]]:
    """
    Lee archivo JSON con tweets desde path y devuelve el top 10 histórico de usuarios (username)
    más influyentes en función del conteo de las menciones (@) que registra cada uno de ellos.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[str, int]]
        Una lista de tuplas con el username y las veces que fue mencionado,
        en orden descendente según cantidad de menciones.
    """

    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)

    # Extracción de columna "mentionedUsers"
    df_mentioned = df['mentionedUsers']
    # Eliminar nulos
    df_mentioned = df_mentioned.dropna()

    # Aplicación de función para extraer usernames
    df_mentioned = df_mentioned.apply(get_usernames_list)
    # "Abrir" cada fila, creando una fila nueva por cada username mencionado en la lista
    df_mentioned = df_mentioned.explode()

    # Contar y ordenar por apariciones de cada username y extraer los 10 mayores
    mentioned_max = df_mentioned.value_counts()[0:10]

    # Ordenar serie de pandas como lista de tuplas
    out = list(zip(mentioned_max, mentioned_max.index))

    return out



def get_usernames_list(users_list: List[Dict[str, str]]) -> List[str]:
    """
    Extrae la key "username" de cada elemento de una lista de "users".

    Parameters
    ----------
    users_list : List[Dict[str, str]]
        Lista con diccionario de datos por cada user.

    Returns
    -------
    List[str]
        Lista con el "username" de cada "user".
    """

    # Verificar que la lista no es None
    if users_list is not None:
        # Se crea una lista extrayendo solo el "username" del diccionario de cada user
        return [user["username"] for user in users_list]
    # Si es None devolver lista vacia
    return []

    
def extract_usernames(batch: pl.DataFrame) -> pl.Series:
    """
    Extrae la key "username" de cada elemento de un Dataframe.

    Parameters
    ----------
    batch : pl.DataFrame
        Dataframe con listas de "users" (diccionario) en cada elemento.

    Returns
    -------
    pl.Series
        Serie de Polars con lista de "username" en cada elemento.
    """

    # Se aplica función "get_usernames_list" a cada item dentro del batch
    usernames_lists = [get_usernames_list(x) for x in batch]
    # Se devuelve la lista como una serie de Polars 
    return pl.Series(usernames_lists)


