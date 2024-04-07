from typing import List, Tuple
import pandas as pd
from datetime import datetime
import polars as pl

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Lee archivo JSON con tweets desde path y devuelve las top 10 fechas donde hay más tweets,
    mencionando el usuario (username) que más publicaciones tiene por cada uno de esos días.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[datetime.date, str]]
        Una lista de tuplas con el dia y el usuario con más tweets en el dia,
        en orden descendente según publicaciones totales en los días.
    """
    
    # Crear LazyFrame para leer el JSON
    lf_tweets = pl.scan_ndjson(file_path, low_memory=True)

    # Selección de columnas a usar
    lf_tweets = lf_tweets.select(
            # Cast "date" a tipo Date de Polars
            pl.col("date").cast(pl.Datetime).cast(pl.Date),
            # Extracción de usernames desde users como columna "username"
            pl.col("user").struct.field("username").alias("username"),
    )

    # Agregar columna "date_count" con el conteo de tweets por día (tweets del mismo día tienen mismo valor en la columna)
    lf_tweets = lf_tweets.with_columns(pl.len().over(["date"]).alias("date_count"))
    # Agregar columna "user_count" con el conteo de tweets por combinacion de día y user (tweets del mismo día y user tienen mismo valor en la columna)
    lf_tweets = lf_tweets.with_columns(pl.len().over(["date", "username"]).alias("user_count"))

    lf_tweets = (lf_tweets
                 # Ordenar por conteo de tweets por username
                 .sort(by=["user_count"], descending=True)
                 # Agrupar por días
                 .group_by(["date"])
                 .agg(
                     # Agregar columna "username_max" con el primer username de cada particion/dia
                     pl.col("username").first().alias("username_max"), 
                     # Agregar columna "date_count" con el date count asociado (conteo de tweets del dia)
                     pl.col("date_count").first().alias("date_count"))
                )

    # Ordenar según el conteo total de tweets de los días y extraer los 10 mayores
    lf_tweets = lf_tweets.sort(by=["date_count"], descending=True).limit(10)

    # Materializar el LazyFrame a DataFrame
    df_tweets = lf_tweets.collect()

    # Eliminar columna "date_count" para preparar output
    df_tweets = df_tweets.drop("date_count")

    # Pasar DataFrame a lista de tuplas
    out = df_tweets.rows()

    return out


def q1_time_pandas(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Lee archivo JSON con tweets desde path y devuelve las top 10 fechas donde hay más tweets,
    mencionando el usuario (username) que más publicaciones tiene por cada uno de esos días.

    Parameters
    ----------
    file_path : str
        Ruta del archivo JSON.

    Returns
    -------
    List[Tuple[datetime.date, str]]
        Una lista de tuplas con el dia y el usuario con más tweets en el dia,
        en orden descendente según publicaciones totales en los días.
    """
    
    # Carga de datos del JSON en Dataframe de Pandas
    df_tweets = pd.read_json(file_path, lines=True)
       
    # Extracción de nombre de usuario
    df_tweets["username"] = df_tweets["user"].apply(lambda x: x.get("username"))

    # Conversión de date a datetime de pandas
    # Se deja solo la fecha para agrupar los días
    df_tweets["date"] = pd.to_datetime(df_tweets["date"]).dt.date

    # Obtención de indices de los 10 días con más tweets
    days_max = df_tweets['date'].value_counts().nlargest(10).index
    # Extracción de los 10 días con más tweets
    df_tweets = df_tweets[df_tweets["date"].isin(days_max)]

    # Conteo de tweets por día y por username (Para cada combinación)
    df_tweets = df_tweets.groupby(["date", "username"]).size().reset_index(name="count")

    # Indices de usernames con más tweets por día
    idx_max = df_tweets.groupby("date")["count"].idxmax()
    # Obtención de usernames con más tweets por día
    usernames_max = df_tweets.loc[idx_max]

    # Eliminar columna "count" para preparar output
    usernames_max = usernames_max.drop("count", axis=1)
    
    # Transformación de Dataframe a lista de tuplas para salida
    out = list(usernames_max.itertuples(index=False, name=None))
    
    return out