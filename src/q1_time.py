from typing import List, Tuple
import pandas as pd
from datetime import datetime

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)
       
    # Extracción de nombre de usuario
    df['username'] = df['user'].apply(lambda x: x.get('username'))

    # Conversión de date a datetime de pandas
    # Se deja solo la fecha para agrupar los días
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Obtención de indices de los 10 días con más tweets
    top_10_days = df['date'].value_counts().nlargest(10).index
    # Extracción de los 10 días con más tweets
    tweets_data = df[df['date'].isin(top_10_days)]

    # Conteo de tweets por día y por username (Para cada combinación)
    tweets_data = tweets_data.groupby(['date', 'username']).size().reset_index(name='count')

    # Indices de usernames con más tweets por día
    idx = tweets_data.groupby('date')['count'].idxmax()
    # Obtención de usernames con más tweets por día
    tweets_data = tweets_data.loc[idx]
    
    # Transformación de Dataframe a lista de tuplas para salida
    result = list(tweets_data.itertuples(index=False, name=None))
    
    return result