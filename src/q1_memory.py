from typing import List, Tuple
from datetime import datetime
import json

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
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
  
    # Diccionario para guardar numero de tweets por cada día
    date_counts = {}
    
    with open(file_path, "r") as file:
        # Iterar por cada linea ie. tweet del archivo
        for line in file:
            # Leer un tweet
            tweet = json.loads(line)
            # Conversion de date del tweet de str a datetime.date
            date = datetime.strptime(tweet["date"][:10], "%Y-%m-%d").date()
            # Actualizacion de conteo para el día
            date_counts[date] = date_counts.get(date, 0) + 1
    
    # Obtención de los 10 días con más tweets
    dates_max = sorted(date_counts, key=date_counts.get, reverse=True)[:10]
    
    # Lista de salida
    out = []
    # Iterar por cada uno de los 10 dates máximos
    for date in dates_max:
        # Diccionario para guardar numero de tweets por cada username
        user_counts = {}
        with open(file_path, "r") as file:
            for line in file:
                tweet = json.loads(line)
                # Comparacion de date del tweet actual con el date máximo considerado actualmente
                if datetime.strptime(tweet["date"][:10], "%Y-%m-%d").date() == date:
                    # Extraccion de username
                    username = tweet.get("user",{}).get("username","")
                    # Actualizacion de conteo para el username
                    user_counts[username] = user_counts.get(username, 0) + 1
                    # Agregar tupla con date máxima actual y el username con el conteo mayor para este día
        out.append((date, max(user_counts, key=user_counts.get)))

    return out