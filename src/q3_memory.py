from typing import List, Tuple
import json
from collections import Counter

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
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

    # Contador para usernames mencionados
    counter_mentioned = Counter()
    
    with open(file_path, "r") as file:
        # Leer linea a linea ie. tweet por tweet
        for line in file:
            # Leer tweet
            tweet = json.loads(line)
            # Extracción de "mentioned users"
            mentioned = tweet.get('mentionedUsers')

            if mentioned is not None:
                # Por cada user mencionado en el tweet
                for user in mentioned:
                    # Actualizar counter de menciones
                    counter_mentioned[user['username']] += 1

    # Extracción de los 10 usernames mas mencionados
    out = counter_mentioned.most_common(10)

    return out