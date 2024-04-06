from typing import List, Tuple
import json
import emoji
from collections import Counter

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
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
    
    # Contador para emojis
    counter_emojis = Counter()
  
    with open(file_path, "r") as file:
        # Iterar por cada linea ie. tweet del archivo
        for line in file:
            # Leer tweet
            tweet = json.loads(line)
            # Extracción de "content"
            content = tweet.get("content")
            # Obtención de emojis en el contenido actual
            # Es una lista de diccionarios por cada emoji de la forma: {'match_start': 262, 'match_end': 263, 'emoji': '🚜'}
            emojis_match = emoji.emoji_list(content)
            # Agregar solo "emoji" a lista
            emojis_list = [match['emoji'] for match in emojis_match]
            # Actualizar counter de emojis
            counter_emojis.update(emojis_list)
            
    # Extraccion de los 10 emojis mas usados
    out = counter_emojis.most_common(10)
    
    return out