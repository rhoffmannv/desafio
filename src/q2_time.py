from typing import List, Tuple
import pandas as pd
import emoji
from collections import Counter

def q2_time(file_path: str) -> List[Tuple[str, int]]:

    # Carga de datos del JSON en Dataframe de Pandas
    df = pd.read_json(file_path, lines=True)

    # Contador para emojis
    counter_emojis = Counter()
    
    # Iterando en columna "content"
    for content in df['content']:
        # Obtención de emojis en el contenido actual
        # Es una lista de diccionarios por cada emoji de la sgte forma: {'match_start': 262, 'match_end': 263, 'emoji': '🚜'}
        emojis_match = emoji.emoji_list(content)
        # Agregar solo 'emoji' a lista
        emojis_list = [match['emoji'] for match in emojis_match]
        # Actualizar counter de emojis
        counter_emojis.update(emojis_list)

    # Extraccion de los 10 emojis mas usados
    out = counter_emojis.most_common(10)
    
    return out