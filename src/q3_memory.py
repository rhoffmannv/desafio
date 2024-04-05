from typing import List, Tuple
import json
from collections import Counter

def q3_memory(file_path: str) -> List[Tuple[str, int]]:

    # Contador para usernames mencionados
    counter_mentioned = Counter()
    
    with open(file_path, "r") as file:
        # Leer linea a linea ie. tweet por tweet
        for line in file:
            # Leer tweet
            tweet = json.loads(line)
            # Extracción de "mentioned users"
            mentioned = tweet.get('mentionedUsers')

            if mentioned != None:
                # Por cada user mencionado en el tweet
                for user in mentioned:
                    # Actualizar counter de menciones
                    counter_mentioned[user['username']] += 1

    # Extracción de los 10 usernames mas mencionados
    out = counter_mentioned.most_common(10)

    return out