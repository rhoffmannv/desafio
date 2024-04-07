from typing import List, Tuple
from datetime import datetime
from google.cloud import storage, bigquery

def q1_bigquery(keyfile_path: str, project_id: str, dataset_id: str, table_name: str) -> List[Tuple[datetime.date, str]]:
    """
    Lee archivo JSON con tweets desde path y devuelve las top 10 fechas donde hay más tweets,
    mencionando el usuario (username) que más publicaciones tiene por cada uno de esos días.

    Parameters
    ----------
    keyfile_path : str
        Ruta del archivo JSON para autenticar la cuenta de servicio
    project_id : str
        La ID del proyecto en Google Cloud Platform
    dataset_id : str
        La ID del dataset en BigQuery
    table_name : str
        El nombre de la tabla dentro del dataset en BigQuery con los datos de los tweets

    Returns
    -------
    List[Tuple[datetime.date, str]]
        Una lista de tuplas con el dia y el usuario con más tweets en el dia,
        en orden descendente según publicaciones totales en los días.
    """

    # Conectarse a BigQuery usando keyfile
    client = bigquery.Client.from_service_account_json(keyfile_path, project=project_id)

    # Se seleccionan de columnas a usar, tomando date y username (desde dentro de user)
    # Se agrega columna date_count con conteo de tweets por día (usando lógica over similar a funciones de venta en SQL).
    # Se agrega columna user_count con conteo tweets por combinacion de día y username (usando lógica over).
    # Se ordena según user_count, se agrupa según date y se extrae el primer username por cada partición, junto con date_count asociada.
    # Se ordena el resultado por date_count descendiente y se extraen los primeros 10.
    query = """
    SELECT date, username FROM (
        SELECT  date,
                username,
                date_count,
                row_number() OVER (partition BY date ORDER BY user_count DESC) AS user_position
                FROM (
                    SELECT USER.username,
                            Cast(date AS DATE) AS date,
                            Count(*) OVER (partition BY Cast(date AS DATE)) AS date_count,
                            Count(*) OVER (partition BY Cast(date AS DATE), USER.username) AS user_count
                    FROM   `{project_id}.{dataset_id}.{table_name}`
                )
    )
    WHERE user_position = 1
    ORDER BY date_count DESC
    limit 10
    """

    # Se reemplaza el project_id, dataset_id y table_name según los argumentos entregados a la función
    query = query.replace("{project_id}", project_id).replace("{dataset_id}", dataset_id).replace("{table_name}", table_name)

    # Se crea un BigQuery Job para ejecutar la query
    job = client.query(query)

	# Convierte salida a lista de tuplas
    out = [tuple(row.values()) for row in job.result()]

    return out



def upload_file_to_cloud_storage(keyfile_path: str, project_id: str, bucket_name: str, file_name: str):
    """
    Sube un archivo a un Bucket en Google Cloud Platform

    Parameters
    ----------
    keyfile_path : str
        Ruta del archivo JSON para autenticar la cuenta de servicio
    project_id : str
        La ID del proyecto en Google Cloud Platform
    bucket_name : str
        La ID del dataset en BigQuery
    file_name : str
        El nombre del archivo a subir
        Debe estar en la misma carpeta que el script que llama a esta función 
    """
    
    # Conectarse a Cloud Storage usando keyfile
    storage_client = storage.Client.from_service_account_json(keyfile_path, project=project_id)

    # Bucket dentro de Cloud Storage
    bucket = storage_client.bucket(bucket_name)

    # Creación de blob con el nombre del archivo
    blob = bucket.blob(file_name)

    # Subir el archivo a Bucket de Google Cloud Storage
    blob.upload_from_filename(file_name)

    return