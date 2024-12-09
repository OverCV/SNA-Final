import os
import pandas as pd
import sqlite3
import networkx as nx
from validator import DatasetValidator
from snbuilder import SocialNetworkBuilder
import pickle


def convert_tf_to_bool(value):
    return True if value == 't' else False if value == 'f' else None


def process_facebook_data(db_path):
    conn = sqlite3.connect(db_path)
    facebook_users = pd.read_sql(
        """
        SELECT DISTINCT 
            id as Nodo,
            'Usuario' as Tipo_de_Nodo,
            'Facebook' as Plataforma,
            'N/A' as Estructura,
            name as Autor,
            NULL as Fecha,
            NULL as Contenido
        FROM member
        """,
        conn,
    )

    facebook_posts = pd.read_sql(
        """
        SELECT 
            'capfb' || pid as Nodo,
            'Captura' as Tipo_de_Nodo,
            'Facebook' as Plataforma,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM comment 
                    WHERE comment.pid = post.pid
                ) THEN 'Reply'
                ELSE 'Status'
            END as Estructura,
            name as Autor,
            timeStamp as Fecha,
            msg as Contenido
        FROM post
        """,
        conn,
    )

    conn.close()
    return pd.concat([facebook_users, facebook_posts], ignore_index=True)


def process_truth_social_data(base_path):
    users = pd.read_csv(f'{base_path}/users.tsv', sep='\t', low_memory=False, on_bad_lines='skip')
    truths = pd.read_csv(
        f'{base_path}/truths.tsv', sep='\t', low_memory=False, on_bad_lines='skip', quoting=3
    )

    truths['is_reply'] = truths['is_reply'].apply(convert_tf_to_bool)
    truths['is_retruth'] = truths['is_retruth'].apply(convert_tf_to_bool)

    ts_users = pd.DataFrame(
        {
            'Nodo': users['username'].apply(lambda x: f'@{x}'),
            'Tipo_de_Nodo': 'Usuario',
            'Plataforma': 'Truth Social',
            'Estructura': 'N/A',
            'Autor': users['username'],
            'Fecha': pd.to_datetime(users['timestamp'], errors='coerce'),
            'Contenido': None,
        }
    )

    user_mapping = users.set_index('id')['username'].to_dict()

    ts_posts = pd.DataFrame(
        {
            'Nodo': truths['id'].apply(lambda x: f'capts{x}'),
            'Tipo_de_Nodo': 'Captura',
            'Plataforma': 'Truth Social',
            'Estructura': truths.apply(
                lambda x: 'Reply'
                if x['is_reply']
                else ('ReTruth' if x['is_retruth'] else 'Status'),
                axis=1,
            ),
            'Fecha': pd.to_datetime(truths['timestamp'].replace('-1', None), errors='coerce'),
            'Autor': truths['author'].map(user_mapping).fillna('unknown'),
            'Contenido': truths['text'],
        }
    )

    return pd.concat([ts_users, ts_posts], ignore_index=True)


def create_initial_dataset(fb_path, ts_path, output_path):
    facebook_data = process_facebook_data(fb_path)
    ts_data = process_truth_social_data(ts_path)

    combined_data = pd.concat([facebook_data, ts_data], ignore_index=True)
    combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
    combined_data.to_csv(output_path, index=False)
    return combined_data


def print_dataset_summary(dataset):
    print('Resumen del dataset:')
    print(f'Total de registros: {len(dataset)}')
    print('\nDistribución por tipo de nodo:')
    print(dataset['Tipo_de_Nodo'].value_counts())
    print('\nDistribución por plataforma:')
    print(dataset['Plataforma'].value_counts())
    print('\nDistribución por estructura:')
    print(dataset['Estructura'].value_counts())


def save_graph(G, filepath):
    """
    Guarda el grafo en formato pickle para uso posterior.

    Args:
        G: Grafo de NetworkX a guardar
        filepath: Ruta donde guardar el archivo
    """
    with open(filepath, 'wb') as f:
        pickle.dump(G, f)
    print(f'Grafo guardado en {filepath}')


def load_graph(filepath):
    """
    Carga un grafo previamente guardado.

    Args:
        filepath: Ruta del archivo pickle que contiene el grafo

    Returns:
        NetworkX graph objeto
    """
    with open(filepath, 'wb') as f:
        return pickle.load(f)


def load_or_create_dataset(fb_path, ts_path, output_path, use_sample=False, sample_fraction=1.0):
    """
    Carga el dataset existente o crea uno nuevo, con opción de muestreo.

    Args:
        fb_path: Ruta a la base de datos de Facebook
        ts_path: Ruta a los archivos de Truth Social
        output_path: Ruta donde guardar/cargar el dataset
        use_sample: Si se debe usar solo una fracción del dataset
        sample_fraction: Fracción del dataset a usar (entre 0 y 1)

    Returns:
        DataFrame con el dataset completo o una muestra
    """
    if os.path.exists(output_path):
        print(f'Cargando dataset existente desde {output_path}')
        dataset = pd.read_csv(output_path, low_memory=False)
    else:
        print(f'Creando nuevo dataset en {output_path}')
        dataset = create_initial_dataset(fb_path, ts_path, output_path)

    if use_sample:
        # Mantener todos los usuarios pero muestrear las capturas
        usuarios = dataset[dataset['Tipo_de_Nodo'] == 'Usuario']
        capturas = dataset[dataset['Tipo_de_Nodo'] == 'Captura']

        # Muestrear capturas manteniendo la proporción por plataforma
        capturas_sample = (
            capturas.groupby('Plataforma')
            .apply(lambda x: x.sample(frac=sample_fraction, random_state=42))
            .reset_index(drop=True)
        )

        # Combinar usuarios con la muestra de capturas
        dataset = pd.concat([usuarios, capturas_sample], ignore_index=True)
        print(f'Dataset reducido al {sample_fraction*100}% de las capturas originales')

    return dataset


if __name__ == '__main__':
    # Definir rutas y configuración
    FB_PATH = 'data/facebook.sqlite'
    TS_PATH = 'data/ts'
    DATASET_PATH = 'dataset_inicial.csv'
    GRAPH_CACHE_PATH = 'graph.pkl'

    # Configuración de muestreo
    USE_SAMPLE = True  # Activar o desactivar muestreo
    SAMPLE_FRACTION = 0.1  # Usar 10% del dataset

    # Cargar o crear dataset con muestreo
    dataset = load_or_create_dataset(
        FB_PATH, TS_PATH, DATASET_PATH, use_sample=USE_SAMPLE, sample_fraction=SAMPLE_FRACTION
    )

    # Imprimir resumen del dataset
    print_dataset_summary(dataset)

    # Construir o cargar el grafo desde caché
    if os.path.exists(GRAPH_CACHE_PATH):
        print('Cargando grafo desde cache...')
        G = load_graph(GRAPH_CACHE_PATH)
    else:
        print('Construyendo el grafo...')
        builder = SocialNetworkBuilder(dataset, ts_base_path=TS_PATH, fb_db_path=FB_PATH)
        G = builder.build_network()
        save_graph(G, GRAPH_CACHE_PATH)

    # Mostrar estadísticas de la red
    stats = {
        'num_nodes': G.number_of_nodes(),
        'num_edges': G.number_of_edges(),
        'density': nx.density(G),
        'num_users': len(
            [n for n in G.nodes if G.nodes[n].get('tipo') == 'Usuario'],
        ),
        'num_capturas': len(
            [n for n in G.nodes if G.nodes[n].get('tipo') == 'Captura'],
        ),
    }

    print('\nEstadísticas de la Red:')
    for key, value in stats.items():
        print(f'{key}: {value}')
