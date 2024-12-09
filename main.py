import os
import pandas as pd
import sqlite3
import networkx as nx
from validator import DatasetValidator
from snbuilder import SocialNetworkBuilder


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


def load_or_create_dataset(fb_path, ts_path, output_path):
    if os.path.exists(output_path):
        print(f'Cargando dataset existente desde {output_path}')
        return pd.read_csv(output_path, low_memory=False)
    else:
        print(f'Creando nuevo dataset en {output_path}')
        return create_initial_dataset(fb_path, ts_path, output_path)


def print_dataset_summary(dataset):
    print('Resumen del dataset:')
    print(f'Total de registros: {len(dataset)}')
    print('\nDistribución por tipo de nodo:')
    print(dataset['Tipo_de_Nodo'].value_counts())
    print('\nDistribución por plataforma:')
    print(dataset['Plataforma'].value_counts())
    print('\nDistribución por estructura:')
    print(dataset['Estructura'].value_counts())


if __name__ == '__main__':
    # Definir rutas
    FB_PATH = 'data/facebook.sqlite'
    TS_PATH = 'data/ts'
    DATASET_PATH = 'dataset_inicial.csv'

    # Cargar o crear dataset
    dataset = load_or_create_dataset(FB_PATH, TS_PATH, DATASET_PATH)

    # Imprimir resumen
    print_dataset_summary(dataset)

    # Validar el dataset
    # validator = DatasetValidator(dataset)
    # validator.validate_schema()
    # temporal_issues = validator.validate_temporal_consistency(dataset)
    # if temporal_issues:
    #     print('Problemas temporales detectados:')
    #     for issue in temporal_issues:
    #         print(issue)

    # Construir red
    builder = SocialNetworkBuilder(dataset, ts_base_path=TS_PATH, fb_db_path=FB_PATH)
    G = builder.build_network()

    # Mostrar estadísticas de la red
    stats = builder.get_network_stats()
    print('\nEstadísticas de la Red:')
    for key, value in stats.items():
        print(f'{key}: {value}')

    # Realizar análisis más profundo de la red
    network_analysis = builder.analyze_network()
    print('\nAnálisis de la red:')
    print('Métricas de Centralidad:', network_analysis['centralidad'].keys())
    print(
        'Comunidades detectadas (ejemplo de partición):',
        len(set(network_analysis['comunidades'].values())),
        'comunidades',
    )
    print('Patrones de difusión:', network_analysis['patrones_difusion'].keys())
