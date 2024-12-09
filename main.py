import pandas as pd
import sqlite3


def process_facebook_data(db_path):
    # El código de Facebook permanece igual
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
            'cap_fb_' || pid as Nodo,
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
    return pd.concat([facebook_users, facebook_posts])


def process_truth_social_data(base_path):
    # Leemos los archivos TSV
    users = pd.read_csv(f'{base_path}/users.tsv', sep='\t', low_memory=False, on_bad_lines='skip')

    truths = pd.read_csv(
        f'{base_path}/truths.tsv', sep='\t', low_memory=False, on_bad_lines='skip', quoting=3
    )

    # Procesamos usuarios
    ts_users = pd.DataFrame(
        {
            'Nodo': users['username'].apply(lambda x: f'@{x}'),
            'Tipo_de_Nodo': 'Usuario',
            'Plataforma': 'Truth Social',
            'Estructura': 'N/A',
            'Autor': users['username'],
            'Fecha': pd.to_datetime(users['timestamp'].replace('-1', None), errors='coerce'),
            'Contenido': None,
        }
    )

    # Mapping de usuarios
    user_mapping = users.set_index('id')['username'].to_dict()

    # Procesamos posts
    ts_posts = pd.DataFrame(
        {
            'Nodo': truths['id'].apply(lambda x: f'cap_ts_{x}'),
            'Tipo_de_Nodo': 'Captura',
            'Plataforma': 'Truth Social',
            'Estructura': truths.apply(
                lambda x: 'Reply' if x['is_reply'] else 'ReTruth' if x['is_retruth'] else 'Status',
                axis=1,
            ),
            'Autor': truths['author'].map(user_mapping).fillna('unknown'),
            'Fecha': pd.to_datetime(truths['timestamp'].replace('-1', None), errors='coerce'),
            'Contenido': truths['text'],
        }
    )

    return pd.concat([ts_users, ts_posts])


def create_initial_dataset(fb_path, ts_path, output_path):
    # Procesamos los datos
    facebook_data = process_facebook_data(fb_path)
    ts_data = process_truth_social_data(ts_path)

    # Combinamos los datos
    combined_data = pd.concat([facebook_data, ts_data])

    # Limpiamos el dataset
    combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
    combined_data = combined_data.fillna('N/A')

    # Guardamos
    combined_data.to_csv(output_path, index=False)

    return combined_data


if __name__ == '__main__':
    dataset = create_initial_dataset(
        fb_path='data/facebook.sqlite', ts_path='data/ts', output_path='dataset_inicial.csv'
    )

    print('Resumen del dataset creado:')
    print(f'Total de registros: {len(dataset)}')
    print('\nDistribución por tipo de nodo:')
    print(dataset['Tipo_de_Nodo'].value_counts())
    print('\nDistribución por plataforma:')
    print(dataset['Plataforma'].value_counts())
    print('\nDistribución por estructura:')
    print(dataset['Estructura'].value_counts())
