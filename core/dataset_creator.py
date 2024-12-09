# core\dataset_creator.py
import os
import pandas as pd
import sqlite3


def convert_tf_to_bool(value):
    return True if value == 't' else False if value == 'f' else None


class DatasetCreator:
    def __init__(self, fb_path, ts_path, output_path):
        self.fb_path = fb_path
        self.ts_path = ts_path
        self.output_path = output_path

    def process_facebook_data(self):
        conn = sqlite3.connect(self.fb_path)
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

    def process_truth_social_data(self):
        users = pd.read_csv(
            f'{self.ts_path}/users.tsv', sep='\t', low_memory=False, on_bad_lines='skip'
        )
        truths = pd.read_csv(
            f'{self.ts_path}/truths.tsv', sep='\t', low_memory=False, on_bad_lines='skip', quoting=3
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

    def create_initial_dataset(self):
        facebook_data = self.process_facebook_data()
        ts_data = self.process_truth_social_data()

        combined_data = pd.concat([facebook_data, ts_data], ignore_index=True)
        combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
        combined_data.to_csv(self.output_path, index=False)
        return combined_data

    def load_or_create_dataset(self, use_sample=False, sample_fraction=1.0):
        if os.path.exists(self.output_path):
            print(f'Cargando dataset existente desde {self.output_path}')
            dataset = pd.read_csv(self.output_path, low_memory=False)
        else:
            print(f'Creando nuevo dataset en {self.output_path}')
            dataset = self.create_initial_dataset()

        if use_sample:
            usuarios = dataset[dataset['Tipo_de_Nodo'] == 'Usuario']
            capturas = dataset[dataset['Tipo_de_Nodo'] == 'Captura']
            capturas_sample = (
                capturas.groupby('Plataforma')
                .apply(lambda x: x.sample(frac=sample_fraction, random_state=42))
                .reset_index(drop=True)
            )
            dataset = pd.concat([usuarios, capturas_sample], ignore_index=True)
            print(f'Dataset reducido al {sample_fraction*100}% de las capturas originales')

        return dataset

    @staticmethod
    def print_dataset_summary(dataset):
        print('Resumen del dataset:')
        print(f'Total de registros: {len(dataset)}')
        print('\nDistribución por tipo de nodo:')
        print(dataset['Tipo_de_Nodo'].value_counts())
        print('\nDistribución por plataforma:')
        print(dataset['Plataforma'].value_counts())
        print('\nDistribución por estructura:')
        print(dataset['Estructura'].value_counts())
