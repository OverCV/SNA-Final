# core\dataset_creator.py
import os
import pandas as pd
import sqlite3
import glob


def convert_tf_to_bool(value):
    return True if value == 't' else False if value == 'f' else None


class DatasetCreator:
    def __init__(self, fb_path, ts_path, twitter_path, output_path):
        self.fb_path = fb_path
        self.ts_path = ts_path
        self.twitter_path = twitter_path
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

    def process_twitter_data(self):
        """
        Process Twitter data from the RepLab 2013 dataset format into our standardized structure.
        Handles encoding issues and timestamp conversion properly.
        """
        # Initialize collections for tweets and users
        all_tweets = []
        all_users = set()

        # Process tweet information from all relevant directories
        for data_dir in ['training/tweet_info', 'test/tweet_info', 'background/tweet_info']:
            dir_path = os.path.join(self.twitter_path, data_dir)
            if not os.path.exists(dir_path):
                continue

            # Process each .dat file in the directory
            for dat_file in glob.glob(os.path.join(dir_path, '*.dat')):
                try:
                    # Read the file with error handling for encoding issues
                    tweets_df = pd.read_csv(
                        dat_file,
                        sep='\t',
                        quoting=3,
                        encoding='utf-8',
                        on_bad_lines='skip',  # Skip problematic lines instead of failing
                        names=[
                            'tweet_id',
                            'author',
                            'entity_id',
                            'tweet_url',
                            'language',
                            'timestamp',
                            'urls',
                            'extended_urls',
                            'md5_extended_urls',
                            'is_near_duplicate_of',
                        ],
                        dtype={
                            'tweet_id': str,
                            'author': str,
                            'entity_id': str,
                            'tweet_url': str,
                            'language': str,
                            'timestamp': str,  # Read as string first
                            'urls': str,
                            'extended_urls': str,
                            'md5_extended_urls': str,
                            'is_near_duplicate_of': str,
                        },
                    )

                    # Convert timestamp after reading
                    # First, skip the header row by checking if timestamp is actually "timestamp"
                    tweets_df = tweets_df[tweets_df['timestamp'] != 'timestamp']
                    # Convert timestamp to numeric, handling errors
                    tweets_df['timestamp'] = pd.to_numeric(tweets_df['timestamp'], errors='coerce')

                    # Add tweets to our collection
                    for _, tweet in tweets_df.iterrows():
                        try:
                            # Convert timestamp to datetime only if it's a valid number
                            timestamp = (
                                pd.to_datetime(tweet.timestamp, unit='s')
                                if pd.notna(tweet.timestamp)
                                else None
                            )

                            all_tweets.append(
                                {
                                    'Nodo': f'captw{tweet.tweet_id}',
                                    'Tipo_de_Nodo': 'Captura',
                                    'Plataforma': 'Twitter',
                                    'Estructura': self.determine_tweet_structure(tweet),
                                    'Autor': tweet.author,
                                    'Fecha': timestamp,
                                    'Contenido': None,  # Twitter content needs to be retrieved separately
                                }
                            )

                            # Add user to our collection
                            if pd.notna(tweet.author):
                                all_users.add(tweet.author)

                        except Exception as e:
                            print(f'Error processing tweet {tweet.tweet_id}: {str(e)}')
                            continue

                except Exception as e:
                    print(f'Error processing {dat_file}: {str(e)}')
                    continue

        # Create users DataFrame
        users_df = pd.DataFrame(
            [
                {
                    'Nodo': f'@{username}',
                    'Tipo_de_Nodo': 'Usuario',
                    'Plataforma': 'Twitter',
                    'Estructura': 'N/A',
                    'Autor': username,
                    'Fecha': None,
                    'Contenido': None,
                }
                for username in all_users
            ]
        )

        # Create tweets DataFrame
        tweets_df = pd.DataFrame(all_tweets)

        # Combine and return
        return pd.concat([users_df, tweets_df], ignore_index=True)

    def determine_tweet_structure(self, tweet):
        """
        Determine the structure of a tweet based on available metadata.
        For now returns 'Status' as default, but could be enhanced with reply detection.
        """
        return 'Status'

    def _determine_tweet_structure(self, tweet):
        """
        Determine the structure of a tweet based on available metadata.
        This is a simplified version - could be enhanced with more complex logic
        if additional metadata is available.
        """
        # For now, we'll just mark everything as 'Status' since we don't have reply/thread info
        # This could be enhanced later with more sophisticated logic
        return 'Status'

    def create_initial_dataset(self):
        """
        Create the initial dataset combining all three social media sources.
        """
        facebook_data = self.process_facebook_data()
        ts_data = self.process_truth_social_data()
        twitter_data = self.process_twitter_data()

        combined_data = pd.concat([facebook_data, ts_data, twitter_data], ignore_index=True)
        combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
        combined_data.to_csv(self.output_path, index=False)
        return combined_data

    # def create_initial_dataset(self):
    #     """
    #     Create the initial dataset combining all three social media sources.
    #     """
    #     facebook_data = self.process_facebook_data()
    #     ts_data = self.process_truth_social_data()
    #     twitter_data = self.process_twitter_data()

    #     combined_data = pd.concat([facebook_data, ts_data, twitter_data], ignore_index=True)
    #     combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
    #     combined_data.to_csv(self.output_path, index=False)
    #     return combined_data

    # def create_initial_dataset(self):
    #     facebook_data = self.process_facebook_data()
    #     ts_data = self.process_truth_social_data()

    #     combined_data = pd.concat([facebook_data, ts_data], ignore_index=True)
    #     combined_data['Fecha'] = pd.to_datetime(combined_data['Fecha'], errors='coerce')
    #     combined_data.to_csv(self.output_path, index=False)
    #     return combined_data

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
