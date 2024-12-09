import pandas as pd
import sqlite3
import networkx as nx
from networkx.algorithms.community import louvain_communities
import pickle


class NetworkBuilder:
    def __init__(self, dataset, ts_base_path=None, fb_db_path=None):
        self.dataset = dataset
        self.G = nx.DiGraph()
        self.ts_base_path = ts_base_path
        self.fb_db_path = fb_db_path
        self._cache_usernames = {}

    def _get_username_from_id(self, user_id):
        if self.ts_base_path:
            users = pd.read_csv(
                f'{self.ts_base_path}/users.tsv', sep='\t', low_memory=False, on_bad_lines='skip'
            )
            mapping = users.set_index('id')['username'].to_dict()
            return mapping.get(user_id, 'unknown_user')
        return 'unknown_user'

    def add_nodes(self):
        for _, row in self.dataset.iterrows():
            if row['Tipo_de_Nodo'] == 'Usuario':
                self.G.add_node(
                    row['Nodo'], tipo='Usuario', plataforma=row['Plataforma'], username=row['Autor']
                )
            else:
                self.G.add_node(
                    row['Nodo'],
                    tipo='Captura',
                    plataforma=row['Plataforma'],
                    estructura=row['Estructura'],
                    autor=row['Autor'],
                    fecha=row['Fecha'],
                    contenido=row['Contenido'],
                )

    def add_edges(self):
        capturas = self.dataset[self.dataset['Tipo_de_Nodo'] == 'Captura']
        for _, captura in capturas.iterrows():
            autor_node = f"@{captura['Autor']}"
            if autor_node in self.G:
                self.G.add_edge(autor_node, captura['Nodo'], tipo='PUBLICA')

    def add_truth_social_relationships(self):
        # Aquí el código que añade hashtags, menciones, etc.
        # Como ejemplo, lo dejamos igual.
        pass

    def add_facebook_relationships(self):
        if self.fb_db_path is None:
            return
        conn = sqlite3.connect(self.fb_db_path)
        comments = pd.read_sql(
            """
            SELECT 
                'comment_' || cid as comment_node,
                'capfb' || pid as post_node,
                id as author_id,
                name as author_name
            FROM comment
            """,
            conn,
        )
        conn.close()

        for _, comment in comments.iterrows():
            if comment['post_node'] in self.G:
                self.G.add_node(
                    comment['comment_node'], tipo='Comentario', autor=comment['author_name']
                )
                self.G.add_edge(
                    comment['post_node'], comment['comment_node'], tipo='TIENE_COMENTARIO'
                )

    def build_network(self):
        self.add_nodes()
        self.add_edges()
        self.add_truth_social_relationships()
        self.add_facebook_relationships()
        return self.G

    def save_graph(self, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(self.G, f)
        print(f'Grafo guardado en {filepath}')

    def load_graph(self, filepath):
        with open(filepath, 'rb') as f:
            self.G = pickle.load(f)
        print(f'Grafo cargado desde {filepath}')

    def get_network_stats(self):
        return {
            'num_nodes': self.G.number_of_nodes(),
            'num_edges': self.G.number_of_edges(),
            'density': nx.density(self.G),
            'num_users': len([n for n in self.G.nodes if self.G.nodes[n].get('tipo') == 'Usuario']),
            'num_capturas': len(
                [n for n in self.G.nodes if self.G.nodes[n].get('tipo') == 'Captura']
            ),
        }
