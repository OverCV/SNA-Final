import pandas as pd
import networkx as nx
import sqlite3
from networkx.algorithms.community import louvain_partitions, louvain_communities


class SocialNetworkBuilder:
    def __init__(self, dataset, ts_base_path=None, fb_db_path=None):
        self.dataset = dataset
        self.G = nx.DiGraph()
        self.ts_base_path = ts_base_path
        self.fb_db_path = fb_db_path
        self._cache_usernames = {}
        self._prepare_user_mapping()

    def _detect_communities(self):
        """
        Detecta comunidades utilizando el algoritmo Louvain.
        Devuelve las comunidades en múltiples niveles.
        """
        undirected = self.G.to_undirected()
        if nx.is_empty(undirected):
            print('⚠ La red está vacía, no se pueden detectar comunidades.')
            return []

        # Detectar las particiones usando louvain_partitions
        partitions = list(louvain_partitions(undirected))
        return partitions

    def analyze_communities(self):
        """
        Analiza las comunidades detectadas en la red.
        """
        partitions = self._detect_communities()
        if not partitions:
            print('No se encontraron comunidades.')
            return {}

        community_summary = {
            'total_levels': len(partitions),
            'final_partition': partitions[-1],  # Último nivel (menor granularidad)
            'num_communities': len(partitions[-1]),  # Número de comunidades finales
        }

        print(f'Comunidades detectadas en {len(partitions)} niveles.')
        print(f'Último nivel tiene {len(partitions[-1])} comunidades.')

        return community_summary

    def _prepare_user_mapping(self):
        # Construir un mapping (opcional) si tuvieras IDs numéricos y usernames
        # Suponiendo que en Truth Social usuario = Autor:
        # Aquí no tenemos un user_id directo, pero si lo tuvieras, podrías guardarlo.
        # Para este ejemplo, dejamos esto vacío o simulado.
        pass

    def _get_username_from_id(self, user_id):
        # Aquí deberías mapear user_id a username leyendo de users.tsv
        # Como ejemplo, lo cargamos on-demand y cacheamos:
        if user_id not in self._cache_usernames:
            users = pd.read_csv(
                f'{self.ts_base_path}/users.tsv', sep='\t', low_memory=False, on_bad_lines='skip'
            )
            mapping = users.set_index('id')['username'].to_dict()
            if user_id in mapping:
                self._cache_usernames[user_id] = mapping[user_id]
            else:
                self._cache_usernames[user_id] = 'unknown_user'
        return self._cache_usernames[user_id]

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
        # Conexión básica: Autor -> Captura (PUBLICA)
        capturas = self.dataset[self.dataset['Tipo_de_Nodo'] == 'Captura']
        for _, captura in capturas.iterrows():
            autor_node = f"@{captura['Autor']}"
            if autor_node in self.G:
                self.G.add_edge(autor_node, captura['Nodo'], tipo='PUBLICA')

        # Si quisiéramos conectar replies con su post original o ReTruth con original
        # necesitaríamos lógica extra. En este ejemplo está pendiente.
        # Podrías extraer el post original de algún campo si estuviera disponible.
        # Este es solo un placeholder.
        # for _, captura in capturas.iterrows():
        #     if captura['Estructura'] == 'Reply':
        #         # Identificar el post original y conectarlo
        #         pass
        #     elif captura['Estructura'] == 'ReTruth':
        #         # Identificar el post original y conectarlo
        #         pass

    def add_truth_social_relationships(self):
        # Hashtags
        hashtag_edges_path = f'{self.ts_base_path}/truth_hashtag_edges.tsv'
        hashtags_path = f'{self.ts_base_path}/hashtags.tsv'
        user_tags_path = f'{self.ts_base_path}/truth_user_tag_edges.tsv'

        if not all(
            [
                pd.io.common.file_exists(hashtag_edges_path),
                pd.io.common.file_exists(hashtags_path),
                pd.io.common.file_exists(user_tags_path),
            ]
        ):
            print('Archivos de relaciones Truth Social faltan, omitiendo esta parte.')
            return

        hashtag_edges = pd.read_csv(
            hashtag_edges_path, sep='\t', low_memory=False, on_bad_lines='skip'
        )
        hashtags = pd.read_csv(hashtags_path, sep='\t', low_memory=False, on_bad_lines='skip')

        for _, edge in hashtag_edges.iterrows():
            truth_node = f"capts{edge['truth_id']}"
            hashtag_node = f"hashtag_{edge['hashtag_id']}"
            if truth_node in self.G:
                ht_row = hashtags[hashtags['id'] == edge['hashtag_id']]
                if not ht_row.empty:
                    hashtag_text = ht_row['hashtag'].iloc[0]
                    self.G.add_node(hashtag_node, tipo='Hashtag', texto=hashtag_text)
                    self.G.add_edge(truth_node, hashtag_node, tipo='CONTIENE_HASHTAG')

        # User tags (menciones)
        user_tags = pd.read_csv(user_tags_path, sep='\t', low_memory=False, on_bad_lines='skip')
        for _, tag in user_tags.iterrows():
            truth_node = f"capts{tag['truth_id']}"
            user_node = f"@{self._get_username_from_id(tag['user_id'])}"
            if truth_node in self.G and user_node in self.G:
                self.G.add_edge(truth_node, user_node, tipo='MENCIONA')

        # Similarmente podrías agregar edges para media o URLs si tienes esos TSV.
        # Ejemplo (asumiendo que tienes truth_media_edges y truth_external_url_edges):

        media_edges_path = f'{self.ts_base_path}/truth_media_edges.tsv'
        if pd.io.common.file_exists(media_edges_path):
            media_edges = pd.read_csv(
                media_edges_path, sep='\t', low_memory=False, on_bad_lines='skip'
            )
            # Aquí no tenemos el contenido completo de media, pero podrías crear nodos tipo 'Media'
            for _, me in media_edges.iterrows():
                truth_node = f"capts{me['truth_id']}"
                media_node = f"media_{me['media_id']}"
                if truth_node in self.G:
                    self.G.add_node(media_node, tipo='Media')
                    self.G.add_edge(truth_node, media_node, tipo='CONTIENE_MEDIA')

        url_edges_path = f'{self.ts_base_path}/truth_external_url_edges.tsv'
        external_urls_path = f'{self.ts_base_path}/external_urls.tsv'
        if pd.io.common.file_exists(url_edges_path) and pd.io.common.file_exists(
            external_urls_path
        ):
            url_edges = pd.read_csv(url_edges_path, sep='\t', low_memory=False, on_bad_lines='skip')
            external_urls = pd.read_csv(
                external_urls_path, sep='\t', low_memory=False, on_bad_lines='skip'
            )
            for _, ue in url_edges.iterrows():
                truth_node = f"capts{ue['truth_id']}"
                url_node = f"url_{ue['url_id']}"
                if truth_node in self.G:
                    url_info = external_urls[external_urls['id'] == ue['url_id']]
                    if not url_info.empty:
                        url_text = url_info['url'].iloc[0]
                        self.G.add_node(url_node, tipo='URL', url=url_text)
                        self.G.add_edge(truth_node, url_node, tipo='CONTIENE_URL')

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

    def get_network_stats(self):
        stats = {
            'num_nodes': self.G.number_of_nodes(),
            'num_edges': self.G.number_of_edges(),
            'density': nx.density(self.G),
            'num_users': len([n for n in self.G.nodes if self.G.nodes[n].get('tipo') == 'Usuario']),
            'num_capturas': len(
                [n for n in self.G.nodes if self.G.nodes[n].get('tipo') == 'Captura']
            ),
        }
        return stats

    def build_network(self):
        self.add_nodes()
        self.add_edges()
        self.add_truth_social_relationships()
        self.add_facebook_relationships()
        return self.G

    def analyze_network(self):
        metrics = {
            'centralidad': self._calculate_centrality_metrics(),
            'comunidades': self._detect_communities(),
            'patrones_difusion': self._analyze_diffusion_patterns(),
        }
        return metrics

    def _calculate_centrality_metrics(self):
        degree = nx.degree_centrality(self.G)
        betweenness = nx.betweenness_centrality(self.G, k=100, normalized=True)  # k para aproximar
        try:
            eigen = nx.eigenvector_centrality(self.G, max_iter=1000)
        except Exception:
            eigen = {}
        return {'degree': degree, 'betweenness': betweenness, 'eigenvector': eigen}

    def _detect_communities(self):
        if 'community_louvain' not in globals():
            return {}
        undirected = self.G.to_undirected()
        partition = louvain_partitions(undirected)
        return partition

    def _analyze_diffusion_patterns(self):
        # Aquí pones la lógica para analizar patrones de difusión.
        # Como ejemplo retornamos un diccionario con llaves vacías.
        # Podrías filtrar subgrafos por plataforma y medir cosas.
        platform_stats = {}
        for platform in ['Facebook', 'Truth Social']:
            sub_nodes = [n for n in self.G.nodes if self.G.nodes[n].get('plataforma') == platform]
            subg = self.G.subgraph(sub_nodes)
            platform_stats[platform] = {
                'velocidad_propagacion': self._calculate_propagation_speed(subg),
                'alcance': self._calculate_reach(subg),
                'influenciadores': self._identify_influencers(subg),
            }
        return platform_stats

    def _calculate_propagation_speed(self, subg):
        # Lógica para estimar velocidad de propagación en el subgrafo
        return 0.0

    def _calculate_reach(self, subg):
        # Lógica para estimar el alcance
        return 0.0

    def _identify_influencers(self, subg):
        # Lógica para identificar influenciadores, por ejemplo top-n en centralidad
        deg = nx.degree_centrality(subg)
        sorted_deg = sorted(deg.items(), key=lambda x: x[1], reverse=True)
        return [node for node, val in sorted_deg[:10]]  # Top 10

    def _detect_communities(self):
        """
        Detecta comunidades usando el algoritmo de Louvain
        Convertir a grafo no dirigido para detección de comunidades
        """
        undirected_graph = self.G.to_undirected()

        try:
            # Detectar comunidades usando el método de Louvain
            communities = louvain_communities(undirected_graph)

            # Crear un diccionario que mapee nodos a sus comunidades
            community_mapping = {}
            for i, comm in enumerate(communities):
                for node in comm:
                    community_mapping[node] = i

            return community_mapping

        except Exception as e:
            print(f'Error al detectar comunidades: {e}')
            return {}

    def analyze_diffusion_patterns(self):
        """
        Analiza patrones de difusión entre plataformas
        """
        patterns = {}

        # Analizar por plataforma
        for platform in ['Facebook', 'Truth Social']:
            # Filtrar nodos de la plataforma
            platform_nodes = [
                n for n in self.G.nodes if self.G.nodes[n].get('plataforma') == platform
            ]

            if not platform_nodes:
                continue

            # Crear subgrafo de la plataforma
            subgraph = self.G.subgraph(platform_nodes)

            # Calcular métricas de difusión
            patterns[platform] = {
                'num_nodes': len(subgraph),
                'num_edges': subgraph.number_of_edges(),
                'density': nx.density(subgraph),
                'avg_clustering': nx.average_clustering(subgraph.to_undirected()),
                'top_influencers': self._get_top_influencers(subgraph, n=5),
            }

        return patterns

    def _get_top_influencers(self, graph, n=5):
        """
        Identifica los n usuarios más influyentes basado en centralidad
        """
        degree_cent = nx.degree_centrality(graph)
        return sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:n]
