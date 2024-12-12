import pandas as pd
from collections import Counter
import networkx as nx
from sklearn.metrics import precision_score, recall_score, f1_score


class DeepAnalyzer:
    """
    Análisis de Métricas de Redes Avanzadas:
    - Clasificación de Capturas
    - Comparación entre Plataformas
    - Métricas de Centralidad, Densidad y Modularidad
    """

    def __init__(self, G):
        """
        Inicializa el DeepAnalyzer con un grafo G.
        :param G: Grafo de la red construido previamente.
        """
        self.G = G

    def classify_captures(self, dataset):
        """
        Clasifica capturas basándose en las plataformas del dataset.
        :param dataset: DataFrame con las capturas.
        :return: Conteo de capturas por plataforma.
        """
        if 'Plataforma' not in dataset.columns:
            raise ValueError("El dataset no contiene una columna 'Plataforma'.")

        # Contar las ocurrencias de cada plataforma
        platform_counts = dataset['Plataforma'].value_counts().to_dict()
        return platform_counts

    def validate_graph_attributes(self, dataset):
        """
        Verifica y alinea los atributos de los nodos del grafo con el dataset.
        :param dataset: DataFrame con las capturas.
        """
        if 'Nodo' not in dataset.columns or 'Plataforma' not in dataset.columns:
            raise ValueError("El dataset debe contener las columnas 'Nodo' y 'Plataforma'.")

        node_platform_map = dict(zip(dataset['Nodo'], dataset['Plataforma']))

        for node in self.G.nodes:
            if node in node_platform_map:
                self.G.nodes[node]['Plataforma'] = node_platform_map[node]

    def centrality_metrics(self):
        """
        Calcula métricas de centralidad para el grafo usando aproximaciones avanzadas.
        :return: Diccionario con medidas de centralidad (grado, cercanía, intermediación, eigenvector).
        """
        degree_centrality = nx.degree_centrality(self.G)
        closeness_centrality = nx.closeness_centrality(self.G)
        betweenness_centrality = nx.betweenness_centrality(
            self.G, k=min(500, len(self.G))
        )  # Aproximación
        eigenvector_centrality = nx.eigenvector_centrality(self.G, max_iter=1000)

        return {
            'degree_centrality': degree_centrality,
            'closeness_centrality': closeness_centrality,
            'betweenness_centrality': betweenness_centrality,
            'eigenvector_centrality': eigenvector_centrality,
        }

    def density(self):
        """
        Calcula la densidad del grafo global.
        :return: Valor de densidad.
        """
        return nx.density(self.G)

    def modularity(self):
        """
        Detecta subcomunidades en el grafo utilizando el algoritmo de comunidades de Louvain.
        :return: Diccionario con la comunidad asignada a cada nodo y estadísticas adicionales.
        """
        try:
            from community import community_louvain
        except ImportError:
            raise ImportError(
                "Se requiere el paquete 'python-louvain' para calcular la modularidad."
            )

        # Convertir el grafo a no dirigido si es necesario
        if self.G.is_directed():
            undirected_graph = self.G.to_undirected()
        else:
            undirected_graph = self.G

        partition = community_louvain.best_partition(undirected_graph, random_state=42)
        num_communities = len(set(partition.values()))
        avg_community_size = len(self.G.nodes) / num_communities if num_communities > 0 else 0

        return {
            'partition': partition,
            'num_communities': num_communities,
            'avg_community_size': avg_community_size,
        }

    def compare_platforms(self, dataset):
        """
        Compara métricas básicas entre plataformas en el grafo y el dataset.
        :param dataset: DataFrame con las capturas.
        :return: Diccionario con métricas comparativas por plataforma.
        """
        self.validate_graph_attributes(dataset)

        # Métricas por plataforma
        platforms = dataset['Plataforma'].unique()
        metrics = {}

        for platform in platforms:
            # Filtrar nodos del grafo que pertenecen a la plataforma
            nodes_in_platform = [
                node for node, data in self.G.nodes(data=True) if data.get('Plataforma') == platform
            ]

            # Crear subgrafo basado en nodos
            subgraph = self.G.subgraph(nodes_in_platform).copy()

            metrics[platform] = {
                'num_nodes': subgraph.number_of_nodes(),
                'num_edges': subgraph.number_of_edges(),
                'average_clustering': nx.average_clustering(subgraph)
                if subgraph.number_of_nodes() > 0
                else 0,
                'density': nx.density(subgraph),
            }

        return metrics

    def capture_classification_metrics(self, y_true, y_pred):
        """
        Calcula métricas de clasificación como precisión, recall y F1.
        :param y_true: Etiquetas reales.
        :param y_pred: Etiquetas predichas.
        :return: Diccionario con métricas de precisión, recall y F1.
        """
        precision = precision_score(y_true, y_pred, average='weighted')
        recall = recall_score(y_true, y_pred, average='weighted')
        f1 = f1_score(y_true, y_pred, average='weighted')

        return {'precision': precision, 'recall': recall, 'f1': f1}

    def analyze(self, dataset, y_true=None, y_pred=None):
        """
        Llama a los métodos de análisis y los combina en un resultado.
        :param dataset: DataFrame con las capturas.
        :param y_true: Etiquetas reales para clasificación de capturas (opcional).
        :param y_pred: Etiquetas predichas para clasificación de capturas (opcional).
        :return: Diccionario con resultados del análisis profundo.
        """
        classification = self.classify_captures(dataset)
        platform_comparison = self.compare_platforms(dataset)
        centrality = self.centrality_metrics()
        density = self.density()
        modularity = self.modularity()

        result = {
            'classification': classification,
            'platform_comparison': platform_comparison,
            'centrality': centrality,
            'density': density,
            'modularity': modularity,
        }

        if y_true is not None and y_pred is not None:
            classification_metrics = self.capture_classification_metrics(y_true, y_pred)
            result['classification_metrics'] = classification_metrics

        return result
