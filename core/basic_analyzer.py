# core\basic_analyzer.py
import networkx as nx


class BasicAnalyzer:
    """
    Análisis de Métricas de Redes Básicas:
    1. Centralidad
    2. Densidad
    3. Modularidad (Comunidades)
    """

    def __init__(self, G):
        self.G = G

    def compute_centralities(self):
        # Solo ejemplo: degree centrality
        degree = nx.degree_centrality(self.G)
        return {'degree': degree}

    def compute_density(self):
        return nx.density(self.G)

    def detect_communities(self):
        # Podrías usar louvain_communities u otro método
        # communities = louvain_communities(self.G.to_undirected())
        # return communities
        return []

    def summarize(self):
        # Retorna un dict con las métricas básicas calculadas
        return {
            'centralities': self.compute_centralities(),
            'density': self.compute_density(),
            'communities': self.detect_communities(),
        }
