# core\basic_analyzer.py
import networkx as nx
from networkx.algorithms.community import louvain_communities
import matplotlib.pyplot as plt
import json


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
        degree = {
            k: v
            for k, v in sorted(
                nx.degree_centrality(self.G).items(), key=lambda item: item[1], reverse=True
            )
            if v > 0
        }
        return {'degree': degree}

    def compute_density(self):
        return nx.density(self.G)

    def detect_communities(self):
        if self.G.is_directed():
            G_undirected = self.G.to_undirected()
        else:
            G_undirected = self.G

        communities = louvain_communities(G_undirected)
        sorted_communities = sorted([c for c in communities if len(c) > 1], key=len, reverse=True)
        return sorted_communities

    def plot_largest_community(self, communities):
        subgraph = self.G.subgraph(communities)
        plt.figure(figsize=(10, 8))
        nx.draw(
            subgraph, with_labels=True, node_color='lightblue', node_size=100, edge_color='gray'
        )
        plt.title('Comunidad Más Grande')
        plt.show()

    def export_communities_to_json(self, communities, filename='communities.json'):
        """
        Exporta las comunidades detectadas a un archivo JSON.
        Cada comunidad será una lista de nodos.
        """
        if not communities:
            print('No hay comunidades para exportar.')
            return

        data = {f'Comunidad {i + 1}': list(community) for i, community in enumerate(communities)}

        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

        print(f'Comunidades exportadas a {filename}')

    def export_centralities_to_json(self, centralities, filename='centralities.json'):
        """
        Exporta las centralidades de los nodos a un archivo JSON.
        """
        if not centralities:
            print('No hay centralidades para exportar.')
            return

        with open(filename, 'w') as f:
            json.dump(centralities, f, indent=4)

        print(f'Centralidades exportadas a {filename}')

    def summarize(self):
        communities = self.detect_communities()
        centralities = self.compute_centralities()

        # Contar el total de nodos con centralidad > 0
        total_central_nodes = sum(len(nodes) for nodes in centralities.values())

        ## Titulo Resultados
        print('\nResultados del Análisis Básico:')

        # Nodo con mayor centralidad
        degree_centralities = centralities['degree']
        if degree_centralities:
            top_node, top_centrality = next(iter(degree_centralities.items()))
            top_degree = self.G.degree(top_node)
            print(
                f"El nodo con la mayor centralidad es '{top_node}' con centralidad {top_centrality:.4f} y grado {top_degree}."
            )

        print(f'Se detectaron {len(communities)} comunidades.')
        if communities:
            largest_community = communities[0]
            print(f'La comunidad más grande tiene: {len(largest_community)} nodos.')
            self.plot_largest_community(largest_community)

        self.export_communities_to_json(communities)
        self.export_centralities_to_json(centralities)

        return {
            'Comunidades detectadas': len(communities),
            'Nodos con centralidad mayor a 0.0': total_central_nodes,
            'Densidad': self.compute_density(),
        }
