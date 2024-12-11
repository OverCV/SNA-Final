import matplotlib.pyplot as plt
import networkx as nx


def plot_network(G, fraction=0.005):


    # Obtener todas las aristas
    edges = list(G.edges())

   
    num_edges = int(len(edges) * fraction)
    fixed_edges = edges[:num_edges]  # Subconjunto fijo de aristas

    # Crear un subgrafo con las aristas seleccionadas
    sampled_subgraph = G.edge_subgraph(fixed_edges).copy()

    # Verificar cuántos nodos y aristas tiene el subgrafo
    print(f"Subgrafo fijo tiene {sampled_subgraph.number_of_nodes()} nodos y {sampled_subgraph.number_of_edges()} aristas.")

    # Si el subgrafo no tiene suficientes nodos o aristas, salimos
    if sampled_subgraph.number_of_nodes() == 0 or sampled_subgraph.number_of_edges() == 0:
        print("El subgrafo resultante no tiene nodos o aristas.")
        return

    # Calcular las centralidades
    centrality = nx.degree_centrality(sampled_subgraph)  # Usamos la centralidad por grado como ejemplo
    max_centrality = max(centrality.values())
    
    # Encontrar el nodo con mayor centralidad
    max_centrality_node = max(centrality, key=centrality.get)

    # Ajustar el tamaño de los nodos según la centralidad
    node_sizes = [10000 * centrality.get(node, 0) for node in sampled_subgraph.nodes]  # Aumento de tamaño para nodos con más centralidad

    # Configuración de colores según el atributo 'tipo'
    node_colors = [
        'red' if sampled_subgraph.nodes[n].get('tipo') == 'Usuario' else 'green'
        for n in sampled_subgraph.nodes
    ]

    # Crear la figura
    plt.figure(figsize=(12, 8))

    # Dibujar los nodos
    nx.draw(
        sampled_subgraph, 
        pos=nx.kamada_kawai_layout(sampled_subgraph, weight=None),
        with_labels=True,
        node_size=node_sizes,  # Tamaño de los nodos según centralidad
        node_color=node_colors,
        font_size=6,
        edge_color="gray",
        alpha=0.7,
        width=0.5
    )

    # Añadir título con información de nodos y aristas
    plt.title(f"Grafo Fijo con Centralidad\nNodos: {sampled_subgraph.number_of_nodes()} - Aristas: {sampled_subgraph.number_of_edges()}", fontsize=15)

    # Añadir leyenda para los tipos de nodos
    user_patch = plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='Usuario')
    capture_patch = plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='green', markersize=10, label='Captura')
    plt.legend(handles=[user_patch, capture_patch], loc='upper left')

    # Crear la tabla de información
    table_data = [
        ["Número de Nodos", sampled_subgraph.number_of_nodes()],
        ["Número de Aristas", sampled_subgraph.number_of_edges()],
        ["Nodo con más Centralidad", max_centrality_node],
        ["Valor de Centralidad Máxima", round(max_centrality, 4)]
    ]

    # Crear la tabla en la figura
    table = plt.table(
    cellText=table_data,               # Datos de la tabla
    colLabels=["Descripción", "Valor"], # Títulos de las columnas
    loc='lower center',                # Ubicación de la tabla
    cellLoc='center',                  # Alineación del texto en las celdas
    colColours=["#d3d3d3", "#d3d3d3"],  # Color gris claro para los encabezados de columna
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)

    # Mostrar la gráfica
    plt.show()