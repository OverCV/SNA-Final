import sqlite3
import networkx as nx


def read_facebook_data(db_path):
    """Lee los datos de la base de datos SQLite y crea un grafo."""
    # Conectar a la base de datos
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Obtener los datos de la tabla (esto puede variar según la estructura de la base de datos)
    cursor.execute('SELECT * FROM comment')
    rows = cursor.fetchall()

    # Crear el grafo
    G = nx.Graph()

    # Agregar las relaciones de amistad al grafo
    for row in rows:
        # user1, user2 = row
        # G.add_edge(user1, user2)
        print(row)

    # Cerrar la conexión a la base de datos
    conn.close()

    return G


def main():
    """Application initializer."""
    db_path = 'data/facebook.sqlite'  # Ruta relativa de la base de datos
    net = read_facebook_data(db_path)
    print(f'{net=}')


if __name__ == '__main__':
    main()
