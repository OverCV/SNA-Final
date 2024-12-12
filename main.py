# main.py
import os
from core.dataset_creator import DatasetCreator
from core.network_builder import NetworkBuilder
from core.data_simulator import DataSimulator
from core.basic_analyzer import BasicAnalyzer
from core.deep_analyzer import DeepAnalyzer
from core.validator import DatasetValidator
from core.visualizer import plot_network

if __name__ == '__main__':
    FB_PATH = 'data/facebook.sqlite'
    TS_PATH = 'data/ts'
    TWITTER_PATH = 'data/twitter'
    DATASET_PATH = 'dataset_inicial.csv'

    GRAPH_CACHE_PATH = 'graph.pkl'

    USE_SAMPLE = True
    SAMPLE_FRACTION = 0.005  # Valor entre 0 y 1 #

    # 1. Revisión del Artículo y el Dataset.

    creator = DatasetCreator(FB_PATH, TS_PATH, TWITTER_PATH, DATASET_PATH)
    dataset = creator.load_or_create_dataset(
        use_sample=USE_SAMPLE,
        sample_fraction=SAMPLE_FRACTION,
    )
    creator.print_dataset_summary(dataset)

    # 2. Dataset de Capturas Simuladas (ejemplo)
    simulator = DataSimulator()
    dataset = simulator.simulate_data(dataset)

    # 3. Validar dataset
    validator = DatasetValidator(dataset)
    validator.run_all_validations()

    # 4. Construcción de la Red
    builder = NetworkBuilder(dataset, ts_base_path=TS_PATH, fb_db_path=FB_PATH)
    if os.path.exists(GRAPH_CACHE_PATH):
        print('Cargando grafo desde cache...')
        builder.load_graph(GRAPH_CACHE_PATH)
    else:
        print('Construyendo el grafo...')
        G = builder.build_network()
        builder.save_graph(GRAPH_CACHE_PATH)

    stats = builder.get_network_stats()
    print('\nEstadísticas de la Red:')
    for key, value in stats.items():
        print(f'{key}: {value}')

    # Graficar directamente el subgrafo reducido
    plot_network(builder.G)

    # 5. Análisis Básico de Métricas de Red
    analyzer = BasicAnalyzer(builder.G)
    basic_metrics = analyzer.summarize()
    print('\nMétricas Básicas de la Red:')
    print(basic_metrics)

    # 6. Análisis Profundo de Métricas de Redes
    deep_analyzer = DeepAnalyzer(builder.G)
    deep_metrics = deep_analyzer.analyze()
    print('\nAnálisis Profundo:')
    print(deep_metrics)
