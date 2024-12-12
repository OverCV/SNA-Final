# core/validator.py
import pandas as pd
import numpy as np


class DatasetValidator:
    def __init__(self, dataset):
        self.dataset = dataset
        self.columns = [
            'Nodo',
            'Tipo_de_Nodo',
            'Plataforma',
            'Estructura',
            'Autor',
            'Fecha',
            'Contenido',
        ]

    def validate_schema(self):
        print('\n=== Validación de Esquema ===')
        missing_cols = set(self.columns) - set(self.dataset.columns)
        extra_cols = set(self.dataset.columns) - set(self.columns)

        if not missing_cols and not extra_cols:
            print('✓ El esquema es correcto')
        if missing_cols:
            print(f'⚠ Columnas faltantes: {missing_cols}')
        if extra_cols:
            print(f'⚠ Columnas extra: {extra_cols}')

    def check_null_values(self):
        """Analiza valores nulos por columna y plataforma"""
        print('\n=== Análisis de Valores Nulos ===')

        null_counts = self.dataset.isna().sum()
        null_percentages = (null_counts / len(self.dataset)) * 100

        print('\nPorcentaje de nulos por columna:')
        for col in self.columns:
            perc = null_percentages[col]
            print(f'{col}: {perc:.2f}% ({null_counts[col]} registros)')

        # Nulos por plataforma
        print('\nPorcentaje de nulos por plataforma:')
        platforms = self.dataset['Plataforma'].unique()
        for platform in platforms:
            platform_data = self.dataset[self.dataset['Plataforma'] == platform]
            platform_nulls = platform_data.isna().sum()
            platform_null_perc = (platform_nulls / len(platform_data)) * 100
            print(f'\n{platform}:')
            for col in self.columns:
                print(f'  {col}: {platform_null_perc[col]:.2f}% ({platform_nulls[col]} registros)')

    def analyze_content_distribution(self):
        """Analiza la distribución de contenido por tipo y plataforma"""
        print('\n=== Análisis de Distribución ===')

        # Distribución por tipo de nodo y plataforma
        platform_node_dist = (
            pd.crosstab(self.dataset['Plataforma'], self.dataset['Tipo_de_Nodo'], normalize='index')
            * 100
        )

        print('\nDistribución por tipo de nodo y plataforma (%):')
        print(platform_node_dist)

        # Distribución de estructuras por plataforma
        platform_struct_dist = (
            pd.crosstab(self.dataset['Plataforma'], self.dataset['Estructura'], normalize='index')
            * 100
        )

        print('\nDistribución de estructuras por plataforma (%):')
        print(platform_struct_dist)

    def validate_temporal_consistency(self):
        print('\n=== Validación Temporal ===')
        temporal_issues = []
        # Asegurar conversión a datetime
        self.dataset['Fecha'] = pd.to_datetime(self.dataset['Fecha'], errors='coerce')
        for _, row in self.dataset.iterrows():
            if row['Tipo_de_Nodo'] == 'Captura':
                if pd.isna(row['Fecha']):
                    temporal_issues.append(f"Captura {row['Nodo']} sin fecha")
                else:
                    # Checar fechas muy futuristas o muy antiguas (ejemplo)
                    if row['Fecha'] > pd.Timestamp.now() + pd.Timedelta(days=30):
                        temporal_issues.append(f"Fecha futura no realista en captura {row['Nodo']}")
                    if row['Fecha'].year < 1970:
                        temporal_issues.append(f"Fecha muy antigua en captura {row['Nodo']}")
        if not temporal_issues:
            print('No se encontraron problemas temporales graves.')
        else:
            print('⚠ Problemas temporales detectados:')
            for issue in temporal_issues:
                print(' - ' + issue)
        return temporal_issues

    def analyze_temporal_distribution(self):
        """Analiza la distribución temporal de los datos"""
        print('\n=== Análisis Temporal ===')

        date_data = pd.to_datetime(self.dataset['Fecha'], errors='coerce')
        valid_dates = date_data.dropna()

        if len(valid_dates) > 0:
            print(f'\nRango de fechas:')
            print(f'Fecha más antigua: {valid_dates.min()}')
            print(f'Fecha más reciente: {valid_dates.max()}')
            print(f'Rango total: {(valid_dates.max() - valid_dates.min()).days} días')

            # Distribución por plataforma
            print('\nRango de fechas por plataforma:')
            for platform in self.dataset['Plataforma'].unique():
                platform_dates = date_data[self.dataset['Plataforma'] == platform].dropna()
                if len(platform_dates) > 0:
                    print(f'\n{platform}:')
                    print(f'  Desde: {platform_dates.min()}')
                    print(f'  Hasta: {platform_dates.max()}')
                    print(f'  Rango: {(platform_dates.max() - platform_dates.min()).days} días')
                else:
                    print(f'\n{platform}: No hay fechas válidas')
        else:
            print('⚠ No se encontraron fechas válidas en el dataset')

    def analyze_content_length(self):
        """Analiza la longitud del contenido por plataforma"""
        print('\n=== Análisis de Longitud de Contenido ===')

        captures = self.dataset[self.dataset['Tipo_de_Nodo'] == 'Captura'].copy()
        captures['content_length'] = captures['Contenido'].fillna('').astype(str).apply(len)

        if len(captures) > 0:
            stats = captures.groupby('Plataforma')['content_length'].describe()
            print('\nEstadísticas de longitud por plataforma:')
            print(stats)
        else:
            print('No hay capturas para analizar la longitud de contenido.')

    def check_value_domains(self):
        """Verifica que valores categóricos estén dentro de los dominios esperados"""
        print('\n=== Validación de Dominios de Valores ===')

        # Validar Tipo_de_Nodo
        expected_types = ['Usuario', 'Captura']
        invalid_types = self.dataset[~self.dataset['Tipo_de_Nodo'].isin(expected_types)]
        if len(invalid_types) > 0:
            print('⚠ Se encontraron tipos de nodo no válidos:')
            print(invalid_types['Tipo_de_Nodo'].unique())
        else:
            print('✓ Todos los nodos tienen tipos válidos.')

        # Validar Plataforma
        # Asumiendo que las plataformas posibles son: Twitter, Facebook, Instagram, Truth Social
        expected_platforms = ['Twitter', 'Facebook', 'Instagram', 'Truth Social']
        invalid_platforms = self.dataset[~self.dataset['Plataforma'].isin(expected_platforms)]
        if len(invalid_platforms) > 0:
            print('⚠ Se encontraron plataformas no válidas:')
            print(invalid_platforms['Plataforma'].unique())
        else:
            print('✓ Todas las plataformas son válidas.')

        # Validar Estructura en Capturas
        # Estructuras esperadas: Status, Reply, Co-Tweet, Cropped Snapshot
        captures = self.dataset[self.dataset['Tipo_de_Nodo'] == 'Captura']
        expected_structures = ['Status', 'Reply', 'Co-Tweet', 'Cropped Snapshot']
        invalid_struct = captures[~captures['Estructura'].isin(expected_structures)]
        if len(invalid_struct) > 0:
            print('⚠ Se encontraron estructuras no válidas en capturas:')
            print(invalid_struct['Estructura'].unique())
        else:
            print('✓ Todas las capturas tienen estructuras válidas.')

        # Validar que Autor no sea nulo
        if self.dataset['Autor'].isna().any():
            print('⚠ Hay nodos sin Autor asignado.')
        else:
            print('✓ Todos los nodos tienen Autor.')

        # Validar que Contenido no sea nulo en capturas
        if captures['Contenido'].isna().any():
            print('⚠ Hay capturas sin contenido.')
        else:
            print('✓ Todas las capturas tienen contenido.')

    def run_all_validations(self):
        """Ejecuta todas las validaciones"""
        print('🔍 Iniciando validación completa del dataset...')
        print(f'Total de registros: {len(self.dataset)}')

        # Paso 1: Validar Esquema
        self.validate_schema()

        # Paso 2: Análisis de Nulos
        self.check_null_values()

        # Paso 3: Análisis de Distribución (Nodos, Estructuras)
        self.analyze_content_distribution()

        # Paso 4: Validación y Análisis Temporal
        self.validate_temporal_consistency()
        self.analyze_temporal_distribution()

        # Paso 5: Análisis de Longitud de Contenido
        self.analyze_content_length()

        # Paso 6: Validar Dominios de Valores (Plataformas, Estructuras, etc.)
        self.check_value_domains()

        print('\n✅ Validaciones Completadas.')
