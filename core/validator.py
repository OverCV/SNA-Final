# core\validator.py
import pandas as pd


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

        # Nulos por columna
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

    def validate_temporal_consistency(self, dataset):
        print('\n=== Validación Temporal ===')
        temporal_issues = []
        for _, row in dataset.iterrows():
            if row['Tipo_de_Nodo'] == 'Captura':
                if pd.isna(row['Fecha']):
                    # Podríamos considerarlo un issue si no hay fecha
                    pass
                else:
                    # Checar fechas futuras o muy antiguas
                    if row['Fecha'] > pd.Timestamp.now():
                        temporal_issues.append(f"Fecha futura en captura {row['Nodo']}")
                    if row['Fecha'].year < 1970:  # Por ejemplo, filtrar fechas imposibles
                        temporal_issues.append(f"Fecha muy antigua en captura {row['Nodo']}")
        if not temporal_issues:
            print('No se encontraron problemas temporales graves.')
        return temporal_issues

    def analyze_temporal_distribution(self):
        """Analiza la distribución temporal de los datos"""
        print('\n=== Análisis Temporal ===')

        # Convertimos a datetime si no lo está ya
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
            print('⚠ No se encontraron fechas válidas en el dataset')

    def analyze_content_length(self):
        """Analiza la longitud del contenido por plataforma"""
        print('\n=== Análisis de Longitud de Contenido ===')

        # Calculamos longitudes solo para registros tipo 'Captura'
        captures = self.dataset[self.dataset['Tipo_de_Nodo'] == 'Captura']
        captures['content_length'] = captures['Contenido'].fillna('').astype(str).apply(len)

        print('\nEstadísticas de longitud por plataforma:')
        stats = captures.groupby('Plataforma')['content_length'].describe()
        print(stats)

    def run_all_validations(self):
        """Ejecuta todas las validaciones"""
        print('🔍 Iniciando validación completa del dataset...')
        print(f'Total de registros: {len(self.dataset)}')

        self.validate_schema()
        self.check_null_values()
        self.analyze_content_distribution()
        self.analyze_temporal_distribution()
        self.analyze_content_length()
