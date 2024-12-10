# core/data_simulator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import string
import random


class DataSimulator:
    """
    Clase mejorada para la fase 3: Dataset de Capturas Simuladas.
    Se encarga de:
    - Normalizar estructuras no válidas (e.g., 'ReTruth' -> 'Reply').
    - Generar distribuciones mínimas por defecto para 'Estructura'.
    - Completar fechas faltantes con rangos plausibles.
    - Generar contenido más variado.
    - Asegurar que no queden valores nulos críticos tras la simulación.
    """

    def simulate_data(self, dataset: pd.DataFrame) -> pd.DataFrame:
        dataset = dataset.copy()

        # Normalizar estructuras no válidas antes de iniciar
        dataset = self._normalize_invalid_structures(dataset)

        # Convertir 'Fecha' a datetime si no lo está
        if not np.issubdtype(dataset['Fecha'].dtype, np.datetime64):
            dataset['Fecha'] = pd.to_datetime(dataset['Fecha'], errors='coerce')

        # Completar Autores Faltantes
        dataset = self._simulate_authors(dataset)

        # Completar Estructura si es 'N/A' en Capturas
        dataset = self._simulate_structure(dataset)

        # Completar Fechas Faltantes
        dataset = self._simulate_dates(dataset)

        # Completar Contenido Faltante en Capturas con mayor variedad
        dataset = self._simulate_content(dataset)

        # Verificación final para asegurar ausencia de nulos en campos críticos de capturas
        dataset = self._final_check(dataset)

        return dataset

    def _normalize_invalid_structures(self, df: pd.DataFrame) -> pd.DataFrame:
        # Mapear estructuras no reconocidas a una estructura válida
        # Por ejemplo, 'ReTruth' no está en el set esperado, lo mapeamos a 'Reply'
        invalid_to_valid_map = {'ReTruth': 'Reply'}

        df['Estructura'] = df['Estructura'].replace(invalid_to_valid_map)
        return df

    def _simulate_authors(self, df: pd.DataFrame) -> pd.DataFrame:
        # Identificar filas sin autor
        mask_no_author = df['Autor'].isna()
        if mask_no_author.any():
            simulated_authors = [f'@usuario_simulado_{i}' for i in range(mask_no_author.sum())]
            df.loc[mask_no_author, 'Autor'] = simulated_authors
        return df

    def _simulate_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        captures = df['Tipo_de_Nodo'] == 'Captura'
        structure_na = (df['Estructura'].isna()) | (df['Estructura'] == 'N/A')

        mask = captures & structure_na
        if not mask.any():
            return df

        possible_structures = ['Status', 'Reply', 'Co-Tweet', 'Cropped Snapshot']

        known_structures = df[captures & ~structure_na]
        if len(known_structures) == 0:
            # Sin datos conocidos, usar una distribución fija
            probs = [0.5, 0.3, 0.1, 0.1]
            chosen_structures = np.random.choice(possible_structures, p=probs, size=mask.sum())
            df.loc[mask, 'Estructura'] = chosen_structures
            return df

        # Calcular distribuciones por plataforma
        platform_groups = known_structures.groupby('Plataforma')['Estructura'].value_counts(
            normalize=True
        )

        def assign_structure(row):
            plat = row['Plataforma']
            # Verificar si hay distribución para la plataforma actual
            if plat in platform_groups.index.levels[0]:
                dist = platform_groups[plat]

                # Si la distribución es muy sesgada (ej: 100% Status),
                # mezclamos con distribución por defecto:
                if len(dist) == 1:
                    # Tomar la estructura única y combinar con una dist mínima
                    dominant_structure = dist.index[0]
                    # Ej: si 100% Status, incorporar un 50% Status y repartir el resto
                    # entre las otras 3 estructuras:
                    fallback_probs = np.array([0.5, 0.3, 0.1, 0.1])
                    # Asignar la dominante con 50% y el resto según fallback_probs
                    # Si la dominante es, por ej, Reply, ajustamos el vector en consecuencia
                    chosen_structs = possible_structures
                    if dominant_structure in chosen_structs:
                        idx = chosen_structs.index(dominant_structure)
                        # Elevar esa estructura a 50%
                        custom_probs = fallback_probs.copy()
                        # Rebalancear para que sumen a 1
                        # Por ejemplo, dejemos la dominante con 50% y distribuyamos el 50% restante
                        # proporcionalmente entre las otras 3 estructuras
                        custom_probs[idx] = 0.5
                        # Normalizar el resto para que sumen 0.5 entre ellos
                        remainder = 1 - 0.5
                        others = [i for i in range(len(custom_probs)) if i != idx]
                        sub_sum = fallback_probs[others].sum()
                        custom_probs[others] = fallback_probs[others] * (remainder / sub_sum)

                        # custom_probs ahora es una distribución más variada
                        structures = possible_structures
                        probs = custom_probs
                    else:
                        # Si por alguna razón la dominante no está en la lista (no debería ocurrir),
                        # usamos dist por defecto
                        structures = possible_structures
                        probs = [0.5, 0.3, 0.1, 0.1]

                else:
                    # Hay varias estructuras, tomar la distribución y, si falta variedad,
                    # mezclamos levemente con la dist por defecto
                    structures = dist.index.values
                    probs = dist.values

                    # Asegurar que las cuatro estructuras tengan al menos cierta representación.
                    # Si hay menos de 4 estructuras, añadimos las que faltan con pequeñas probabilidades
                    missing_structs = set(possible_structures) - set(structures)
                    if missing_structs:
                        # Mezclar dist actual con la dist por defecto
                        default_dist = np.array([0.5, 0.3, 0.1, 0.1])
                        # Crear una distribución base con todas las estructuras
                        full_structs = possible_structures[:]
                        full_probs = np.zeros(4)
                        # Asignar las probs conocidas
                        for s, p in zip(structures, probs):
                            idx = full_structs.index(s)
                            full_probs[idx] = p
                        # Añadir un mínimo para las estructuras faltantes según la dist por defecto
                        for s in missing_structs:
                            idx = full_structs.index(s)
                            full_probs[idx] += (
                                default_dist[idx] * 0.2
                            )  # Añadir un 20% de la dist por defecto

                        # Normalizar
                        full_probs = full_probs / full_probs.sum()
                        structures = full_structs
                        probs = full_probs

            else:
                # Sin datos para esta plataforma, usar dist por defecto
                structures = possible_structures
                probs = [0.5, 0.3, 0.1, 0.1]

            return np.random.choice(structures, p=probs)

        df.loc[mask, 'Estructura'] = df[mask].apply(assign_structure, axis=1)
        return df

    def _simulate_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        mask_no_date = df['Fecha'].isna()
        if not mask_no_date.any():
            return df

        known_dates = df['Fecha'].dropna()
        if len(known_dates) > 0:
            min_date = known_dates.min()
            max_date = known_dates.max()
        else:
            # Rango por defecto
            min_date = datetime(2020, 1, 1)
            max_date = datetime(2024, 12, 31)

        delta = (max_date - min_date).days
        if delta < 1:
            delta = 365
        random_days = np.random.randint(0, delta, size=mask_no_date.sum())
        simulated_dates = [min_date + timedelta(days=int(d)) for d in random_days]

        df.loc[mask_no_date, 'Fecha'] = simulated_dates
        return df

    def _simulate_content(self, df: pd.DataFrame) -> pd.DataFrame:
        captures = df['Tipo_de_Nodo'] == 'Captura'
        mask_no_content = df['Contenido'].isna() & captures

        if not mask_no_content.any():
            return df

        structure_templates = {
            'Status': 'Este es un post individual simulado.',
            'Reply': 'Esta es una respuesta simulada.',
            'Co-Tweet': 'Este es un post colaborativo simulado.',
            'Cropped Snapshot': 'Esta es una captura recortada simulada.',
        }
        default_content = 'Este es un contenido simulado genérico.'

        def random_words(num_words=10):
            # Generar palabras aleatorias para aumentar variedad
            words = []
            for _ in range(num_words):
                word_length = np.random.randint(3, 10)
                word = ''.join(random.choice(string.ascii_lowercase) for _ in range(word_length))
                words.append(word)
            return ' '.join(words)ñ

        def assign_content(row):
            struct = row['Estructura']
            base = structure_templates.get(struct, default_content)
            # Añadir entre 5 y 20 "palabras" adicionales para variar longitud
            extra_words = random_words(np.random.randint(5, 20))
            return f'{base} {extra_words}'

        df.loc[mask_no_content, 'Contenido'] = df[mask_no_content].apply(assign_content, axis=1)
        return df

    def _final_check(self, df: pd.DataFrame) -> pd.DataFrame:
        # Asegurar que no queden nulos críticos en capturas: Estructura y Contenido
        captures = df['Tipo_de_Nodo'] == 'Captura'
        # Estructura
        df.loc[captures & (df['Estructura'].isna()), 'Estructura'] = 'Status'
        # Contenido
        df.loc[captures & (df['Contenido'].isna()), 'Contenido'] = (
            'Este es un contenido simulado genérico adicional.'
        )

        return df
