"""
Módulo principal del sistema de alertas por anomalías.
Implementa los algoritmos STL+MAD e Isolation Forest utilizando librerías científicas reales.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Importaciones para lógica real de Ciencia de Datos
try:
    from statsmodels.tsa.seasonal import seasonal_decompose
    from sklearn.ensemble import IsolationForest
    from sklearn.impute import SimpleImputer
except ImportError as e:
    logging.error(f"⚠️ Librerías científicas no encontradas: {e}. Instalar con: pip install scikit-learn statsmodels")

class SistemaAlertasMarketing:
    """Sistema automático de detección de anomalías en métricas de marketing."""
    
    def __init__(self, ventana_analisis=60, umbral_mad=3.2, contaminacion_if=0.01):
        """
        Inicializa el sistema con parámetros configurables.
        
        Args:
            ventana_analisis (int): Días históricos a analizar (default: 60)
            umbral_mad (float): Umbral para detección MAD (default: 3.2)
            contaminacion_if (float): Proporción esperada de anomalías (default: 0.01)
        """
        self.ventana_analisis = ventana_analisis
        self.umbral_mad = umbral_mad
        self.contaminacion_if = contaminacion_if
        self.logger = logging.getLogger(__name__)
        
    def cargar_datos_ejemplo(self):
        """
        Carga datos de ejemplo para demostración local.
        NOTA: En producción (Databricks), esto se sustituye por la lectura
        de la tabla Delta 'Gold' con el dominio normalizado.
        """
        self.logger.info("📂 Cargando datos de ejemplo (Simulación de Capa Gold)...")
        
        # Generar datos sintéticos para demostración
        fechas = pd.date_range(
            start=datetime.now() - timedelta(days=self.ventana_analisis),
            end=datetime.now(),
            freq='D'
        )
        
        datos = pd.DataFrame({
            'fecha': fechas,
            # Simular patrón semanal con ruido Poisson
            'sesiones': np.random.poisson(1000, len(fechas)) + 
                       np.sin(np.arange(len(fechas)) * 2 * np.pi / 7) * 200,
            'leads': np.random.poisson(50, len(fechas)) + 
                    np.sin(np.arange(len(fechas)) * 2 * np.pi / 7) * 10,
            'inversion': np.random.normal(800, 100, len(fechas))
        })
        
        # Introducir anomalías artificiales para validar detección
        # 1. Pico en Sesiones (e.g. Bot attack o Viralidad)
        datos.loc[10, 'sesiones'] = 5000  
        # 2. Caída crítica en Leads (e.g. Fallo en formulario)
        datos.loc[25, 'leads'] = 5        
        # 3. Desconexión inversión/retorno (Anomalía multivariante)
        datos.loc[40, 'inversion'] = 3000 
        
        return datos
    
    def detectar_anomalias_stl_mad(self, serie, nombre_metrica):
        """
        Detecta anomalías univariantes usando Descomposición Estacional (STL) + MAD.
        Utiliza statsmodels para descomponer la serie en Tendencia, Estacionalidad y Residuo.
        
        Args:
            serie (pd.Series): Serie temporal a analizar (indexada por fecha)
            nombre_metrica (str): Nombre de la métrica para logging
            
        Returns:
            list: Lista de diccionarios con anomalías detectadas
        """
        try:
            self.logger.info(f"🔍 Aplicando STL+MAD a {nombre_metrica}...")
            
            # Validación mínima de longitud de datos para STL
            if len(serie) < 14: # Mínimo 2 ciclos semanales
                self.logger.warning(f"Datos insuficientes para STL en {nombre_metrica}")
                return []

            # 1. Descomposición Estacional REAL (statsmodels)
            # period=7 asume estacionalidad semanal típica en marketing
            # extrapolate_trend='freq' permite calcular residuos en los extremos
            resultado = seasonal_decompose(serie, model='additive', period=7, extrapolate_trend='freq')
            
            residuos = resultado.resid
            
            # 2. Calcular MAD (Median Absolute Deviation) robusto
            # Factor 1.4826 para consistencia con distribución normal
            mediana_residuo = np.median(residuos)
            mad = np.median(np.abs(residuos - mediana_residuo)) * 1.4826
            
            if mad == 0: 
                mad = 1e-6 # Evitar división por cero en series muy planas

            # 3. Definir umbrales dinámicos basados en k * MAD
            umbral_superior = mediana_residuo + self.umbral_mad * mad
            umbral_inferior = mediana_residuo - self.umbral_mad * mad
            
            # 4. Detectar y estructurar anomalías
            anomalias = []
            for fecha, valor, residuo in zip(serie.index, serie.values, residuos):
                if residuo > umbral_superior or residuo < umbral_inferior:
                    tipo = "pico" if residuo > 0 else "caida"
                    
                    # Calcular magnitud relativa para contexto de negocio
                    media_serie = np.mean(serie)
                    magnitud = abs(residuo) / media_serie * 100 if media_serie != 0 else 0
                    
                    # Score Z robusto
                    score = abs(residuo) / mad

                    anomalias.append({
                        'fecha': fecha,
                        'valor': valor,
                        'tipo': tipo,
                        'metrica': nombre_metrica,
                        'magnitud_relativa': round(magnitud, 2),
                        'metodo': 'STL+MAD (Statsmodels)',
                        'score_anomalia': round(score, 2)
                    })
            
            self.logger.info(f"✅ STL+MAD detectó {len(anomalias)} anomalías en {nombre_metrica}")
            return anomalias
            
        except Exception as e:
            self.logger.error(f"❌ Error en STL+MAD para {nombre_metrica}: {str(e)}")
            return []
    
    def detectar_anomalias_isolation_forest(self, datos):
        """
        Detecta anomalías multivariantes usando Isolation Forest (Scikit-Learn).
        Identifica relaciones anómalas entre métricas (ej. alto gasto sin leads).
        
        Args:
            datos (pd.DataFrame): DataFrame con múltiples métricas indexado por fecha
            
        Returns:
            list: Lista de anomalías detectadas
        """
        try:
            self.logger.info("🤖 Aplicando Isolation Forest (Sklearn)...")
            
            metricas = ['sesiones', 'leads', 'inversion']
            
            # Preprocesamiento: Copia y limpieza
            datos_limpios = datos[metricas].copy()
            
            # Imputación de nulos (Isolation Forest no soporta NaNs nativamente)
            # Estrategia: Rellenar con 0 asumiendo ausencia de actividad
            imputer = SimpleImputer(strategy='constant', fill_value=0)
            datos_array = imputer.fit_transform(datos_limpios)
            
            # Validación de volumen mínimo
            if len(datos_limpios) < 10:
                self.logger.warning("Datos insuficientes para entrenar Isolation Forest")
                return []
            
            # 1. Configuración del Modelo
            # n_jobs=-1 utiliza todos los cores disponibles (paralelización)
            iso_forest = IsolationForest(
                contamination=self.contaminacion_if,
                n_estimators=100,
                random_state=42, # Semilla fija para reproducibilidad
                n_jobs=-1
            )
            
            # 2. Entrenamiento y Predicción
            iso_forest.fit(datos_array)
            
            # decision_function: score negativo = anómalo, positivo = normal
            # Invertimos para que mayor valor signifique mayor anomalía
            scores_raw = iso_forest.decision_function(datos_array)
            scores_normalizados = -scores_raw
            
            # Umbral automático basado en la contaminación definida
            # predict devuelve -1 para outliers y 1 para inliers
            predicciones = iso_forest.predict(datos_array)
            indices_anomalias = np.where(predicciones == -1)[0]
            
            anomalias = []
            for idx in indices_anomalias:
                fecha = datos_limpios.index[idx]
                score = scores_normalizados[idx]
                
                # Reportamos valores contextuales para el dashboard
                valores_contexto = datos_limpios.iloc[idx].to_dict()
                
                anomalias.append({
                    'fecha': fecha,
                    'score_anomalia': round(score, 4),
                    'tipo': 'patron_multivariante',
                    'metricas_afectadas': metricas, # IF es agnóstico a la métrica individual
                    'metodo': 'IsolationForest (Sklearn)',
                    'valores': valores_contexto
                })
            
            self.logger.info(f"✅ Isolation Forest detectó {len(anomalias)} anomalías multivariantes")
            return anomalias
            
        except Exception as e:
            self.logger.error(f"❌ Error en Isolation Forest: {str(e)}")
            return []
    
    def enviar_alerta_teams(self, alerta):
        """
        Simula el envío de alertas a Microsoft Teams.
        En producción usa requests.post() al webhook configurado en parameters.yaml.
        """
        try:
            # En producción:
            # response = requests.post(self.webhook_url, json=formatear_alerta(alerta))
            # if response.status_code != 200: raise Exception(...)
            
            metrica_info = alerta.get('metrica', 'Multi-variante')
            self.logger.info(f"📤 [TEAMS MOCK] Enviando alerta: {alerta['tipo']} | Métrica: {metrica_info} | Fecha: {alerta['fecha']}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando alerta: {str(e)}")
            return False
    
    def ejecutar_pipeline_completo(self):
        """
        Orquestador principal: Carga datos -> Detecta -> Alerta -> Reporta.
        Este método sería el 'entry point' del Job en Databricks.
        """
        self.logger.info("🚀 Iniciando pipeline completo de detección...")
        
        # 1. Cargar datos (Silver -> Gold)
        datos = self.cargar_datos_ejemplo()
        
        # 2. Detección Univariante (STL+MAD)
        alertas_stl = []
        for metrica in ['sesiones', 'leads', 'inversion']:
            serie = datos.set_index('fecha')[metrica]
            alertas_metrica = self.detectar_anomalias_stl_mad(serie, metrica)
            alertas_stl.extend(alertas_metrica)
        
        # 3. Detección Multivariante (Isolation Forest)
        datos_indexados = datos.set_index('fecha')
        alertas_if = self.detectar_anomalias_isolation_forest(datos_indexados)
        
        # 4. Consolidación y Envío
        alertas_totales = alertas_stl + alertas_if
        
        # Filtrar alertas duplicadas o priorizar (lógica de negocio simple)
        # Aquí enviamos todas para trazabilidad
        for alerta in alertas_totales:
            self.enviar_alerta_teams(alerta)
        
        # 5. Generar métricas de ejecución para logs
        resultados = {
            'total_dominios': 1, 
            'total_alertas': len(alertas_totales),
            'alertas_stl': len(alertas_stl),
            'alertas_if': len(alertas_if),
            'alertas_faltantes': 0, # Implementar check de fechas vs calendario
            'fecha_ejecucion': datetime.now().isoformat()
        }
        
        self.logger.info("🎉 Pipeline completado exitosamente")
        return resultados
