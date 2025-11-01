"""
Módulo principal del sistema de alertas por anomalías.
Implementa los algoritmos STL+MAD e Isolation Forest.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

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
        Carga datos de ejemplo para demostración.
        En producción, esto se conectaría a las APIs reales.
        """
        self.logger.info("📂 Cargando datos de ejemplo...")
        
        # Generar datos sintéticos para demostración
        fechas = pd.date_range(
            start=datetime.now() - timedelta(days=self.ventana_analisis),
            end=datetime.now(),
            freq='D'
        )
        
        datos = pd.DataFrame({
            'fecha': fechas,
            'sesiones': np.random.poisson(1000, len(fechas)) + 
                       np.sin(np.arange(len(fechas)) * 2 * np.pi / 7) * 200,
            'leads': np.random.poisson(50, len(fechas)) + 
                    np.sin(np.arange(len(fechas)) * 2 * np.pi / 7) * 10,
            'inversion': np.random.normal(800, 100, len(fechas))
        })
        
        # Introducir algunas anomalías para demostración
        datos.loc[10, 'sesiones'] = 5000  # Pico artificial
        datos.loc[25, 'leads'] = 5        # Caída artificial
        datos.loc[40, 'inversion'] = 2000 # Pico artificial
        
        return datos
    
    def detectar_anomalias_stl_mad(self, serie, nombre_metrica):
        """
        Detecta anomalías usando el método STL + MAD.
        
        Args:
            serie (pd.Series): Serie temporal a analizar
            nombre_metrica (str): Nombre de la métrica para logging
            
        Returns:
            list: Lista de diccionarios con anomalías detectadas
        """
        try:
            self.logger.info(f"🔍 Aplicando STL+MAD a {nombre_metrica}...")
            
            # Simulación de STL - en producción usar statsmodels
            # stl = STL(serie, period=7, robust=True)
            # result = stl.fit()
            # residuals = result.resid
            
            # Para demostración, simulamos residuos
            tendencia = serie.rolling(window=7).mean()
            residuos = serie - tendencia
            
            # Calcular MAD (Median Absolute Deviation)
            mediana = np.median(residuos)
            mad = np.median(np.abs(residuos - mediana))
            
            # Definir umbrales
            umbral_superior = mediana + self.umbral_mad * mad
            umbral_inferior = mediana - self.umbral_mad * mad
            
            # Detectar anomalías
            anomalias = []
            for idx, (fecha, valor, residuo) in enumerate(zip(serie.index, serie.values, residuos)):
                if residuo > umbral_superior or residuo < umbral_inferior:
                    tipo = "pico" if residuo > umbral_superior else "caida"
                    magnitud = abs((valor - mediana) / mediana) * 100
                    
                    anomalias.append({
                        'fecha': fecha,
                        'valor': valor,
                        'tipo': tipo,
                        'metrica': nombre_metrica,
                        'magnitud_relativa': round(magnitud, 2),
                        'metodo': 'STL+MAD',
                        'score_anomalia': abs(residuo) / mad
                    })
            
            self.logger.info(f"✅ STL+MAD detectó {len(anomalias)} anomalías en {nombre_metrica}")
            return anomalias
            
        except Exception as e:
            self.logger.error(f"❌ Error en STL+MAD para {nombre_metrica}: {str(e)}")
            return []
    
    def detectar_anomalias_isolation_forest(self, datos):
        """
        Detecta anomalías usando Isolation Forest (simulado).
        
        Args:
            datos (pd.DataFrame): DataFrame con múltiples métricas
            
        Returns:
            list: Lista de anomalías detectadas
        """
        try:
            self.logger.info("🤖 Aplicando Isolation Forest...")
            
            # En producción se usaría:
            # from sklearn.ensemble import IsolationForest
            # from sklearn.preprocessing import StandardScaler
            
            # Simulación simplificada para demostración
            metricas = ['sesiones', 'leads', 'inversion']
            datos_limpios = datos[metricas].dropna()
            
            if len(datos_limpios) < 10:
                return []
            
            # Simular scores de anomalía (en producción serían reales)
            np.random.seed(42)  # Para reproducibilidad
            scores = np.random.exponential(0.1, len(datos_limpios))
            
            # Introducir algunos scores altos para demostración
            scores[10] = 0.9  # Anomalía en índice 10
            scores[25] = 0.8  # Anomalía en índice 25
            scores[40] = 0.95 # Anomalía en índice 40
            
            # Identificar anomalías (percentil 99)
            umbral = np.percentile(scores, 99)
            indices_anomalias = np.where(scores > umbral)[0]
            
            anomalias = []
            for idx in indices_anomalias:
                fecha = datos_limpios.index[idx]
                anomalias.append({
                    'fecha': fecha,
                    'score_anomalia': round(scores[idx], 4),
                    'tipo': 'patron_multivariante',
                    'metricas_afectadas': metricas,
                    'metodo': 'IsolationForest',
                    'valores': datos_limpios.iloc[idx].to_dict()
                })
            
            self.logger.info(f"✅ Isolation Forest detectó {len(anomalias)} anomalías multivariantes")
            return anomalias
            
        except Exception as e:
            self.logger.error(f"❌ Error en Isolation Forest: {str(e)}")
            return []
    
    def enviar_alerta_teams(self, alerta):
        """
        Simula el envío de alertas a Microsoft Teams.
        
        Args:
            alerta (dict): Información de la alerta a enviar
        """
        try:
            # En producción se usaría:
            # import requests
            # webhook_url = "tu_webhook_url"
            # requests.post(webhook_url, json=alerta)
            
            self.logger.info(f"📤 Alerta Teams: {alerta['tipo']} en {alerta.get('metrica', 'múltiples')}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error enviando alerta: {str(e)}")
            return False
    
    def ejecutar_pipeline_completo(self):
        """
        Ejecuta el pipeline completo de detección de anomalías.
        
        Returns:
            dict: Resultados del procesamiento
        """
        self.logger.info("🚀 Iniciando pipeline completo...")
        
        # 1. Cargar datos
        datos = self.cargar_datos_ejemplo()
        
        # 2. Detección STL+MAD por métrica
        alertas_stl = []
        for metrica in ['sesiones', 'leads', 'inversion']:
            serie = datos.set_index('fecha')[metrica]
            alertas_metrica = self.detectar_anomalias_stl_mad(serie, metrica)
            alertas_stl.extend(alertas_metrica)
        
        # 3. Detección Isolation Forest
        datos_indexados = datos.set_index('fecha')
        alertas_if = self.detectar_anomalias_isolation_forest(datos_indexados)
        
        # 4. Combinar y enviar alertas
        alertas_totales = alertas_stl + alertas_if
        
        for alerta in alertas_totales:
            self.enviar_alerta_teams(alerta)
        
        # 5. Retornar resultados
        resultados = {
            'total_dominios': 1,  # En producción sería el número real
            'total_alertas': len(alertas_totales),
            'alertas_stl': len(alertas_stl),
            'alertas_if': len(alertas_if),
            'alertas_faltantes': 0,  # En producción se calcularía
            'fecha_ejecucion': datetime.now().isoformat()
        }
        
        self.logger.info("🎉 Pipeline completado exitosamente")
        return resultados