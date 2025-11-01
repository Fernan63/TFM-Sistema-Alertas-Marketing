"""
Utilidades auxiliares para el sistema de alertas.
"""
import json
import yaml
from datetime import datetime
import logging

def cargar_configuracion(ruta_archivo='config/parameters.yaml'):
    """
    Carga la configuración desde archivo YAML.
    
    Args:
        ruta_archivo (str): Ruta al archivo de configuración
        
    Returns:
        dict: Configuración cargada
    """
    try:
        with open(ruta_archivo, 'r') as archivo:
            config = yaml.safe_load(archivo)
        logging.info("✅ Configuración cargada exitosamente")
        return config
    except Exception as e:
        logging.warning(f"⚠️ No se pudo cargar configuración: {e}")
        return {}

def guardar_resultados(resultados, ruta_archivo='resultados_ejecucion.json'):
    """
    Guarda los resultados de la ejecución en formato JSON.
    
    Args:
        resultados (dict): Resultados a guardar
        ruta_archivo (str): Ruta donde guardar los resultados
    """
    try:
        with open(ruta_archivo, 'w') as archivo:
            json.dump(resultados, archivo, indent=2, default=str)
        logging.info(f"💾 Resultados guardados en {ruta_archivo}")
    except Exception as e:
        logging.error(f"❌ Error guardando resultados: {e}")

def formatear_alerta_teams(alerta):
    """
    Formatea una alerta para enviar a Microsoft Teams.
    
    Args:
        alerta (dict): Alerta a formatear
        
    Returns:
        dict: Mensaje formateado para Teams
    """
    if alerta['metodo'] == 'STL+MAD':
        color = "FFA500" if alerta['tipo'] == 'pico' else "008000"
        emoji = "📈" if alerta['tipo'] == 'pico' else "📉"
        
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": f"Alerta - {alerta['tipo']} - {alerta['metrica']}",
            "sections": [{
                "activityTitle": f"{emoji} ALERTA - {alerta['tipo'].upper()} DETECTADA",
                "facts": [
                    {"name": "Métrica:", "value": alerta['metrica']},
                    {"name": "Fecha:", "value": alerta['fecha'].strftime("%Y-%m-%d")},
                    {"name": "Magnitud:", "value": f"{alerta['magnitud_relativa']}%"},
                    {"name": "Método:", "value": alerta['metodo']}
                ]
            }]
        }
    
    else:  # Isolation Forest
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FF0000",
            "summary": "Alerta - Patrón Multivariante",
            "sections": [{
                "activityTitle": "🤖 ALERTA - PATRÓN MULTIVARIANTE",
                "facts": [
                    {"name": "Score:", "value": f"{alerta['score_anomalia']}"},
                    {"name": "Métricas:", "value": ", ".join(alerta['metricas_afectadas'])},
                    {"name": "Fecha:", "value": alerta['fecha'].strftime("%Y-%m-%d")}
                ]
            }]
        }

def validar_datos(dataframe, metricas_requeridas):
    """
    Valida que el DataFrame tenga las columnas requeridas.
    
    Args:
        dataframe (pd.DataFrame): DataFrame a validar
        metricas_requeridas (list): Lista de columnas requeridas
        
    Returns:
        bool: True si los datos son válidos
    """
    try:
        columnas_faltantes = [col for col in metricas_requeridas if col not in dataframe.columns]
        if columnas_faltantes:
            logging.error(f"❌ Columnas faltantes: {columnas_faltantes}")
            return False
        
        if dataframe.empty:
            logging.error("❌ DataFrame vacío")
            return False
        
        # Verificar que no haya fechas duplicadas
        if dataframe['fecha'].duplicated().any():
            logging.warning("⚠️ Fechas duplicadas detectadas")
            
        logging.info("✅ Validación de datos exitosa")
        return True
        
    except Exception as e:
        logging.error(f"❌ Error en validación: {e}")
        return False