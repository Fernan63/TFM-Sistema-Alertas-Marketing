# COMMAND ----------
# ============================================================
# CELDA 0: CONFIGURACIÓN Y DATOS SENSIBLES (PRIVADO)
# ============================================================
# Edita esta celda con tus datos reales. 
# El resto del código leerá de aquí sin exponer secretos.

config = {
    # --- CREDENCIALES Y URLS ---
    "webhook_teams": "https://tu-organizacion.webhook.office.com/webhookb2/...",  # <--- PEGA TU WEBHOOK AQUÍ
    "dashboard_url": "https://app.powerbi.com/groups/me/reports/...",             # <--- PEGA TU LINK DE POWER BI AQUÍ
    
    # --- NOMBRES DE TABLAS EN DATA LAKE ---
    "tabla_ga4": "production.digitalmetrics_ga4.digitalmetrics_vw",
    "tabla_facebook": "development.digital_media_analytics_temp.dma_windsor_facebookads_daily",
    "tabla_google": "development.digital_media_analytics_temp.dma_windsor_googleads_daily",
    
    # --- PARÁMETROS DE NEGOCIO ---
    "dias_historico": 65,
    "zona_horaria": "America/Bogota",
    
    # --- HIPERPARÁMETROS MODELO (TUNING) ---
    "umbral_mad": 3.5,        # Sensibilidad STL (Más alto = menos alertas)
    "contaminacion_if": 0.01  # Sensibilidad Isolation Forest (Más bajo = menos alertas)
}

print("🔒 Configuración cargada correctamente. Los secretos están en memoria.")

# COMMAND ----------
# ============================================================
# CELDA 1: GESTIÓN DE DEPENDENCIAS
# ============================================================
try:
    import sklearn
    import statsmodels
    import pytz 
    print("✅ Librerías científicas ya instaladas.")
except ImportError:
    print("⚠️ Instalando dependencias...")
    %pip install scikit-learn statsmodels pytz
    print("✅ Instalación completada. Reiniciando kernel...")
    dbutils.library.restartPython()

# COMMAND ----------
# ============================================================
# CELDA 2: CLASE DE MODELADO (BLINDADA)
# ============================================================
import pandas as pd
import numpy as np
import logging
import requests
import json
import pytz
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Configurar Fecha de Corte usando la Zona Horaria de la Configuración
tz = pytz.timezone(config['zona_horaria'])
FECHA_CORTE_ANALISIS = (datetime.now(tz) - timedelta(days=1)).date()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TFM_Secure")

print(f"📅 Fecha de Análisis (Ayer en {config['zona_horaria']}): {FECHA_CORTE_ANALISIS}")

class SistemaAlertasMarketing:
    def __init__(self, fecha_corte, umbral_mad=3.2, contaminacion_if=0.02):
        self.umbral_mad = umbral_mad
        self.contaminacion_if = contaminacion_if
        self.fecha_objetivo = fecha_corte
        
    def detectar_stl_mad(self, df_pandas, metrica):
        try:
            serie = df_pandas.set_index('fecha')[metrica].sort_index()
            if serie.empty: return []
            
            # Validación estricta de fecha
            idx_max = serie.index.max()
            fecha_ultimo = idx_max.date() if isinstance(idx_max, datetime) else idx_max
            
            if fecha_ultimo != self.fecha_objetivo:
                return []

            if len(serie) < 14: return []
            
            # Importación Lazy para ahorrar memoria si no se usa
            from statsmodels.tsa.seasonal import seasonal_decompose
            
            res = seasonal_decompose(serie, model='additive', period=7, extrapolate_trend='freq')
            residuos = res.resid.dropna()
            
            mediana = np.median(residuos)
            mad = np.median(np.abs(residuos - mediana)) * 1.4826
            if mad == 0: mad = 1e-6
            
            umbral_sup = mediana + self.umbral_mad * mad
            umbral_inf = mediana - self.umbral_mad * mad
            
            if idx_max in residuos.index:
                residuo_hoy = residuos.loc[idx_max]
                valor_hoy = serie.loc[idx_max]
                
                if residuo_hoy > umbral_sup or residuo_hoy < umbral_inf:
                    return [{
                        'fecha': idx_max.strftime('%Y-%m-%d'),
                        'metrica': metrica,
                        'tipo': "Pico Inusual" if residuo_hoy > 0 else "Caída Abrupta",
                        'valor': float(valor_hoy),
                        'metodo': 'STL+MAD',
                        'score': round(abs(residuo_hoy) / mad, 2),
                        'params': f"MAD={self.umbral_mad}"
                    }]
            return []
        except Exception as e:
            logger.error(f"Error STL {metrica}: {e}")
            return []

    def detectar_isolation_forest(self, df_pandas):
        try:
            if df_pandas.empty: return []
            
            from sklearn.ensemble import IsolationForest
            
            df_pandas['fecha_dt'] = pd.to_datetime(df_pandas['fecha'])
            df_filtrado = df_pandas[df_pandas['fecha_dt'].dt.date <= self.fecha_objetivo].copy()
            
            if df_filtrado.empty: return []
            
            if df_filtrado['fecha_dt'].max().date() != self.fecha_objetivo:
                return []
            
            metricas = ['inversion_total', 'leads_total', 'sesiones_total']
            df_model = df_filtrado[metricas].fillna(0)
            
            if len(df_model) < 20: return []
            
            iso = IsolationForest(contamination=self.contaminacion_if, random_state=42, n_jobs=-1)
            df_model['anomaly'] = iso.fit_predict(df_model)
            
            if df_model.iloc[-1]['anomaly'] == -1:
                return [{
                    'fecha': df_filtrado.iloc[-1]['fecha'].strftime('%Y-%m-%d'),
                    'metrica': 'Multivariante',
                    'tipo': 'Patrón Irregular (Gasto/Tráfico)',
                    'valor': 'N/A',
                    'metodo': 'Isolation Forest',
                    'score': 'N/A',
                    'params': f"Contam={self.contaminacion_if}"
                }]
            return []
        except Exception as e:
            logger.error(f"Error IF: {e}")
            return []

print("✅ Lógica de negocio inicializada.")

# COMMAND ----------
# ============================================================
# CELDA 3: ETL PARAMETRIZADO (SIN DATOS DUROS)
# ============================================================
print("🚀 Iniciando ETL...")

# Mock de diccionario si no existe
if 'df_diccionario' not in locals():
    print("⚠️ Usando diccionario mock para demo...")
    data_mock = {'AccountID': ['123'], 'Dominios Normalizado': ['test.com']}
    df_diccionario = pd.DataFrame(data_mock)

df_diccionario_spark = spark.createDataFrame(df_diccionario)
df_diccionario_norm = df_diccionario_spark.select(
    F.regexp_replace(F.col("AccountID").cast("string"), r"[^0-9]", "").alias("account_id_norm"),
    F.col("Dominios Normalizado").alias("dominio_normalizado")
).dropDuplicates(["account_id_norm"])

# Preparamos variables para SQL injection segura
fecha_sql = FECHA_CORTE_ANALISIS.strftime('%Y-%m-%d')
dias = config['dias_historico']

print(f"📥 Consultando tablas: {config['tabla_ga4']} ...")

# Queries usando f-strings con las variables de CONFIG
df_ga4 = spark.sql(f"""
    SELECT REGEXP_REPLACE(CAST(propertyID AS STRING),'[^0-9]','') AS account_id_norm,
    DATE(date) AS fecha, CAST(SUM(TotalConversions) AS INT) AS leads, CAST(SUM(TotalSessions) AS INT) AS sesiones
    FROM {config['tabla_ga4']} 
    WHERE DATE(date) >= DATE_SUB(DATE('{fecha_sql}'), {dias})
      AND DATE(date) <= DATE('{fecha_sql}')
    GROUP BY REGEXP_REPLACE(CAST(propertyID AS STRING), '[^0-9]', ''), DATE(date)
""")

df_facebook = spark.sql(f"""
    SELECT REGEXP_REPLACE(CAST(account_id AS STRING), '[^0-9]', '') AS account_id_norm,
    DATE(date) AS fecha, CAST(SUM(spend_gbp) AS INT) AS spend
    FROM {config['tabla_facebook']}
    WHERE DATE(date) >= DATE_SUB(DATE('{fecha_sql}'), {dias})
      AND DATE(date) <= DATE('{fecha_sql}')
    GROUP BY REGEXP_REPLACE(CAST(account_id AS STRING), '[^0-9]', ''), DATE(date)
""")

df_googleads = spark.sql(f"""
    SELECT REGEXP_REPLACE(CAST(account_id AS STRING), '[^0-9]', '') AS account_id_norm,
    DATE(date) AS fecha, CAST(SUM(spend_gbp) AS INT) AS spend
    FROM {config['tabla_google']}
    WHERE DATE(date) >= DATE_SUB(DATE('{fecha_sql}'), {dias})
      AND DATE(date) <= DATE('{fecha_sql}')
    GROUP BY REGEXP_REPLACE(CAST(account_id AS STRING), '[^0-9]', ''), DATE(date)
""")

# Joins
df_ga4_enr = df_ga4.join(df_diccionario_norm, "account_id_norm", "left") \
    .groupBy("dominio_normalizado", "fecha").agg(F.sum("leads").alias("leads"), F.sum("sesiones").alias("sesiones"))

df_inv_total = df_facebook.join(df_diccionario_norm, "account_id_norm", "left").select("dominio_normalizado", "fecha", "spend") \
    .unionByName(df_googleads.join(df_diccionario_norm, "account_id_norm", "left").select("dominio_normalizado", "fecha", "spend")) \
    .groupBy("dominio_normalizado", "fecha").agg(F.sum("spend").alias("inversion_total"))

df_final = df_inv_total.join(df_ga4_enr, ["dominio_normalizado", "fecha"], "full") \
    .fillna(0, subset=["inversion_total", "leads", "sesiones"]) \
    .withColumnRenamed("leads", "leads_total") \
    .withColumnRenamed("sesiones", "sesiones_total") \
    .filter(F.col("dominio_normalizado").isNotNull()) \
    .orderBy("dominio_normalizado", "fecha")

count = df_final.count()
print(f"✅ ETL Completado. {count} registros listos.")

# COMMAND ----------
# ============================================================
# CELDA 4: TUNING (LEE DE CONFIG)
# ============================================================
print(f"🧪 Iniciando detección (MAD={config['umbral_mad']}, Contam={config['contaminacion_if']})...")

lista_dominios = [row.dominio_normalizado for row in df_final.select("dominio_normalizado").distinct().collect()]
resultados_alertas = []

for dominio in lista_dominios:
    df_pd = df_final.filter(F.col("dominio_normalizado") == dominio).toPandas()
    if df_pd.empty: continue
    
    motor = SistemaAlertasMarketing(
        fecha_corte=FECHA_CORTE_ANALISIS, 
        umbral_mad=config['umbral_mad'], 
        contaminacion_if=config['contaminacion_if']
    )
    alertas_dom = []
    
    for met in ['inversion_total', 'leads_total', 'sesiones_total']:
        alertas_dom.extend(motor.detectar_stl_mad(df_pd, met))
    
    alertas_dom.extend(motor.detectar_isolation_forest(df_pd))
    
    for a in alertas_dom:
        a['dominio'] = dominio
        resultados_alertas.append(a)

print(f"\n📊 ALERTAS DETECTADAS: {len(resultados_alertas)}")

if resultados_alertas:
    df_res = pd.DataFrame(resultados_alertas)
    # Conversión para evitar error PySpark Arrow
    df_res['valor'] = df_res['valor'].astype(str)
    df_res['score'] = df_res['score'].astype(str)
    display(spark.createDataFrame(df_res))
else:
    print("✅ Sin alertas críticas.")

# COMMAND ----------
# ============================================================
# CELDA 5: ENVÍO SEGURO (USA URL DE CONFIG)
# ============================================================
def enviar_adaptive_card(webhook, alerta):
    try:
        # Extraer datos seguros
        metrica = alerta.get('metrica', 'N/A')
        tipo = alerta.get('tipo', 'N/A')
        valor = str(alerta.get('valor', 'N/A'))
        dominio = alerta.get('dominio', 'Dominio')
        
        color = "Attention" if tipo == 'Caída Abrupta' else "Warning"
        emoji = "🚨" if tipo == 'Caída Abrupta' else "📈"
        
        # Construcción de la Adaptive Card
        payload = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock", 
                            "text": f"{emoji} Alerta: {dominio}", 
                            "weight": "Bolder", 
                            "size": "Medium", 
                            "color": color
                        },
                        {
                            "type": "FactSet", "facts": [
                                {"title": "Métrica:", "value": metrica},
                                {"title": "Tipo:", "value": tipo},
                                {"title": "Valor:", "value": valor},
                                {"title": "Fecha:", "value": str(alerta['fecha'])}
                            ]
                        }
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "📊 Ver en Power BI",
                            "url": config['dashboard_url'] # <--- LINK REFERENCIADO DESDE CELDA 0
                        }
                    ]
                }
            }]
        }
        
        requests.post(webhook, json=payload).raise_for_status()
        return True
    except Exception as e:
        print(f"Error envío: {e}")
        return False

if resultados_alertas:
    webhook_safe = config['webhook_teams']
    if "tu-organizacion" in webhook_safe:
        print("❌ ERROR: Configura la URL del Webhook en la Celda 0.")
    else:
        print(f"📤 Enviando {len(resultados_alertas)} alertas...")
        enviados = sum([enviar_adaptive_card(webhook_safe, a) for a in resultados_alertas])
        print(f"✅ Enviados: {enviados}/{len(resultados_alertas)}")
else:
    print("⚠️ Nada que enviar.")
