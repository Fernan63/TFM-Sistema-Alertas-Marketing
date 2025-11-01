
## 📄 **3. docs/technical-specs.md**

# 🔧 Especificaciones Técnicas

## Algoritmos Implementados

### STL + MAD Detector
- **Método**: Seasonal-Trend decomposition using Loess
- **Periodo Estacional**: 7 días (patrón semanal)
- **Robustez**: Parámetro robust=True para manejar outliers
- **Umbral MAD**: 3.2 desviaciones absolutas medianas
- **Transformación**: Anscombe para métricas de conteo (sesiones, leads)

### Isolation Forest
- **Tipo**: Aprendizaje no supervisado
- **Hiperparámetros**:
  - `contamination`: 0.01 (1% de anomalías esperadas)
  - `n_estimators`: 100 árboles
  - `random_state`: 42 (reproducibilidad)
- **Preprocesamiento**: StandardScaler para normalización
- **Umbral**: Percentil 99 de los scores de anomalía

### Detección de Calidad de Datos
- **Validación**: Calendario completo vs datos existentes
- **Métrica**: Días faltantes como anomalías críticas
- **Umbral**: Cero tolerancia para datos missing

## Stack Tecnológico

### Procesamiento de Datos
- **Databricks**: Plataforma unificada de analytics
- **PySpark**: Procesamiento distribuido
- **Python 3.8+**: Lenguaje de programación principal

### Machine Learning
- **scikit-learn 1.0+**: Isolation Forest y preprocesamiento
- **statsmodels 0.13+**: Descomposición STL
- **numpy 1.21+**: Cálculos numéricos
- **pandas 1.3+**: Manipulación de datos

### Integraciones
- **Microsoft Teams API**: Notificaciones via webhooks
- **Power BI**: Visualización y reporting
- **Google Analytics API**: Extracción de datos
- **Meta Ads API**: Extracción de datos publicitarios

## Parámetros de Configuración

```yaml
system:
  ventana_analisis: 60
  ejecucion_automatica: true
  
detection:
  stl_mad:
    umbral: 3.2
    periodo_estacional: 7
    
  isolation_forest:
    contaminacion: 0.01
    n_estimators: 100
