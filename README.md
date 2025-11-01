# 🚨 TFM: Sistema Automático de Alertas por Anomalías en Tráfico Digital

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache-Spark-orange)
![ML](https://img.shields.io/badge/Machine-Learning-brightgreen)
![Status](https://img.shields.io/badge/Status-Completed-success)

## 📊 Descripción del Proyecto

Sistema integral de detección de anomalías en métricas de marketing digital que implementa un ensemble de métodos estadísticos y machine learning para la monitorización proactiva del tráfico digital.

**Tecnologías:** Databricks, Python, PySpark, Power BI, Microsoft Teams

## 🏗️ Arquitectura del Sistema

### Flujo Principal
1. **Extracción**: Datos de GA4, Google Ads, Meta Ads
2. **Transformación**: Normalización por dominio único
3. **Análisis**: 
   - STL + MAD (detección estadística)
   - Isolation Forest (ML multivariante)
   - Validación de calidad de datos
4. **Alerting**: Notificaciones a Microsoft Teams
5. **Visualización**: Dashboard Power BI

## 📁 Estructura del Repositorio

FM-Sistema-Alertas-Marketing/
├── 📄 README.md # Este archivo
├── 📁 docs/ # Documentación
├── 📁 src/ # Código fuente
├── 📁 config/ # Configuraciones
├── 📁 assets/ # Imágenes/diagramas
└── 📄 LICENSE # Licencia MIT
