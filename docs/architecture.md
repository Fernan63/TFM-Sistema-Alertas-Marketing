# 🏗️ Arquitectura del Sistema

## Diagrama de Componentes

```mermaid
graph TB
    A[Fuentes de Datos] --> B[Databricks]
    B --> C[Pipeline ETL]
    C --> D[STL + MAD Detector]
    C --> E[Isolation Forest]
    D --> F[Ensemble Fusion]
    E --> F
    F --> G[Microsoft Teams]
    F --> H[Power BI]
