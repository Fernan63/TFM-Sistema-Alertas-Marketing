# 🏗️ Arquitectura del Sistema

## Diagrama de Componentes

```mermaid
graph TB
    A[GA4] --> B[Data Integration]
    A2[Google Ads] --> B
    A3[Meta Ads] --> B
    B --> C[Databricks Platform]
    C --> D[STL + MAD Detector]
    C --> E[Isolation Forest]
    C --> F[Data Quality Check]
    D --> G[Alert Fusion]
    E --> G
    F --> G
    G --> H[Microsoft Teams]
    G --> I[Power BI Dashboard]
    
