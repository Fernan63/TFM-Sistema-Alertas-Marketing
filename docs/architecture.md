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
    
    style A fill:#e1f5fe
    style A2 fill:#e1f5fe
    style A3 fill:#e1f5fe
    style C fill:#fff3e0
    style G fill:#f3e5f5
    style H fill:#e8f5e8
    style I fill:#e8f5e8
