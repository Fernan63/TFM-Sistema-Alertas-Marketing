"""
Punto de entrada principal del sistema de alertas.
TFM - Diego Fernando Parra Moreno
"""
from sistema_alertas import SistemaAlertasMarketing
import logging

def configurar_logging():
    """Configura el sistema de logging para el proyecto."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sistema_alertas.log'),
            logging.StreamHandler()
        ]
    )

def main():
    """Función principal que ejecuta el sistema completo."""
    try:
        # Configurar logging
        configurar_logging()
        logger = logging.getLogger(__name__)
        
        logger.info("🚀 Iniciando Sistema de Alertas de Marketing...")
        
        # Inicializar sistema con parámetros por defecto
        sistema = SistemaAlertasMarketing()
        
        # Ejecutar pipeline completo
        logger.info("📊 Ejecutando pipeline de detección...")
        resultados = sistema.ejecutar_pipeline_completo()
        
        # Reporte final
        logger.info("✅ Procesamiento completado exitosamente")
        logger.info(f"📈 Dominios analizados: {resultados['total_dominios']}")
        logger.info(f"🚨 Alertas generadas: {resultados['total_alertas']}")
        logger.info(f"📊 STL+MAD: {resultados['alertas_stl']} alertas")
        logger.info(f"🤖 Isolation Forest: {resultados['alertas_if']} alertas")
        logger.info(f"🔍 Días faltantes: {resultados['alertas_faltantes']} alertas")
        
        return resultados
        
    except Exception as e:
        logger.error(f"💥 Error en ejecución principal: {str(e)}")
        raise

if __name__ == "__main__":
    main()