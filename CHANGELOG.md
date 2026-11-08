# Cambios

## v6.2 · Cierre operativo final en Serverless, encadenamiento de Jobs y entregables finales (7-nov-2026)
- **Inferencia y simulación en streaming a Delta Lake (`22` y `30`):** puntuación lote por horizonte (`h = 1…14`) con escritura inmediata en Delta y liberación explícita de memoria (`gc.collect()`), evitando desbordamientos de RAM en el driver de 16 GB de Databricks Serverless al procesar los 9.19 millones de registros operativos y los 45.9 millones de registros de escenarios *what-if*.
- **Compatibilidad de esquemas MLflow y Delta (`modelo.py`, `22`, `30`, `31`):** tipado estricto de 32 bits (`float32` / `int32`), generación retrocompatible de columnas `cod_*` cuando el modelo registrado lo solicita, alineación automática al esquema Delta existente y reemplazo idempotente por `DELETE FROM ... WHERE origen = DATE'...'`.
- **Resiliencia en volúmenes y SQL ANSI (`13` y `40`):** respaldo automático en `/tmp/calendario_promociones_plan.xlsx` ante errores de montaje FUSE (`OSError`) cuando el volumen `outputs/calendario` aún no ha sido aprovisionado; desactivación de `spark.sql.ansi.enabled` y división segura frente a denominadores en cero en `40_salidas_dashboard`; blindaje de `gold.dash_estado_pipeline`.
- **Jobs Serverless y Tablero AI/BI (`jobs/`, `dashboard/`):** comandos `%pip install` sin comillas compatibles con el constructor de entornos de Serverless Workflows, corrida integral validada de `J4 Inferencia` (77 min), encadenamiento `J1 → J2 → J4` mediante tareas `Run Job` (todos los *schedules* en `PAUSED`), y corrección de `allowHTMLByDefault: false` en las 7 vistas de `mna_equipo43_demanda.lvdash.json`.
- **Notebooks `.ipynb` y entregables finales (`databricks/`, `entregables_finales/`, `docs/`):** versión `.ipynb` de todos los notebooks con resúmenes auditados de la corrida de cierre (`MODO_PUBLICO = True`), documentación de restricciones Serverless vs. migración a clúster dedicado, contrato de carga del catálogo promocional de Bnext, Reporte Final, Presentación Ejecutiva, Producto de Difusión y Resumen Ejecutivo.

## v6.1 · Octubre y promociones con sólo fechas (4-nov-2026)
- `MESES` hasta `2026-10`; cuatro bloques de prueba (1-sep a 26-oct); eventos de octubre a diciembre.
- Calendario: `categoria_id = TODAS` con `tipo_categoria`, imputación del descuento histórico, bloque comentado para el
  catálogo de Bnext; plantilla con columna `tipo_categoria` y ejemplo de sólo fechas.
- Tablero: fracción de series con promoción (observada/planeada) en la serie de tiempo.

## v6.0 · Avance 6 (noviembre de 2026)
- Operación: `22_pronostico_operativo` (inferencia con el modelo registrado), `23_monitoreo` (acierto, PSI, alertas),
  `24_promocion_modelo` (`@challenger` → `@champion` con aprobaciones en tabla).
- Calendario de promociones editable (`13_calendario_promociones_plan`, `calendario/`) y variables `f_*` sin fuga.
- Avance 6 corregido: `30` what-if re-puntuando con el modelo, `31` reparto a producto con fila OTROS y reconciliación.
- `40_salidas_dashboard` y tablero AI/BI importable (7 vistas).
- Jobs J1–J5 (`jobs/`), parámetros como widgets, compuerta de datos con paro duro, constraints y comentarios en tablas,
  publicación idempotente, bitácoras de corridas y pronósticos.
- Documentación: auditoría de código, runbook, diccionario de datos, pendientes con Bnext. Pruebas locales y CI.

## v5.0 · Avance 5 (octubre de 2026)
- `Avance5.43`: ajuste fino, ensambles homogéneos y heterogéneos, prueba en datos no vistos (septiembre), intervalos,
  compuerta, registro en Unity Catalog y pronóstico operativo. Pipeline 00–20 con septiembre.
