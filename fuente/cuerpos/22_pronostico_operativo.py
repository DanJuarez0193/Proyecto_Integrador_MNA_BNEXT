# Databricks notebook source
# MAGIC %md
# MAGIC # 22 · Pronóstico operativo (inferencia con el modelo registrado)
# MAGIC **Proyecto Integrador MNA · Equipo 43**
# MAGIC
# MAGIC Separa **pronosticar** de **entrenar**: toma el modelo final del registro de Unity Catalog
# MAGIC (`demanda_categoria @champion`, o `@challenger` si todavía no hay campeón), puntúa el conjunto `pronostico` de la tabla
# MAGIC Oro (origen = último día con datos, h = 1…14) y escribe `gold.pronostico_14d` con pronóstico puntual, P10/P50/P90 y
# MAGIC techo operativo. Tarda minutos, no horas: es lo que corre cada vez que llegan datos nuevos (Job `J4_inferencia`),
# MAGIC sin reentrenar.
# MAGIC
# MAGIC **Promociones planeadas.** Si el modelo se entrenó con las variables `f_*`, aquí se unen desde
# MAGIC `silver.calendario_promociones_plan_dia` (13): cambiar el plan y volver a correr este notebook actualiza el pronóstico.
# MAGIC
# MAGIC **Otro origen.** Con `ORIGEN_PRONOSTICO = "AAAA-MM-DD"` se pronostica un origen distinto al último (por ejemplo, el
# MAGIC conjunto `pronostico` de una Oro anterior). El modelo rechaza orígenes anteriores a la calibración de sus intervalos
# MAGIC (protección anti-fuga): el acierto en producción se mide con `23_monitoreo` cuando llega la venta real de los días
# MAGIC pronosticados hacia adelante; mientras tanto la referencia es la prueba de septiembre del Avance 5.
# MAGIC
# MAGIC | | |
# MAGIC | --- | --- |
# MAGIC | **Lee** | `gold.features_origen` (conjunto `pronostico`) · modelo `demanda_categoria` (UC o run de MLflow) · `silver.calendario_promociones_plan_dia` |
# MAGIC | **Escribe** | `gold.pronostico_14d` (reemplaza sólo su origen) · `mlops.pronosticos_bitacora` |
# MAGIC | **Parámetros** | `ALIAS_MODELO`, `PERMITIR_CHALLENGER`, `RUN_ID_MODELO`, `ORIGEN_PRONOSTICO`, `USAR_CALENDARIO_PROMOS`, `MODO_PUBLICO` |
# MAGIC | **Tiempo** | 10–30 min |
# MAGIC
# MAGIC **Requisito.** Una corrida de `Avance5.43` en modo completo que haya guardado el modelo final (compuerta técnica
# MAGIC aprobada o `REGISTRAR_EN_UC = True`). Si no hay modelo, este notebook se detiene y lo dice; **no** fabrica pronósticos.

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

# MAGIC %pip install catboost

# COMMAND ----------

try:
    dbutils.library.restartPython()      # necesario después de %pip en Databricks; fuera de Databricks no existe y se ignora
except Exception:
    pass

# COMMAND ----------

# @@CONFIG

# COMMAND ----------

# @@APOYO

# COMMAND ----------

# @@PROMOCIONES

# COMMAND ----------

# @@MODELO

# COMMAND ----------

# ----------------------------------------------------------------- parámetros de este notebook (también como parámetros de Job)
ALIAS_MODELO = parametro("ALIAS_MODELO", "champion")            # alias del registro: champion (producción) o challenger
PERMITIR_CHALLENGER = parametro("PERMITIR_CHALLENGER", True)    # si no hay @champion, usar @challenger
RUN_ID_MODELO = parametro("RUN_ID_MODELO", None)                # corrida del Avance 5 cuyo modelo usar si el registro no está disponible
ORIGEN_PRONOSTICO = parametro("ORIGEN_PRONOSTICO", None)        # None = último día con datos (operativo); una fecha = retrospectivo
USAR_CALENDARIO_PROMOS = parametro("USAR_CALENDARIO_PROMOS", True)
MODO_PUBLICO = parametro("MODO_PUBLICO", True)                  # True: no se imprimen importes
MIN_DIAS_CON_VENTA = 5                                          # mismas filas elegibles que en el entrenamiento (Avance 4 y 5)
TABLA_BITACORA = tabla("mlops", "pronosticos_bitacora")
RUN_ID_PRONOSTICO = "pron_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dt%H%M%S")
from uuid import uuid4
RUN_ID_PRONOSTICO += "_" + uuid4().hex[:6]
print("Corrida de pronóstico:", RUN_ID_PRONOSTICO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Modelo

# COMMAND ----------

titulo("1. MODELO REGISTRADO")
MODELO, INFO_MODELO = cargar_modelo_operativo(ALIAS_MODELO, RUN_ID_MODELO, PERMITIR_CHALLENGER)
ENTRADAS = columnas_modelo(MODELO) or []
USA_F = all(c in ENTRADAS for c in VARIABLES_PROMO_FUTURA)
mostrar(pd.DataFrame([{k: v for k, v in INFO_MODELO.items() if k != "etiquetas"}]), "Modelo cargado")
print(f"Variables de entrada del modelo: {len(ENTRADAS)} · usa promociones planeadas (f_*): {USA_F}")
hallazgo(f"Modelo {INFO_MODELO.get('modelo')} tomado de {INFO_MODELO['origen']} (alias {INFO_MODELO.get('alias')}, versión {INFO_MODELO.get('version')}), "
         f"entrenado en la corrida {INFO_MODELO.get('run_id_av5')}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filas de la Oro a pronosticar

# COMMAND ----------

titulo("2. FILAS A PRONOSTICAR")
ORO = leer_tabla(TABLA_ORO, "20_features_oro")
try:
    VERSION_ORO = int(spark.sql(f"DESCRIBE HISTORY {TABLA_ORO} LIMIT 1").collect()[0]["version"])
except Exception:
    VERSION_ORO = None
if ORIGEN_PRONOSTICO is None:
    fila = ORO.where(F.col("conjunto") == "pronostico").agg(F.max("origen").alias("o")).collect()[0]
    if fila["o"] is None:
        raise RuntimeError("La Oro no tiene conjunto 'pronostico'. Corre 20_features_oro con GENERAR_PRONOSTICO = True.")
    ORIGEN, CONJUNTO, FASE = fila["o"], "pronostico", "operativo"
else:
    ORIGEN = pd.Timestamp(ORIGEN_PRONOSTICO).date()
    disponibles = {r["conjunto"] for r in ORO.where(F.col("origen") == F.lit(ORIGEN)).select("conjunto").distinct().collect()}
    if not disponibles:
        raise RuntimeError(f"La Oro no tiene filas con origen {ORIGEN}; orígenes de evaluación: ver mlops.oro_resumen.")
    CONJUNTO = "pronostico" if "pronostico" in disponibles else "evaluacion"
    FASE = "operativo" if CONJUNTO == "pronostico" else "retrospectivo"
elegible = ((F.col("dias_con_venta_hist") >= MIN_DIAS_CON_VENTA) & F.col("ingenuo_estacional").isNotNull() & (F.col("s_dias_activos_7") >= 5))
FILAS = ORO.where((F.col("conjunto") == CONJUNTO) & (F.col("origen") == F.lit(ORIGEN)) & elegible)
if USAR_CALENDARIO_PROMOS and USA_F:
    PLAN_DIA, INFO_CAL = cargar_variables_promo_futura(spark)
    if PLAN_DIA is None:
        aviso(f"El modelo usa promociones planeadas pero el calendario no está disponible ({INFO_CAL.get('motivo')}): se pronostica como si no hubiera promociones planeadas.")
    FILAS = unir_promociones_planeadas(FILAS, PLAN_DIA)
    print("Calendario de promociones:", INFO_CAL)
elif USAR_CALENDARIO_PROMOS and not USA_F:
    aviso("El modelo registrado no se entrenó con las variables f_* del calendario: el plan de promociones no influye en este pronóstico "
          "(reentrena con USAR_CALENDARIO_PROMOS = True en Avance5.43).")
    INFO_CAL = {"activo": False}
else:
    INFO_CAL = {"activo": False}
    FILAS = unir_promociones_planeadas(FILAS, None) if USA_F else FILAS
FILAS = asegurar_columnas_categoricas_v1(FILAS, ORO, ENTRADAS)
n = FILAS.agg(F.count("*").alias("filas"), F.countDistinct("tienda", "categoria_id").alias("series"), F.countDistinct("h").alias("h")).collect()[0]
print(f"Origen {ORIGEN} ({FASE}, conjunto {CONJUNTO}) · {n['filas']:,} filas · {n['series']:,} series · {n['h']} horizontes · Oro versión {VERSION_ORO}")
if n["filas"] == 0:
    raise RuntimeError("No hay filas elegibles para pronosticar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Puntuar y escribir `gold.pronostico_14d` por lotes (streaming a Delta Lake)

# COMMAND ----------

import gc

titulo("3. PRONÓSTICO")
faltan = [c for c in ENTRADAS if c not in FILAS.columns]
if faltan:
    raise ValueError(f"A la Oro le faltan columnas que pide el modelo: {faltan}")
extra = {"modelo": INFO_MODELO.get("modelo") or "modelo_final", "run_id": INFO_MODELO.get("run_id_av5") or "",
         "version_oro": str(VERSION_ORO), "version_modelo": str(INFO_MODELO.get("version")), "alias_modelo": INFO_MODELO.get("alias"),
         "fase": FASE, "run_id_pronostico": RUN_ID_PRONOSTICO, "version_plan": str(INFO_CAL.get("version_plan", ""))}
llaves = ["tienda", "categoria_id", "origen", "fecha_objetivo", "h", "tipo_categoria", "region"]
if USA_F:
    llaves += [c for c in ["f_promos_planeadas", "f_descuento_planeado"] if c in FILAS.columns]
columnas_consulta = list(dict.fromkeys(llaves + ENTRADAS))
horizontes = [r[0] for r in FILAS.select("h").distinct().orderBy("h").collect()]
print(f"Puntuando y guardando {len(horizontes)} horizontes directamente en Delta Lake...")
total_filas = 0
limpiado_origen = False
with Cronometro() as reloj:
    for h_val in horizontes:
        t0 = time.time()
        pdf = FILAS.where(F.col("h") == h_val).select(*columnas_consulta).toPandas()
        if pdf.empty:
            continue
        try:
            pred_raw = MODELO.predict(_preparar_entrada(pdf[ENTRADAS].copy(), MODELO))
        except ValueError as e:
            if "calibraci" in str(e):
                raise RuntimeError(f"El modelo no puede pronosticar el origen {ORIGEN}: sus intervalos se calibraron con datos posteriores y la "
                                   "protección anti-fuga lo impide. El acierto en producción se mide cuando llega la venta real de un origen "
                                   "pronosticado hacia adelante (23_monitoreo); la referencia mientras tanto es la fase de prueba del Avance 5.") from e
            raise
        salida = _formatear_salida_modelo(pred_raw)
        pdf_lote = pd.concat([pdf[llaves].reset_index(drop=True), salida], axis=1)
        cols_orden = COLUMNAS_PRONOSTICO + [c for c in llaves if c not in COLUMNAS_PRONOSTICO]
        pdf_lote = pdf_lote[cols_orden].copy()
        for k, v in extra.items():
            pdf_lote[k] = v
        pdf_lote["fecha_generacion"] = pd.Timestamp.now()
        for c in ("tienda", "categoria_id", "tipo_categoria", "region"):
            pdf_lote[c] = pdf_lote[c].astype(str)
        for c in ("origen", "fecha_objetivo"):
            pdf_lote[c] = pd.to_datetime(pdf_lote[c]).dt.date
        pdf_lote["h"] = pdf_lote["h"].astype("int32")
        limpio, esquema = _para_spark(pdf_lote)
        sdf_lote = spark.createDataFrame(limpio, schema=esquema).withColumn("h", F.col("h").cast("int"))
        if not limpiado_origen:
            if existe(TABLA_PRONOSTICO):
                spark.sql(f"DELETE FROM {TABLA_PRONOSTICO} WHERE origen = DATE'{ORIGEN}'")
                esquema_existente = {f.name: f.dataType for f in spark.table(TABLA_PRONOSTICO).schema.fields}
            else:
                esquema_existente = {}
            limpiado_origen = True
        if existe(TABLA_PRONOSTICO) and esquema_existente:
            for c in sdf_lote.columns:
                if c in esquema_existente:
                    sdf_lote = sdf_lote.withColumn(c, F.col(c).cast(esquema_existente[c]))
            sdf_lote.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TABLA_PRONOSTICO)
        else:
            sdf_lote.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLA_PRONOSTICO)
            esquema_existente = {f.name: f.dataType for f in spark.table(TABLA_PRONOSTICO).schema.fields}
        filas_h = len(pdf_lote)
        total_filas += filas_h
        print(f"   h = {h_val:2d}: {filas_h:,} filas puntuadas y escritas en Delta ({time.time() - t0:.1f} s)")
        del pdf, salida, pdf_lote, limpio, sdf_lote
        gc.collect()
asegurar_restricciones(TABLA_PRONOSTICO, {"pron_h_valido": "h BETWEEN 1 AND 14",
                                          "pron_no_negativo": "prediccion_puntual >= 0 AND p10 >= 0",
                                          "pron_intervalo_ordenado": "p10 <= p50 AND p50 <= p90"})
cols_tabla = set(spark.table(TABLA_PRONOSTICO).columns)
comentar_columnas(TABLA_PRONOSTICO, {k: v for k, v in DESCRIPCION_PRONOSTICO.items() if k in cols_tabla})
try:
    spark.sql(f"COMMENT ON TABLE {TABLA_PRONOSTICO} IS 'Pronostico operativo a 14 dias por tienda x categoria x dia con intervalo P10-P90 y techo de surtido; cada corrida reemplaza solo su origen'")
except Exception:
    pass
DESTINO = TABLA_PRONOSTICO
print(f"{total_filas:,} filas pronosticadas y escritas en {reloj.segundos / 60:.1f} min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revisión (sin importes) y bitácora

# COMMAND ----------

titulo("4. REVISIÓN")
PRON_DF = spark.table(TABLA_PRONOSTICO).where(F.col("origen") == F.lit(ORIGEN))
ancho_expr = (F.col("p90") - F.col("p10")) / F.greatest(F.col("prediccion_puntual"), F.lit(1.0))
metricas = PRON_DF.agg(
    F.count("*").alias("filas"),
    F.countDistinct("tienda", "categoria_id").alias("series"),
    F.avg(F.when((F.col("p10") <= F.col("prediccion_puntual")) & (F.col("prediccion_puntual") <= F.col("p90")), 1.0).otherwise(0.0)).alias("p10_le_pron_le_p90"),
    F.expr("percentile_approx((p90 - p10) / greatest(prediccion_puntual, 1.0), 0.5)").alias("ancho_relativo_mediano"),
    F.avg(F.when(F.col("prediccion_puntual") <= 0, 1.0).otherwise(0.0)).alias("pronostico_cero"),
    F.min("fecha_objetivo").alias("desde"),
    F.max("fecha_objetivo").alias("hasta")
).collect()[0]
series_promo = 0.0
if "f_promos_planeadas" in PRON_DF.columns:
    series_promo = float(PRON_DF.groupBy("tienda", "categoria_id").agg(F.max("f_promos_planeadas").alias("mx"))
                         .agg(F.avg(F.when(F.col("mx") > 0, 1.0).otherwise(0.0))).collect()[0][0] or 0.0)
hallazgo(f"Pronóstico {FASE} del origen {ORIGEN} escrito en {DESTINO}: {int(metricas['filas']):,} filas, {int(metricas['series']):,} series, "
         f"{metricas['desde']} → {metricas['hasta']}.")
rev = pd.DataFrame([{
    "filas": int(metricas["filas"]), "series": int(metricas["series"]),
    "p10_le_pron_le_p90": float(metricas["p10_le_pron_le_p90"] or 0.0),
    "ancho_relativo_mediano": float(metricas["ancho_relativo_mediano"] or 0.0),
    "pronostico_cero": float(metricas["pronostico_cero"] or 0.0),
    "series_con_promo_planeada": series_promo,
}])
mostrar(rev, "Revisión del pronóstico", formatos={"filas": "int", "series": "int", "p10_le_pron_le_p90": "pct", "ancho_relativo_mediano": "dec2",
                                                 "pronostico_cero": "pct", "series_con_promo_planeada": "pct"})
por_region = (PRON_DF.withColumn("_a", ancho_expr).groupBy("region", "tipo_categoria")
              .agg(F.countDistinct("tienda").alias("series"), F.count("*").alias("filas"),
                   F.expr("percentile_approx(_a, 0.5)").alias("ancho_rel")).orderBy("region", "tipo_categoria").toPandas())
mostrar(por_region, "Series y ancho relativo del intervalo por región × tipo", formatos={"series": "int", "filas": "int", "ancho_rel": "dec2"})
if not MODO_PUBLICO:
    tot = PRON_DF.groupBy("fecha_objetivo").agg(F.sum("prediccion_puntual").alias("prediccion_puntual"), F.sum("p10").alias("p10"), F.sum("p90").alias("p90")).orderBy("fecha_objetivo").toPandas()
    mostrar(tot, "Totales diarios (pesos)", formatos={"prediccion_puntual": "dinero", "p10": "dinero", "p90": "dinero"})
bit = pd.DataFrame([{"run_id_pronostico": RUN_ID_PRONOSTICO, "origen": ORIGEN, "fase": FASE, "modelo": extra["modelo"], "alias_modelo": extra["alias_modelo"],
                     "version_modelo": extra["version_modelo"], "run_id_entrenamiento": extra["run_id"], "version_oro": str(VERSION_ORO),
                     "version_plan": extra["version_plan"], "filas": int(metricas["filas"]), "series": int(metricas["series"]),
                     "desde": metricas["desde"], "hasta": metricas["hasta"], "minutos": float(reloj.segundos / 60),
                     "fecha": pd.Timestamp.now()}])
guardar(bit, "mlops", "pronosticos_bitacora", "Corridas de pronostico operativo: origen, modelo, version y filas (22)", modo="append")

# COMMAND ----------

cerrar("22_pronostico_operativo", siguiente="23_monitoreo · 30_simulacion_escenarios_whatif · 31_desagregacion_predicciones_sku · 40_salidas_dashboard")
