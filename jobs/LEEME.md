# Jobs (Workflows) del pipeline — Equipo 43

Cinco Jobs serverless que encadenan los notebooks de `mna_equipo43_avance6`. Cada tarea es un notebook con sus
parámetros (los mismos nombres que las variables del código: `MESES_A_CARGAR`, `MODO_RAPIDO`, …); dentro del
notebook, `parametro()` toma el valor del Job y, si no hay, el del código.

| Job | Tareas (en orden) | Cuándo | Tiempo aprox. |
| --- | --- | --- | --- |
| **J1 ingesta_mes** | `02` (sólo el mes nuevo) → `08` (todos los meses) → `09` → `10` → `11` (**se detiene si una regla crítica falla**) → *Run Job* `disparar_j2_features` | cuando Bnext entrega un mes | 1.5–4 h |
| **J2 features** | `13` (calendario de promociones del archivo) → `20` (tabla Oro) → *Run Job* `disparar_j4_inferencia` | después de J1, o cuando cambia el calendario | 40–90 min |
| **J3 entrenamiento** | `Avance5.43` completo (ajuste, ensambles, prueba, modelo final, `@challenger`, pronóstico) | mensual, o cuando el monitoreo lo pida | 4–6 h |
| **J4 inferencia** | `22` (pronóstico con `@champion`/`@challenger`) → (`30` y `31` en paralelo) → `40` (tablero) | después de J2 o semanal (lunes 06:00 CDMX, dejado en `PAUSED`) | 75–80 min |
| **J5 monitoreo** | `23` → `40` | semanal (lunes 07:00 CDMX, dejado en `PAUSED` hasta activarlo) | 10–20 min |

Demo de punta a punta («llega un mes → corre todo → predicciones y tablero»): J1 → J2 → J3 → J4 → J5.
Operación normal encadenada sin reentrenar: J1 dispara automáticamente a J2 (`disparar_j2_features`, condición `All succeeded`) y J2 dispara a J4 (`disparar_j4_inferencia`, condición `All succeeded`). Todos los *schedules* quedan en estado `PAUSED` al cierre académico para evitar consumo automático de DBUs hasta que TI de Bnext asigne el calendario productivo.

## Crear los Jobs

**Opción A — interfaz.** Workflows → Create job → por cada tarea: *Type* Notebook, *Path* el notebook, *Compute*
Serverless, *Parameters* los de la tabla `base_parameters` del JSON correspondiente, *Depends on* la tarea previa.
En *Environment* (serverless) agrega las dependencias de `environments[].spec.dependencies` del JSON (J3 y J4
necesitan `lightgbm`, `xgboost`, `catboost`, `optuna`, `shap`). En *Notifications* agrega un correo en *On failure*.
Recomendado para J3: *Timeout* 8 h, sin reintentos.

**Opción B — CLI.** Con la CLI de Databricks autenticada (`databricks auth login`):

```bash
./crear_jobs.sh "/Workspace/Users/<usuario>/mna_equipo43_avance6" "correo@dominio.com"
```

El script sustituye la ruta de los notebooks y el correo en los JSON y llama a `databricks jobs create`. Si el
workspace usa otra versión de entorno serverless, cambia `"client"` en `environments` (la que aparece en el panel
*Environment* del notebook).

## Parámetros útiles

| Job | Parámetro | Para qué |
| --- | --- | --- |
| J1 | `MES` (parámetro del Job; por defecto `2026-10`) | el mes que se carga en Bronce; `08` siempre relimpia todos |
| J3 | `RUN_ID_REANUDAR` | reanudar una corrida interrumpida desde su punto de control |
| J3 | `MODO_RAPIDO = True` | prueba de humo (20–40 min) antes de la corrida completa |
| J4 | `ALIAS_MODELO = challenger` | pronosticar con el retador para compararlo |
| J4 | `ORIGEN_PRONOSTICO` (en `22`) | pronosticar un origen distinto al último (el modelo rechaza orígenes anteriores a su calibración) |

## Reglas de operación y notas de Serverless vs. Cluster dedicado

- **Sintaxis de `%pip install` en Serverless Jobs:** Databricks Serverless extrae literalmente las líneas `%pip install` de los notebooks hacia un archivo temporal de entorno; nunca usar comillas dobles (`%pip install "lightgbm>=4.3,<5"`), sino nombres limpios (`%pip install lightgbm`).
- **Migración a Job Cluster dedicado:** como el workspace operó exclusivamente en cómputo Serverless (sin clúster aprovisionado visible para el rol externo), si TI de Bnext habilita un Job Cluster dedicado (`Standard_DS4_v2` / `Standard_E8ds_v5`, Databricks Runtime ML LTS, acceso `Single User` o `Shared` con Unity Catalog), basta cambiar `"environment_key": "serverless"` por `"job_cluster_key": "cluster_mlops"` en estos JSON: las librerías de ML ya vienen preinstaladas en DBR ML, se habilita caché en disco/memoria (`df.persist()`) y se eliminan los topes de 16 GB de RAM en el driver.
- J1 falla a propósito si `11` encuentra una regla crítica rota: nada se entrena ni se pronostica con datos malos.
- `Avance5.43` registra el modelo como `@challenger`; `24_promocion_modelo` lo pasa a `@champion` con evidencia y las
  aprobaciones de `mlops.aprobaciones`. `22` usa `@champion` y, si no existe, `@challenger`.
- Cada corrida escribe en `mlops.corridas` (tiempos) y `mlops.hallazgos`; el tablero (`40`) los muestra.
