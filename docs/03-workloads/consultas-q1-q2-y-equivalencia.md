# Consultas experimentales Q1 y Q2, equivalencia lógica y criterio empírico sobre spill

## 1. Catálogo y versionado de consultas

Todas las consultas están registradas y versionadas en [`workloads/shared/workloads_spec.json`](../../workloads/shared/workloads_spec.json) (`workloads_version: 1.0.0`). Cada vez que se captura un plan de ejecución (`explain_runner.py`), se guarda el número de versión, el `sha256` del archivo de consulta y el código fuente exacto junto al plan en `evidence/explain/`.

| Consulta | Motor | Variante | Archivo | Parámetros |
| --- | --- | --- | --- | --- |
| **Q1** (`q1-unwind-filter-group`) | MongoDB | `default` | [`q1_pipeline.json`](../../workloads/q1-unwind-filter-group/mongodb/q1_pipeline.json) | `{}` |
| **Q1** (`q1-unwind-filter-group`) | PostgreSQL | `default` | [`q1_query.sql`](../../workloads/q1-unwind-filter-group/postgresql/q1_query.sql) | `{}` |
| **Q2** (`q2-sort-topk`) | MongoDB | `allowDiskUse_false` | [`q2_allow_disk_use_false.json`](../../workloads/q2-sort-topk/mongodb/q2_allow_disk_use_false.json) | `{"allowDiskUse": false}` |
| **Q2** (`q2-sort-topk`) | MongoDB | `allowDiskUse_true` | [`q2_allow_disk_use_true.json`](../../workloads/q2-sort-topk/mongodb/q2_allow_disk_use_true.json) | `{"allowDiskUse": true}` |
| **Q2** (`q2-sort-topk`) | PostgreSQL | `work_mem_4MB` | [`q2_work_mem_4mb.sql`](../../workloads/q2-sort-topk/postgresql/q2_work_mem_4mb.sql) | `SET LOCAL work_mem = '4MB'` |
| **Q2** (`q2-sort-topk`) | PostgreSQL | `work_mem_64MB` | [`q2_work_mem_64mb.sql`](../../workloads/q2-sort-topk/postgresql/q2_work_mem_64mb.sql) | `SET LOCAL work_mem = '64MB'` |

## 2. Equivalencia lógica entre ambos motores

### Q1 — Unwind, Filter & Grouping
1. **Desanidado**: `$unwind: "$respuestas"` en MongoDB vs `CROSS JOIN LATERAL jsonb_array_elements(i.payload -> 'respuestas') AS r(elem)` en PostgreSQL.
2. **Filtrado**: `respuestas.evaluacion.puntaje_maximo > 5` en ambos motores.
3. **Expresión calculada**: `puntaje_ponderado = puntaje_obtenido * ponderacion`.
4. **Agrupación y salida**: agrupa por `(leccion.area, respuestas.tema)` devolviendo `total_respuestas`, `suma_puntaje_ponderado` y `promedio_puntaje_ponderado` redondeados a 4 decimales y ordenados ascendentemente por `(area, tema)`.

### Q2 — Massive Sort + Top-K
1. **Filtrado**: `estado == "completado"` (`payload->>'estado' = 'completado'`).
2. **Ordenamiento**: `resultado.porcentaje DESC` con desempate determinista por `intento_id ASC` para garantizar que ambos motores devuelvan exactamente las mismas 1,000 filas cuando existen empates en `porcentaje`.
3. **Top-K**: `$limit: 1000` / `LIMIT 1000`.
4. **Proyección**: `intento_id`, `usuario_id`, `leccion_id`, `fecha_inicio`, `estado`, `porcentaje`, `puntaje_total`, `duracion_segundos`.

## 3. Criterio metodológico sobre Q2 y la observación empírica de spill

> [!IMPORTANT]
> **No se modifica artificialmente Q2 para forzar un spill a disco.**
> El objetivo del experimento es observar empíricamente si ocurre o no almacenamiento temporal bajo los perfiles (`PROFILE_A_BASELINE`, `PROFILE_B_MEDIUM`, `PROFILE_C_SEVERE`), variantes (`allowDiskUse=false/true`, `work_mem=4MB/64MB`) y niveles de concurrencia (`1, 10, 50` clientes) establecidos.

- **Detección en MongoDB** ([`adapters/mongodb/adapter.py`](../../runner/python/src/adapters/mongodb/adapter.py)):
  - Inspección recursiva de `explain("executionStats")` buscando `usedDisk: true`, `spilledRecords > 0`, `spilledDataStorageSize > 0` o `spilledBytes > 0`.
  - Deltas en `db.serverStatus().metrics.queryExecutor` (`spilledRanges`, `spilledRecords`, `spilledDataStorageSize`).
  - Registros del contenedor emitidos por el filtro `operationProfiling.filter: '{ "usedDisk": true }'`.
- **Detección en PostgreSQL** ([`adapters/postgresql/adapter.py`](../../runner/python/src/adapters/postgresql/adapter.py)):
  - Inspección recursiva de `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON)` buscando `Sort Space Type: "Disk"` (`external merge`), `Temp Read Blocks > 0`, `Temp Written Blocks > 0` o `Disk Usage > 0`.
  - Deltas en `pg_stat_database` (`temp_files`, `temp_bytes`).
  - Líneas `LOG: temporary file: path ..., size ...` generadas por `log_temp_files = 0`.
- **Clasificación de estados** ([`metrics/outcome.py`](../../runner/python/src/metrics/outcome.py)):
  - `ok`: ejecución completada sin errores y sin almacenamiento temporal observado.
  - `spill`: ejecución completada sin errores en la que el motor utilizó almacenamiento temporal en disco.
  - `oom`: evento Out Of Memory detectado en el contenedor/cgroup o motor.
  - `error`: fallo operativo sin evidencia de OOM.
- **Interpretación**: la ausencia de spill **no es un fallo del experimento**; si ninguna configuración produce spill en la matriz oficial, el reporte final registra explícitamente que no se observó almacenamiento temporal en el rango de condiciones evaluadas.

