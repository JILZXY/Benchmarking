-- =============================================================================
-- Q2 — Massive Sort + Top-K (PostgreSQL JSONB)
-- Version: 1.0.0 | Variant: work_mem_4MB
-- =============================================================================
-- Par de sesión aplicado por el runner / transacción antes de ejecutar:
--   SET LOCAL work_mem = '4MB';
-- Equivalencia lógica exacta con workloads/q2-sort-topk/mongodb/q2_*.json:
--   1. Filtra intentos con `payload->>'estado' = 'completado'`.
--   2. Ordena por `(payload #>> '{resultado,porcentaje}')::numeric DESC` y
--      desempate determinista por `(payload->>'intento_id') ASC`.
--   3. Obtiene los primeros 1,000 resultados.
-- =============================================================================

SELECT
    i.payload ->> 'intento_id' AS intento_id,
    i.payload ->> 'usuario_id' AS usuario_id,
    i.payload ->> 'leccion_id' AS leccion_id,
    i.payload ->> 'fecha_inicio' AS fecha_inicio,
    i.payload ->> 'estado' AS estado,
    ((i.payload #>> '{resultado,porcentaje}')::numeric)::double precision AS porcentaje,
    ((i.payload #>> '{resultado,puntaje_total}')::numeric)::double precision AS puntaje_total,
    ((i.payload #>> '{resultado,duracion_segundos}')::integer) AS duracion_segundos
FROM public.intentos AS i
WHERE i.payload ->> 'estado' = 'completado'
ORDER BY
    ((i.payload #>> '{resultado,porcentaje}')::numeric) DESC,
    (i.payload ->> 'intento_id') ASC
LIMIT 1000;

