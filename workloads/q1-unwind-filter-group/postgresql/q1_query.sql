-- =============================================================================
-- Q1 — Unwind, Filter & Grouping (PostgreSQL JSONB)
-- Version: 1.0.0 | Variant: default
-- =============================================================================
-- Equivalencia lógica exacta con workloads/q1-unwind-filter-group/mongodb/q1_pipeline.json:
--   1. Desanida el arreglo `respuestas` con `jsonb_array_elements(i.payload->'respuestas')`.
--   2. Filtra respuestas con `(r->'evaluacion'->>'puntaje_maximo')::int > 5`.
--   3. Calcula `puntaje_ponderado = puntaje_obtenido * ponderacion`.
--   4. Agrupa por `leccion.area` y `respuestas.tema`, ordenando por `(area, tema)`.
-- =============================================================================

SELECT
    i.payload #>> '{leccion,area}' AS area,
    r.elem ->> 'tema' AS tema,
    COUNT(*)::bigint AS total_respuestas,
    ROUND(
        SUM(
            ((r.elem #>> '{evaluacion,puntaje_obtenido}')::numeric)
            * ((r.elem #>> '{evaluacion,ponderacion}')::numeric)
        ),
        4
    )::double precision AS suma_puntaje_ponderado,
    ROUND(
        AVG(
            ((r.elem #>> '{evaluacion,puntaje_obtenido}')::numeric)
            * ((r.elem #>> '{evaluacion,ponderacion}')::numeric)
        ),
        4
    )::double precision AS promedio_puntaje_ponderado
FROM public.intentos AS i
CROSS JOIN LATERAL jsonb_array_elements(i.payload -> 'respuestas') AS r(elem)
WHERE ((r.elem #>> '{evaluacion,puntaje_maximo}')::integer) > 5
GROUP BY
    i.payload #>> '{leccion,area}',
    r.elem ->> 'tema'
ORDER BY
    area ASC,
    tema ASC;

