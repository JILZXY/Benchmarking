-- =============================================================================
-- Esquema e índices oficiales para PostgreSQL (tabla `intentos`)
-- =============================================================================
-- Equivale en finalidad al esquema de MongoDB (README §5):
--   1) Identificador UUID como PRIMARY KEY (índice B-Tree `intentos_pkey`).
--   2) Columna `payload JSONB NOT NULL` con el documento íntegro sin alterar.
--   3) Índice B-Tree sobre la fecha extraída del JSONB (`payload->>'fecha_inicio'`),
--      cuyo formato ISO-8601 UTC canónico ('YYYY-MM-DDTHH:MM:SSZ') preserva el
--      orden cronológico exacto.
-- No se crean índices GIN ni índices sobre campos de Q1/Q2.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.intentos (
    id UUID PRIMARY KEY,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intentos_fecha_inicio
    ON public.intentos USING btree ((payload->>'fecha_inicio'));

