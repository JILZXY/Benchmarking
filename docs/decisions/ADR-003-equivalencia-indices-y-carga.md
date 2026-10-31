# ADR-003 — Equivalencia de índices y preservación del payload en MongoDB y PostgreSQL

- **Estado:** aceptada
- **Fecha:** 2026-10-30

## Contexto

El README (§5 y §13) establece que:
- En **MongoDB** se usa la colección `intentos` con índice sobre `intento_id` e índice sobre `fecha_inicio`.
- En **PostgreSQL** se usa la tabla `intentos` con `id UUID PRIMARY KEY`, `payload JSONB NOT NULL`, índice B-Tree sobre el identificador e índice B-Tree sobre la fecha extraída del JSONB.
- No se deben agregar índices que aceleren artificialmente las consultas Q1 o Q2 ni transformar los valores del dataset.

En PostgreSQL, las expresiones indexadas deben ser `IMMUTABLE`. Un cast directo `((payload->>'fecha_inicio')::timestamptz)` no es `IMMUTABLE` porque depende del parámetro de sesión `TimeZone`.

## Decisión

1. **Preservación íntegra del documento**:
   - Ambos cargadores leen el mismo archivo JSONL canónico generado por `dataset/generator/generate.py`.
   - En MongoDB se inserta exactamente el objeto JSON decodificado (sin mutar tipos ni valores).
   - En PostgreSQL se almacena `id = doc["intento_id"]` como `UUID` y `payload` con la línea JSON original sin ninguna transformación.
   - Al recuperar un documento en MongoDB (removiendo `_id`) y en PostgreSQL (`SELECT payload`), su serialización canónica (`json.dumps(..., sort_keys=True, separators=(",", ":"))`) produce exactamente el mismo SHA-256 que el archivo JSONL de origen.

2. **Índices equivalentes**:
   - **MongoDB**:
     - `idx_intentos_intento_id`: `{ "intento_id": 1 }` (`unique: true`).
     - `idx_intentos_fecha_inicio`: `{ "fecha_inicio": 1 }`.
   - **PostgreSQL**:
     - `intentos_pkey`: `PRIMARY KEY (id)` (B-Tree único sobre UUID).
     - `idx_intentos_fecha_inicio`: `CREATE INDEX ON public.intentos USING btree ((payload->>'fecha_inicio'))`.
   - Como `fecha_inicio` se almacena en ambos motores como cadena ISO-8601 UTC canónica de longitud fija (`YYYY-MM-DDTHH:MM:SSZ`), el orden lexicográfico del B-Tree coincide exactamente con el orden cronológico en ambos motores y la expresión `(payload->>'fecha_inicio')` es `IMMUTABLE` de forma nativa.

3. **Orden de creación de índices**:
   - En cargas limpias (`--drop`), primero se insertan los lotes y al finalizar se crean los índices (y se ejecuta `ANALYZE public.intentos` en PostgreSQL), reduciendo el tiempo de ingesta y dejando árboles B-Tree compactos.

