# Modelo de datos experimental y generador determinista

## 1. Colección / tabla `intentos`

Cada documento representa un intento de un usuario al realizar una lección en ETICORE.
El contrato formal está definido en [`dataset/schema/intentos.schema.json`](../../dataset/schema/intentos.schema.json).

### Estructura de cada documento

```json
{
  "intento_id": "2c8b0f0a-7c1b-4d4a-9b8e-3d1d9e0f2a11",
  "fecha_inicio": "2026-06-14T18:22:09Z",
  "usuario_id": "b1f4c7e2-3a90-4f10-8a11-7e5c4d2b1a09",
  "leccion_id": "9a1e3c4b-1d2f-4b88-8c77-0a1b2c3d4e5f",
  "estado": "completado",
  "usuario": {
    "nivel": "intermedio",
    "segmento": "estudiante"
  },
  "leccion": {
    "area": "privacidad",
    "tema": "proteccion_datos",
    "dificultad": "media"
  },
  "respuestas": [
    {
      "pregunta_id": "7f8e9d0c-1b2a-4c3d-8e9f-0a1b2c3d4e5f",
      "tema": "proteccion_datos",
      "tipo": "opcion_multiple",
      "evaluacion": {
        "puntaje_obtenido": 8,
        "puntaje_maximo": 10,
        "ponderacion": 1.2
      }
    }
  ],
  "resultado": {
    "puntaje_total": 9.6,
    "porcentaje": 80.0,
    "duracion_segundos": 420
  },
  "metadata": {
    "canal": "web",
    "session_id": "session-000421",
    "flags": []
  }
}
```

## 2. Propiedades de reproducibilidad y determinismo

El generador ([`dataset/generator/generate.py`](../../dataset/generator/generate.py)) cumple las siguientes garantías:

1. **PRNG aislado (`random.Random(seed)`)**: ningún valor depende del estado global ni del reloj del sistema.
2. **UUID v4 deterministas**: todos los identificadores (`intento_id`, `usuario_id`, `leccion_id`, `pregunta_id`) se construyen a partir de 128 bits extraídos del PRNG con los bits RFC 4122 fijados.
3. **Fechas ISO-8601 deterministas**: se calculan como desplazamientos en segundos desde la época fija `2026-01-01T00:00:00Z`.
4. **JSONL canónico**: cada documento se serializa con `json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)` y salto de línea `\n` (LF), de modo que el hash SHA-256 es idéntico en Windows, Linux y macOS.
5. **Procesamiento en streaming por lotes (`batch_size`)**: los documentos se generan con un iterador, se validan uno por uno ([`dataset/generator/validator.py`](../../dataset/generator/validator.py)) y se vuelcan a disco en bloques (`5,000` documentos por defecto), manteniendo el consumo de RAM constante sin importar si se generan `1,000` o `500,000` registros.

## 3. Distribuciones sintéticas relevantes para Q1 y Q2

- **Cantidad por defecto**: `500,000` documentos (`DATASET_SIZE`), semilla `20261030` (`DATASET_SEED`).
- **Respuestas por intento**: distribución discreta `{1, 2, 2, 3, 3, 3, 4, 4, 5}` con media exacta `3.0` (`~1,500,000` respuestas en el dataset completo).
- **Selectividad en Q1 (`evaluacion.puntaje_maximo > 5`)**: el catálogo de `puntaje_maximo` es `{2, 4, 5, 8, 10, 10, 12, 15, 20}` (`6/9 ≈ 66.7 %` de las respuestas superan el filtro).
- **Selectividad en Q2 (`estado == "completado"`)**: pesos `completado=72 %`, `en_progreso=12 %`, `abandonado=11 %`, `expirado=5 %` (`~360,000` documentos califican para el ordenamiento en el dataset de `500,000`).

## 4. Manifiesto y verificación de integridad

Cada generación produce un manifiesto en `dataset/manifests/` con:
- `schema_version` y `generator_version`
- `seed`, `record_count`, `avg_respuestas_config`, `avg_respuestas_observed`
- `total_respuestas`, `q1_qualifying_respuestas`, `q2_completed_intentos`
- `file_name`, `file_size_bytes` y `sha256`

Comandos:

```powershell
# Generar fixture de prueba (100 registros)
.\scripts\data\generate_dataset.ps1 --count 100 --seed 20261030 `
  --output dataset/fixtures/intentos_sample_100.jsonl `
  --manifest dataset/manifests/intentos_sample_100.manifest.json

# Generar el dataset oficial de 500,000 documentos
.\scripts\data\generate_dataset.ps1 --count 500000 --seed 20261030

# Verificar integridad contra el manifiesto
.\scripts\data\generate_dataset.ps1 --count 500000 --seed 20261030 --verify-only
```

## 5. Carga por lotes e índices equivalentes en MongoDB y PostgreSQL

Los cargadores ([`mongodb_loader.py`](../../runner/python/src/load/mongodb_loader.py) y [`postgresql_loader.py`](../../runner/python/src/load/postgresql_loader.py)) leen el mismo archivo JSONL canónico en streaming por lotes (`--batch-size 5000`) y no introducen ninguna transformación sobre los valores del dataset:

| Motor | Estructura | Carga por lotes | Índices creados |
| --- | --- | --- | --- |
| **MongoDB** | Colección `intentos` | `insert_many(docs, ordered=False)` | `idx_intentos_intento_id` (`{intento_id: 1}`, `unique: true`) y `idx_intentos_fecha_inicio` (`{fecha_inicio: 1}`) |
| **PostgreSQL** | Tabla `public.intentos (id UUID PRIMARY KEY, payload JSONB NOT NULL)` | `COPY public.intentos (id, payload) FROM STDIN` + `ANALYZE` | `intentos_pkey` (`PRIMARY KEY (id)`) y `idx_intentos_fecha_inicio` (`USING btree ((payload->>'fecha_inicio'))`) |

La justificación técnica de los índices equivalentes está documentada en [ADR-003](../decisions/ADR-003-equivalencia-indices-y-carga.md).

### Comandos de carga y verificación de equivalencia

```powershell
# Cargar en MongoDB (con el contenedor mongodb activo)
.\scripts\data\load_data.ps1 mongodb

# Cargar en PostgreSQL (con el contenedor postgresql activo)
.\scripts\data\load_data.ps1 postgresql

# Verificar conteo, índices y coincidencia canónica bit a bit de los registros
.\scripts\data\load_data.ps1 verify --engine mongodb
.\scripts\data\load_data.ps1 verify --engine postgresql
```


