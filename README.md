# ETICORE DB Benchmark

## 1. Descripción del proyecto

Proyecto experimental para evaluar y comparar el rendimiento de MongoDB y PostgreSQL utilizando JSONB, bajo diferentes condiciones de disponibilidad de recursos.

El experimento utiliza como referencia el dominio educativo de ETICORE y toma la colección `intentos` como base para construir un conjunto de datos sintéticos.

El objetivo es analizar cómo se comportan ambos motores al ejecutar operaciones de agregación, filtrado y ordenamiento sobre documentos JSON con estructuras anidadas.

La comparación se realizará utilizando Docker, Docker Compose y cgroups v2 para controlar los recursos disponibles.

## 2. Objetivo general

Medir y comparar el comportamiento de MongoDB y PostgreSQL JSONB considerando:

- Latencia P50, P95 y P99.
- Throughput (consultas por segundo).
- Consumo de CPU.
- Consumo de memoria RAM.
- Spill a disco.
- Eventos Out Of Memory (OOM).
- Comportamiento bajo concurrencia.

Se busca obtener resultados reproducibles que permitan identificar las fortalezas, limitaciones y diferencias de ambos motores.

## 3. Tecnologías utilizadas

| Tecnología     | Responsabilidad                            |
| -------------- | ------------------------------------------ |
| MongoDB        | Motor documental                           |
| PostgreSQL     | Motor relacional con JSONB                 |
| Python         | Generación de datos y ejecución de pruebas |
| Docker         | Aislamiento de servicios                   |
| Docker Compose | Orquestación del entorno                   |
| Prometheus     | Recolección de métricas                    |
| cAdvisor       | Monitorización de contenedores             |
| Node Exporter  | Métricas del sistema anfitrión             |

## 4. Modelo de datos experimental

La colección principal será `intentos`.

Cada documento representará un intento de un usuario al completar una lección de ETICORE.

### Estructura propuesta

```json
{
  "intento_id": "uuid",
  "fecha_inicio": "2026-10-30T10:00:00Z",
  "usuario_id": "uuid",
  "leccion_id": "uuid",
  "estado": "completado",
  "usuario": {
    "nivel": "intermedio",
    "segmento": "estudiante"
  },
  "leccion": {
    "area": "etica",
    "tema": "privacidad",
    "dificultad": "media"
  },
  "respuestas": [
    {
      "pregunta_id": "uuid",
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
    "puntaje_total": 85,
    "porcentaje": 85,
    "duracion_segundos": 420
  },
  "metadata": {
    "canal": "web",
    "session_id": "session-001",
    "flags": []
  }
}
```

**Nota:** Esta es una estructura experimental propuesta, inspirada en el dominio de ETICORE. No representa necesariamente todos los atributos definitivos del modelo de producción.

### Características del dataset

- 500,000 documentos.
- Aproximadamente 3 respuestas por intento.
- Estructuras JSON anidadas.
- Identificadores UUID.
- Fechas ISO-8601.
- Datos sintéticos.
- Generación determinista mediante una semilla fija.

El mismo dataset deberá utilizarse en ambos motores.

## 5. Motores de base de datos

### MongoDB

- Motor WiredTiger.
- Sin sharding.
- Colección `intentos`.
- Índice sobre `intento_id`.
- Índice sobre `fecha_inicio`.
- Agregaciones mediante Aggregation Pipeline.

### PostgreSQL

- Tabla `intentos`.
- Identificador UUID como PRIMARY KEY.
- Columna JSONB para almacenar el documento.
- Índice B-Tree sobre el identificador.
- Índice B-Tree sobre la fecha extraída del JSONB.
- Consultas mediante operadores JSONB y funciones de agregación.

Los índices deben mantenerse equivalentes en finalidad. No se agregarán índices específicos para acelerar artificialmente las consultas de prueba.

## 6. Cargas de trabajo

### Q1 — Unwind, Filter & Grouping

Objetivo: evaluar el procesamiento de arreglos, filtrado y agrupación.

Operaciones:

1. Desanidar el arreglo `respuestas`.
2. Filtrar respuestas cuyo `puntaje_maximo` sea mayor que 5.
3. Calcular el puntaje ponderado.
4. Agrupar por área de la lección y tema de la respuesta.

Expresión:

`puntaje_ponderado = puntaje_obtenido × ponderacion`

Métricas prioritarias:

- Throughput.
- P95.
- P99.
- CPU throttling.

### Q2 — Massive Sort + Top-K

Objetivo: evaluar el consumo de memoria durante operaciones de ordenamiento.

Operaciones:

1. Filtrar intentos completados.
2. Ordenar por `resultado.porcentaje` descendente.
3. Obtener los primeros 1,000 resultados.

Variantes:

MongoDB:

- allowDiskUse = false.
- allowDiskUse = true.

PostgreSQL:

- work_mem = 4 MB.
- work_mem = 64 MB.

Métricas prioritarias:

- Spill a disco.
- OOM.
- Throughput.
- Latencia.

## 7. Perfiles de recursos

| Perfil             | CPU |     RAM | Swappiness |
| ------------------ | --: | ------: | ---------: |
| PROFILE_A_BASELINE |   4 | 4096 MB |          0 |
| PROFILE_B_MEDIUM   |   2 | 2048 MB |          0 |
| PROFILE_C_SEVERE   |   1 | 1024 MB |          0 |

Los límites se aplicarán al motor que esté siendo evaluado.

## 8. Matriz experimental

Se evaluarán las siguientes dimensiones:

- 2 motores.
- 3 perfiles de recursos.
- 2 consultas.
- 3 niveles de concurrencia.
- 3 repeticiones.

Concurrencias:

- 1 cliente.
- 10 clientes.
- 50 clientes.

Total:

`2 × 3 × 2 × 3 × 3 = 108 ejecuciones`

Cada ejecución tendrá:

| Etapa    |     Duración |
| -------- | -----------: |
| Warmup   |  60 segundos |
| Medición | 300 segundos |
| Cooldown |  60 segundos |

## 9. Monitorización

Prometheus deberá recolectar métricas cada segundo.

Fuentes:

- cAdvisor.
- Node Exporter.

KPIs principales:

### Rendimiento

- throughput_qps.
- latency_p50_ms.
- latency_p95_ms.
- latency_p99_ms.

### CPU

- container_cpu_cfs_throttled_seconds_total.

### Memoria

- container_memory_rss_bytes.

### Motor

- disk_spill_detected.

### Estabilidad

- oom_killed.

## 10. Resultados y evidencias

Cada ejecución deberá generar:

- Una fila en el CSV normalizado.
- Identificador único de ejecución.
- Registro de configuración.
- Logs.
- Estado de ejecución.
- Métricas recopiladas.

Estados:

- ok.
- spill.
- oom.
- error.

También se conservarán:

- Planes de ejecución de MongoDB.
- EXPLAIN de PostgreSQL.
- Evidencias de spill.
- Evidencias de OOM.
- Resultados de Prometheus.
- Hash del dataset utilizado.

## 11. Automatización

El proyecto deberá permitir ejecutar automáticamente toda la matriz experimental.

El ejecutor deberá:

1. Verificar que Docker esté disponible.
2. Verificar que los motores estén preparados.
3. Comprobar que el dataset esté generado y cargado.
4. Configurar el perfil de recursos.
5. Ejecutar la consulta correspondiente.
6. Registrar las métricas.
7. Guardar los resultados.
8. Continuar con el siguiente escenario.
9. Recuperarse de interrupciones mediante checkpoints.

La ejecución deberá ser secuencial para evitar que dos motores compitan por los mismos recursos.

Se deberá implementar un mecanismo de reanudación que evite repetir ejecuciones ya completadas.

## 12. Reproducibilidad

Un tercero deberá poder:

1. Obtener el repositorio.
2. Configurar las variables de entorno.
3. Levantar los servicios.
4. Generar o cargar el dataset.
5. Ejecutar la matriz experimental.
6. Consultar los resultados.
7. Revisar los planes de ejecución.

Las configuraciones, versiones, semillas y parámetros utilizados deberán quedar registrados.

## 13. Restricciones

- No utilizar datos personales reales.
- No modificar el modelo de producción de ETICORE.
- No ejecutar ambos motores bajo carga simultáneamente.
- No alterar los perfiles durante una ejecución.
- No eliminar resultados anteriores.
- No utilizar índices diferentes que favorezcan injustamente a un motor.
- No ejecutar pruebas sin validar previamente el entorno.

## 14. Consideraciones finales

El proyecto no pretende demostrar que un motor sea universalmente superior al otro.

Su finalidad es comparar su comportamiento bajo condiciones experimentales controladas, utilizando estructuras documentales y cargas de trabajo equivalentes.

Los resultados deberán interpretarse considerando las configuraciones, limitaciones y planes de ejecución de cada motor.
