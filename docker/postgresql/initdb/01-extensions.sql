-- Se ejecuta solo en la primera inicialización del volumen postgresql_data.
-- El esquema de la tabla `intentos` y sus índices se crean en la fase de carga
-- del dataset, no aquí.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

