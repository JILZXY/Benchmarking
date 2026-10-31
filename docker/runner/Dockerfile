# Imagen del ejecutor del benchmark (generador de dataset + runner).
ARG PYTHON_IMAGE=python:3.12.11-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/runner/src \
    TZ=UTC

WORKDIR /app

COPY runner/python/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# El código se monta como volumen (./runner/python -> /app/runner) para que los
# cambios no requieran reconstruir la imagen; esta copia sirve como respaldo
# cuando se ejecuta la imagen sin volúmenes.
COPY runner/python /app/runner

CMD ["python", "-c", "import sys; print('eticore runner listo - Python', sys.version.split()[0])"]

