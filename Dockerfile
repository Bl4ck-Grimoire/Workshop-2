# ==============================================================================
# Imagen personalizada con soporte para TaskFlow API, Great Expectations y MySQL
# ==============================================================================

ARG AIRFLOW_IMAGE=apache/airflow:2.10.4-python3.11
FROM ${AIRFLOW_IMAGE}

USER root

# Paquetes del sistema operativo requeridos para compiladores y clientes de red
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiar el archivo de requerimientos e instalar dependencias de Python
COPY --chown=airflow:root requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Configurar variable de entorno para que Python reconozca el módulo 'src'
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
