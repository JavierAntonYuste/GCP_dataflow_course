# Guía detallada para ejecutar un pipeline de Apache Beam en Google Cloud

## Descripción de la actividad

Esta guía te llevará a través de los pasos necesarios para configurar y ejecutar un pipeline de Apache Beam en Google Cloud, desde la preparación de los archivos hasta la ejecución del pipeline en un contenedor Docker.

## Requisitos previos

Antes de comenzar, asegúrate de tener una cuenta de Google Cloud y de estar familiarizado con los conceptos básicos de Google Cloud Storage y Apache Beam.

## Paso 1: Copiar los archivos al Cloud Shell

1. **Acceder a Cloud Shell:**

   Abre Google Cloud Shell desde la consola de Google Cloud.

2. **Copiar los archivos desde el bucket a Cloud Shell:**

   Usa el siguiente comando para copiar los archivos desde el bucket a tu entorno de Cloud Shell:

   ```sh
   gsutil cp gs://<project_name>-bucket/* .
   ```

## Paso 4: Lanzar un contenedor Python

**Ejecutar el contenedor Docker:**

   Ejecuta el siguiente comando para lanzar un contenedor Docker con Python 3.9, montando tu directorio de trabajo actual:

   ```sh
   docker run -it -v /home:/home -e DEVSHELL_PROJECT_ID=$DEVSHELL_PROJECT_ID python:3.9 /bin/bash
   ```


## Paso 5: Instalar las dependencias en el contenedor

**Instalar Apache Beam y otras dependencias:**

   Dentro del contenedor Docker, ejecuta el siguiente comando para instalar Apache Beam con soporte para Google Cloud Platform, pandas y pandasql:

   ```sh
   pip install 'apache-beam[gcp]==2.42.0' pandas pandasql
   ```

## Paso 6: Exportar las variables de entorno

**Configurar las variables de entorno:**

   Exporta las variables de entorno necesarias para tu proyecto:

   ```sh
   export REGION=us-east4
   export PROJECT_NAME=<project_name>
   export BUCKET_NAME=$PROJECT_NAME-bucket
   ```

## Paso 7: Ejecutar el pipeline de Apache Beam

1. **Ejecutar el pipeline:**

   Ejecuta el pipeline dentro del contenedor Docker usando el siguiente comando:

   ```sh
   python -m pipeline_read --project=$PROJECT_NAME \
       --region=$REGION \
       --staging_location=$BUCKET_NAME/staging \
       --temp_location=$BUCKET_NAME/temp \
       --input=$BUCKET_NAME/input.csv \
       --output=$PROJECT_NAME:customers.information
   ```