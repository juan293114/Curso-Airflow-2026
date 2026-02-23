# Pipeline ELT Medallion (Airflow Local)

## Resumen
Este proyecto implementa un pipeline ELT con Airflow para consumir la API publica de FakeStore y organizar los datos en una arquitectura Medallion: raw -> bronze -> silver -> gold (dimensiones y hechos).

## Flujo macro
1. Ingesta: descarga JSON desde la API y lo guarda en `data_lake/raw/`.
2. Bronze: convierte el JSON raw a Parquet en `data_lake/bronze/`.
3. Silver: limpia y normaliza columnas en `data_lake/silver/`.
4. Gold: genera dimensiones y tabla de hechos en `data_lake/gold/`.
5. Sensor y notificacion: espera el parquet de facts y finaliza.

## Orquestacion
El DAG principal es `dags/elt_medallon.py`, programado diariamente a las 05:00 (America/Lima). Las dimensiones se ejecutan en paralelo y luego se construye la tabla de hechos.

## Estructura principal
- `dags/elt_medallon.py`: definicion del DAG y dependencias.
- `elt/`: funciones de cada capa (raw, bronze, silver, gold).
- `data_lake/`: salidas del pipeline por capa.
- `docker-compose.yaml`: stack local de Airflow con volúmenes a `/opt/airflow`.

## Notas
- Proyecto pensado para ejecucion local con Docker y Airflow.
- La capa gold queda lista para consumo en BI.

## Como ejecutar

## Requisitos
- Docker Desktop (con Docker Compose habilitado).

## Levantar Airflow con Docker
1. Abre una terminal en la carpeta del proyecto:
```powershell
cd C:\Users\ASUS\OneDrive\Documentos\Learning\ds_data_engineer\Curso-Airflow-2026\B_Proyectos\A_Grupo_1\proyecto_final
```
2. Inicializa la base de datos y el usuario admin:
```powershell
docker compose up airflow-init
```
3. Levanta los servicios:
```powershell
docker compose up -d
```

## Ejecutar el DAG
1. Abre Airflow UI: `http://localhost:8080`
2. Usuario/clave por defecto: `airflow` / `airflow`
3. Busca el DAG `elt_medallon_fakestore`, actívalo (toggle) y haz "Trigger".

## Ver salidas
Los archivos se generan en `data_lake/`:
- `raw/` JSON
- `bronze/` Parquet
- `silver/` Parquet
- `gold/` dimensiones y hechos

## Apagar servicios
```powershell
docker compose down
```
