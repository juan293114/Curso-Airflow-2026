# 🌍 Earthquake Lakehouse – Arquitectura Medallion con Airflow

## 🚀 Descripción del Proyecto

Proyecto end-to-end de Ingeniería de Datos que implementa una arquitectura Lakehouse utilizando el patrón Medallion (Raw, Bronze, Silver, Gold), orquestado con Apache Airflow y visualizado en Power BI.

Este proyecto simula un entorno productivo de procesamiento de datos sísmicos obtenidos desde la API pública de USGS.

---

## 🛠 Tecnologías Utilizadas

- Apache Airflow (Orquestación de pipelines)
- Docker (Entorno contenerizado)
- Python
- Almacenamiento en formato Parquet
- Arquitectura Medallion
- Modelado Dimensional (Esquema Estrella)
- Power BI (Visualización)

---

## 🏗 Arquitectura del Proyecto

Flujo de procesamiento:

USGS API (JSON)

   ↓
   
Raw Layer

   ↓
   
Bronze Layer

   ↓
   
Silver Layer

   ↓
   
Gold Layer (Modelo Estrella)

   ↓
   
Power BI

---

## 🔹 Capas de la Arquitectura

### 🟤 Raw
- Ingesta de datos en formato JSON desde API externa.
- Conservación de datos originales.

### 🟠 Bronze
- Conversión a formato estructurado (Parquet).
- Normalización inicial.

### ⚪ Silver
- Limpieza de datos.
- Eliminación de duplicados.
- Estandarización de tipos.
- Validaciones básicas de calidad.

### 🟡 Gold
- Implementación de modelo dimensional (Star Schema).
- Tabla de hechos: `fact_earthquakes`
- Tablas de dimensiones:
  - dim_date
  - dim_location
  - dim_event_type
  - dim_status

---

## ⭐ Modelo Dimensional

El modelo en la capa Gold sigue un esquema estrella:

- fact_earthquakes (tabla de hechos)
- dim_date
- dim_location
- dim_event_type
- dim_status

Esto permite análisis eficientes en herramientas de BI.

---

## 📊 Dashboard en Power BI

Se desarrolló un dashboard analítico que permite:

- Análisis temporal de actividad sísmica.
- Tendencias de magnitud promedio.
- Visualización geográfica de eventos.
- Relación profundidad vs magnitud.
- Análisis de eventos con alerta de tsunami.

---

## 🧠 Conceptos de Ingeniería Aplicados

- Orquestación de procesos ETL con Airflow.
- Uso de TaskGroups.
- Arquitectura Lakehouse basada en Medallion.
- Modelado dimensional.
- Separación clara de capas.
- Versionamiento con Git.
- Contenerización con Docker.

---

## ▶️ Cómo Ejecutar el Proyecto

1. Clonar el repositorio:

```bash
git clone https://github.com/22juan22/earthquake-lakehouse-airflow.git
