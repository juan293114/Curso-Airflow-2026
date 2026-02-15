# Taller: Modelado Dimensional y Arquitectura Medallion

**De Datos Crudos (JSON) a Inteligencia de Negocios (Estrella)**

**Autor:** Juan David Ramírez Ávila  
**Requisitos:** Python 3.x con `pandas`, `requests`, `pyarrow`

---

## 🏛️ El Objetivo: Pensamiento Dimensional

En este taller no solo vamos a mover datos. Vamos a **modelar la realidad de un negocio de E-Commerce**.

Tu misión es transformar datos transaccionales desordenados (JSON) en un **Modelo Estrella** que permita responder preguntas de negocio de forma eficiente.

Usaremos la **Arquitectura Medallion** para organizar nuestro proceso de modelado:

| Capa   | Rol en el Modelado                                      | Formato |
|--------|---------------------------------------------------------|---------|
| Bronze | Fuente Original: Sin modelar. "La verdad cruda".        | JSON    |
| Silver | Limpieza: Tipos de datos correctos, estructuras aplanadas. | Parquet |
| Gold   | Modelo Dimensional: Hechos y Dimensiones separadas.     | Parquet |

---

## 1. Preparación del "Data Warehouse"

Creamos la infraestructura física (carpetas) donde vivirá nuestro modelo.

```python
import os
import shutil

# Reiniciamos el entorno para asegurar limpieza
if os.path.exists('datalake'):
    shutil.rmtree('datalake')

# Creamos las tres zonas de modelado
for layer in ['datalake/bronze', 'datalake/silver', 'datalake/gold']:
    os.makedirs(layer, exist_ok=True)

print("✅ Infraestructura de Data Lake creada.")
```

---

## 2. Capa Bronze: La Realidad de los Datos

En el modelado de datos, el primer paso es el **Data Profiling**. Debemos entender qué tenemos antes de diseñar.

**Concepto de Modelado:** Aquí los datos son no estructurados o semi-estructurados. No hay llaves primarias definidas ni integridad referencial garantizada.

```python
import requests
import json

base_url = "https://fakestoreapi.com"
endpoints = ['products', 'users', 'carts']

print("📥 Ingestando datos crudos...")

for e in endpoints:
    # Simulamos la extracción de la fuente
    raw_data = requests.get(f"{base_url}/{e}").json()
    
    # Persistencia en formato nativo (JSON)
    with open(f'datalake/bronze/{e}.json', 'w') as f:
        json.dump(raw_data, f)

print(f"   -> {len(endpoints)} entidades cargadas en Bronze.")
```

---

## 3. Capa Silver: Estandarización y Tipos

Antes de modelar el esquema estrella, necesitamos **tablas limpias**.

**Decisión de Modelado:**

- **Tipos de Datos:** El dinero debe ser numérico (float), las fechas deben ser tiempo (datetime).
- **Aplanamiento:** Los modelos relacionales no manejan bien los objetos anidados (como `address` dentro de `users`). En Silver, "aplanamos" estos atributos.

```python
import pandas as pd

print("⚙️ Aplicando reglas de calidad (Silver Layer)...")

# --- 1. PRODUCTOS ---
# Regla: El precio viene como string o int, forzamos a Float para consistencia financiera.
df_prod = pd.read_json('datalake/bronze/products.json')
df_prod['price'] = df_prod['price'].astype(float)
df_prod.to_parquet('datalake/silver/products.parquet')

# --- 2. USUARIOS ---
# Regla de Modelado: Aplanar la jerarquía.
# 'address' es un diccionario. Lo convertimos en columnas: address_city, address_street...
with open('datalake/bronze/users.json') as f:
    users_data = json.load(f)

# json_normalize es nuestra herramienta de normalización
df_users = pd.json_normalize(users_data, sep='_')
df_users.to_parquet('datalake/silver/users.parquet')

# --- 3. CARRITOS (Transacciones) ---
# Regla: Las fechas deben ser objetos manipulables, no texto.
df_carts = pd.read_json('datalake/bronze/carts.json')
df_carts['date'] = pd.to_datetime(df_carts['date'])
df_carts.to_parquet('datalake/silver/carts.parquet')

print("✅ Datos limpios y estandarizados en formato Parquet.")
```

---

## 4. Capa Gold: Diseño del Esquema Estrella

Este es el **corazón del taller**. Vamos a construir el siguiente modelo lógico:

### 4.1. Definición de Dimensiones

Las dimensiones responden a: **¿Quién?** (User), **¿Qué?** (Product) y **¿Dónde?** (City).

Seleccionamos solo los atributos descriptivos necesarios.

```python
# Leemos desde Silver (nuestra fuente limpia)
s_prod = pd.read_parquet('datalake/silver/products.parquet')
s_user = pd.read_parquet('datalake/silver/users.parquet')

# --- DIMENSIÓN PRODUCTO ---
# Selección de atributos maestros
dim_product = s_prod[['id', 'title', 'category', 'price']].copy()
dim_product.columns = ['product_id', 'product_name', 'category', 'unit_price']

# --- DIMENSIÓN USUARIO ---
# Selección de atributos demográficos y geográficos
dim_user = s_user[['id', 'username', 'email', 'address_city']].copy()
dim_user.columns = ['user_id', 'username', 'email', 'city']

# Materialización
dim_product.to_parquet('datalake/gold/dim_product.parquet')
dim_user.to_parquet('datalake/gold/dim_user.parquet')

print("✅ Dimensiones creadas.")
```

### 4.2. Definición de la Tabla de Hechos (Fact Table)

Aquí tomamos la decisión de modelado más importante: **La Granularidad**.

- **En Bronze/Silver (Carts):** Una fila = Un Carrito (que contiene muchos productos).
- **En Gold (Fact_Sales):** Una fila = Un Producto vendido dentro de un carrito.

Si no hacemos esta transformación (`explode`), no podemos sumar ventas por categoría de producto.

```python
s_cart = pd.read_parquet('datalake/silver/carts.parquet')

# 1. CAMBIO DE GRANULARIDAD (La clave del modelado)
# Convertimos la lista de productos en filas individuales
fact_sales = s_cart.explode('products').reset_index(drop=True)

# 2. EXTRACCIÓN DE LLAVES FORÁNEAS (FK)
# Ahora cada fila tiene un diccionario {'productId': 1, 'quantity': 2}
# Extraemos esos valores para que sean columnas relacionables
fact_sales['product_id'] = fact_sales['products'].apply(lambda x: x['productId'])
fact_sales['quantity'] = fact_sales['products'].apply(lambda x: x['quantity'])

# 3. ENRIQUECIMIENTO (Cálculo de Métricas)
# El hecho "Venta" necesita el monto ($). El carrito no trae precios.
# Hacemos un JOIN con la dimensión producto para traer el precio unitario.
fact_sales = fact_sales.merge(dim_product[['product_id', 'unit_price']], on='product_id', how='left')

# Métrica Calculada: Cantidad * Precio Unitario
fact_sales['total_amount'] = fact_sales['quantity'] * fact_sales['unit_price']

# 4. DIMENSIÓN TIEMPO DERIVADA
# Extraemos atributos de fecha para facilitar el filtrado
fact_sales['year'] = fact_sales['date'].dt.year
fact_sales['month'] = fact_sales['date'].dt.month
fact_sales['day'] = fact_sales['date'].dt.day

# 5. SELECCIÓN FINAL (Esquema Estrella)
# Nos quedamos solo con las llaves (FK) y las métricas numéricas
final_columns = [
    'id',           # Sale ID (Podemos usar el Cart ID como referencia)
    'userId',       # FK hacia Dim_User
    'product_id',   # FK hacia Dim_Product
    'date', 'year', 'month', # Dimensiones Temporales
    'quantity',     # Métrica 1
    'total_amount'  # Métrica 2
]

fact_sales_final = fact_sales[final_columns].rename(columns={'id': 'sale_id', 'userId': 'user_id'})

# Materialización
fact_sales_final.to_parquet('datalake/gold/fact_sales.parquet')

print("✅ Tabla de Hechos (Fact_Sales) creada con granularidad de línea de producto.")
```

---

## 5. Validación del Modelo

Un modelo de datos solo es útil si puede **responder preguntas de negocio**. Usaremos las tablas de la capa Gold para simular un reporte de BI.

**Pregunta de Negocio:** ¿Qué categoría de productos genera más ingresos?

```python
# Cargamos el modelo (simulando Power BI o Tableau)
gold_fact = pd.read_parquet('datalake/gold/fact_sales.parquet')
gold_prod = pd.read_parquet('datalake/gold/dim_product.parquet')

# Hacemos el JOIN entre Hechos y Dimensiones
modelo_estrella = gold_fact.merge(gold_prod, on='product_id')

# Agregación (GROUP BY)
reporte = modelo_estrella.groupby('category')[['quantity', 'total_amount']].sum().sort_values('total_amount', ascending=False)

# Formato visual
pd.options.display.float_format = '${:,.2f}'.format
print(reporte)
```

---

## 📝 Reflexión para el Estudiante

Observa la diferencia entre los archivos de tu Data Lake:

- **Abre el archivo `bronze/users.json`:** Verás una estructura anidada y compleja.
- **Observa `gold/fact_sales`:** Es una tabla numérica, limpia y estrecha.

> **El arte del modelado de datos es hacer que lo complejo (Bronze) se vuelva simple y analizable (Gold).**

---

## ¿Por qué este enfoque es mejor para modelado?

1. **Foco en Granularidad:** Se resalta explícitamente el paso de `explode` como una decisión de granularidad ("Cambio de granularidad" en el comentario del código), que es el concepto más difícil de entender para los novatos.

2. **FKs y Métricas:** Separamos explícitamente las Llaves Foráneas de las Métricas.

3. **Visualización Mental:** El código invita a pensar en tablas que se cruzan (Joins), no solo en scripts secuenciales.

---

## 6. Preguntas de Negocio a Resolver

Ahora que has construido el modelo dimensional, es momento de **probarlo**. Un buen modelo de datos se valida respondiendo preguntas de negocio reales.

### 🟢 Nivel 1: Consultas Básicas (Agregaciones Simples)

**Objetivo:** Verificar que las métricas (`quantity`, `total_amount`) funcionan.

#### 1. ¿Cuál es el Ingreso Total Histórico (Total Revenue) de la compañía?

**Insight:** Conocer el tamaño del negocio.

**Pista:** `fact_sales['total_amount'].sum()`

```python
# Tu código aquí
total_revenue = gold_fact['total_amount'].sum()
print(f"💰 Ingreso Total: ${total_revenue:,.2f}")
```

#### 2. ¿Cuántas unidades (Quantity) se han vendido en total?

**Insight:** Conocer el volumen de logística necesario.

**Pista:** `fact_sales['quantity'].sum()`

```python
# Tu código aquí
total_units = gold_fact['quantity'].sum()
print(f"📦 Unidades Vendidas: {total_units:,}")
```

#### 3. ¿Cuál es el promedio de precio de venta (Average Selling Price)?

**Insight:** Entender el posicionamiento de precios de la tienda.

**Pista:** `fact_sales['unit_price'].mean()` (Requiere cruce con Dim_Product si no persististe el precio en la Fact, o usar la métrica calculada).

```python
# Tu código aquí
avg_price = gold_prod['unit_price'].mean()
print(f"💵 Precio Promedio: ${avg_price:,.2f}")
```

---

### 🟡 Nivel 2: Análisis Dimensional (Joins y Group By)

**Objetivo:** Verificar que las Llaves Foráneas (FK) conectan correctamente con las Dimensiones.

#### 4. ¿Cuál es la Categoría de Productos más rentable?

**Insight:** Identificar qué segmento del negocio "paga las cuentas".

**Pista:**
- Join `Fact_Sales` con `Dim_Product`
- Agrupar por `category`
- Sumar `total_amount`
- Ordenar descendente

```python
# Tu código aquí
categoria_rentable = modelo_estrella.groupby('category')['total_amount'].sum().sort_values(ascending=False)
print("📊 Ingresos por Categoría:")
print(categoria_rentable)
```

#### 5. Top 5: ¿Cuáles son los productos "Estrella" (Más vendidos por ingresos)?

**Insight:** Gestión de inventario y marketing para los productos líderes.

**Pista:** Agrupar por `product_name` -> Sumar `total_amount` -> `.head(5)`

```python
# Tu código aquí
top_productos = modelo_estrella.groupby('product_name')['total_amount'].sum().sort_values(ascending=False).head(5)
print("⭐ Top 5 Productos Estrella:")
print(top_productos)
```

#### 6. ¿Cuál es la ciudad (City) con mayor volumen de compras?

**Insight:** Identificar dónde concentrar los esfuerzos de logística o publicidad local.

**Pista:** Join `Fact_Sales` con `Dim_User` -> Agrupar por `city`

```python
# Tu código aquí
gold_user = pd.read_parquet('datalake/gold/dim_user.parquet')
ventas_por_ciudad = gold_fact.merge(gold_user, on='user_id').groupby('city')['total_amount'].sum().sort_values(ascending=False)
print("🌆 Ventas por Ciudad:")
print(ventas_por_ciudad.head(10))
```

---

### 🔴 Nivel 3: Análisis Avanzado (Granularidad y Tiempo)

**Objetivo:** Probar la lógica de negocio y el manejo de fechas.

#### 7. Análisis de Tendencia: ¿Cómo se comportan las ventas por mes?

**Insight:** Detectar estacionalidad (ej. ¿se vende más en diciembre?).

**Pista:** Usar la columna `month` o `date` de la Fact Table para agrupar y graficar una línea de tiempo.

```python
# Tu código aquí
ventas_mensuales = gold_fact.groupby('month')['total_amount'].sum().sort_index()
print("📈 Tendencia de Ventas Mensuales:")
print(ventas_mensuales)

# Opcional: Graficar
# import matplotlib.pyplot as plt
# ventas_mensuales.plot(kind='line', marker='o')
# plt.title('Ventas por Mes')
# plt.xlabel('Mes')
# plt.ylabel('Ingresos ($)')
# plt.show()
```

#### 8. ¿Cuál es el "Ticket Promedio" (AOV - Average Order Value)?

**¡Pregunta Trampa!** (Ideal para evaluar modelado)

**Insight:** Cuánto gasta un cliente cada vez que compra.

**El Reto:** La `Fact_Sales` tiene granularidad de producto, no de transacción.

**Pista:**
1. Primero, agrupar por `sale_id` (o `cart_id`) para obtener el total de cada carrito
2. Luego, calcular el promedio (`mean`) de esos totales

```python
# Tu código aquí
ticket_por_venta = gold_fact.groupby('sale_id')['total_amount'].sum()
aov = ticket_por_venta.mean()
print(f"🛒 Ticket Promedio (AOV): ${aov:,.2f}")
```

#### 9. ¿Quiénes son los clientes "VIP"? (Top 3 clientes por gasto acumulado)

**Insight:** Programas de fidelización.

**Pista:** Join con `Dim_User` -> Agrupar por `username` o `email` -> Sumar `total_amount`

```python
# Tu código aquí
clientes_vip = gold_fact.merge(gold_user, on='user_id').groupby('username')['total_amount'].sum().sort_values(ascending=False).head(3)
print("👑 Top 3 Clientes VIP:")
print(clientes_vip)
```

---

### 🧪 Reto de Validación Final (El Examen)

Si ejecutaste correctamente todo el taller, tu modelo debe ser capaz de responder esta pregunta:

**"Calcular el porcentaje de ingresos que proviene de la categoría 'electronics'"**

$$\text{Share} = \frac{\text{Ventas de Electrónica}}{\text{Ventas Totales}} \times 100$$

```python
# Reto de Validación
total_ventas = modelo_estrella['total_amount'].sum()
ventas_electronics = modelo_estrella[modelo_estrella['category'] == 'electronics']['total_amount'].sum()
porcentaje = (ventas_electronics / total_ventas) * 100

print(f"🔌 Participación de Electronics: {porcentaje:.2f}%")
```

**Si obtienes un número coherente (ej. "35%" o similar), ¡aprobaste el taller de modelado dimensional! 🎓**

---

## Instrucciones de Ejecución

Para ejecutar este taller, puedes:

1. **Copiar y pegar cada bloque de código** en un intérprete de Python o Jupyter Notebook
2. **Ejecutar todo el código secuencialmente** desde un archivo `.py`
3. **Convertirlo a Jupyter Notebook** para una experiencia interactiva en VS Code

Asegúrate de tener instaladas las dependencias:
```bash
pip install pandas requests pyarrow
```
