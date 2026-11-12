@'
# Informe Técnico de Integridad Criptográfica, Saneamiento y Calidad de Datos (KDD / ISO/IEC 25012)

- **Módulo Profesional:** 5075 — Big Data Aplicado (Real Decreto 279/2021, de 20 de abril)
- **Resultados de Aprendizaje implicados:** RA3 (CE 3.a, CE 3.b, CE 3.c, CE 3.d) y RA5 (CE 5.a, CE 5.b)
- **Lote analizado:** `raw_batch_202611/`
- **Dataset resultante:** `processed/dataset_kdd_listo.csv`

---

## 1. Resumen Ejecutivo y Arquitectura de la Pasarela de Calidad

Con el objetivo de evitar la contaminación de los almacenes analíticos de *Business Intelligence* (BI) y garantizar la fiabilidad de los modelos posteriores de minería de datos, se ha implementado un *pipeline* automatizado en Python estructurado en tres etapas secuenciales (*Data Quality Gate*):

1. **Pasarela Criptográfica por Lotes (`01_verificar_integridad.py`):** Verificación de sumas de control SHA-256 contra el manifiesto oficial `manifest_checksums.sha256` antes de cualquier lectura analítica.
2. **Saneamiento Estructural, de Dominio y Detección de Anomalías (`02_pipeline_saneamiento.py`):** Validación de esquema, normalización temporal ISO 8601, cumplimiento de reglas de negocio, aislamiento de valores atípicos mediante rango intercuartílico (IQR) e imputación controlada de variables secundarias.
3. **Perfilado KDD y Certificación ISO/IEC 25012 (`02_pipeline_saneamiento.py`):** Exportación del conjunto saneado y generación de evidencias auditables en JSON, CSV y texto plano.

---

## 2. Resultados de la Fase 1: Control de Integridad Criptográfica (SHA-256)

Se contrastaron los 3 ficheros declarados en `manifest_checksums.sha256` mediante lectura binaria por bloques de 64 KiB:

| Fichero de origen | Hash SHA-256 esperado (prefijo) | Hash SHA-256 calculado (prefijo) | Estado | Destino en el pipeline |
| :--- | :--- | :--- | :--- | :--- |
| `transacciones_tienda_norte.csv` | `34cc61612494703f...` | `34cc61612494703f...` | **Válido** | `staging/verified/` |
| `transacciones_tienda_sur.csv` | `0773c2cde0d45d3b...` | `1c47dff4bf34eff5...` | **Corrupto / Alterado** | `quarantine/tampered/` |
| `transacciones_tienda_este.csv` | `645e9de21d1fbe99...` | `645e9de21d1fbe99...` | **Válido** | `staging/verified/` |

- **Tasa de integridad criptográfica del lote:** **66,67 %** (2 de 3 ficheros verificados; 1 fichero aislado).
- **Decisión operativa:** `transacciones_tienda_sur.csv` fue segregado automáticamente a `quarantine/tampered/` sin interrumpir la ejecución del *pipeline*, evitando que registros potencialmente manipulados o truncados en red (como el importe anómalo de `9999,99 €` detectado en su interior) ingresaran en la fase de transformación.

---

## 3. Resultados de la Fase 2: Saneamiento, Coherencia y Detección de Outliers

Superar la firma criptográfica certifica la integridad en tránsito del fichero, pero no su validez semántica. Al concatenar los dos ficheros aprobados (`transacciones_tienda_norte.csv` con 50 filas y `transacciones_tienda_este.csv` con 50 filas), entraron **100 registros** en la fase de saneamiento:

### 3.1. Registros segregados a cuarentena (`quarantine/bad_records.csv`)

Se aislaron **13 registros no conformes (13,00 %)** pertenecientes íntegramente a `transacciones_tienda_este.csv`, documentando el motivo exacto de cada rechazo:

| Criterio de validación aplicado | Regla de negocio / estadística | Registros rechazados | Ejemplos detectados |
| :--- | :--- | :---: | :--- |
| **Integridad de clave primaria** | `id_transaccion` no nulo y único | **2** | Filas sin identificador (`SIN_ID`) |
| **Coherencia temporal (ISO 8601)** | Formato `YYYY-MM-DD HH:MM:SS` y año `2026` | **3** | `2026-99-99 INVALID_TS`, `2024-02-11`, `2027-08-21` |
| **Reglas de dominio económico** | `importe_eur > 0` y no nulo | **3** | `-34,50 €`, `0,00 €`, valor nulo |
| **Reglas de dominio logístico** | `unidades >= 1` y no nulo | **2** | `0` unidades, `-3` unidades |
| **Detección de outliers (IQR Tukey)** | $Q_1 - 1{,}5 \cdot \text{IQR} \le \text{importe\_eur} \le Q_3 + 1{,}5 \cdot \text{IQR}$ | **3** | `1480,00 €`, `2750,90 €`, `4120,00 €` (> `138,61 €`) |
| **Total segregado a cuarentena** | — | **13** | Guardados en `quarantine/bad_records.csv` |

### 3.2. Imputación controlada de variables secundarias

Para preservar transacciones comercialmente válidas que presentaban ausencias aisladas en atributos descriptivos no críticos, se aplicó imputación estadística:

- **`categoria` (1 registro nulo — `TXE-20261110-0007`):** Imputado mediante la **moda estadística** (`Electronica`).
- **`metodo_pago` (1 registro nulo — `TXE-20261110-0015`):** Imputado mediante la **moda estadística** (`Transferencia`).

---

## 4. Perfilado de Calidad según la Norma ISO/IEC 25012 (Fase 3)

Los resultados extraídos de `output_samples/metricas_perfilado.json` certifican el cumplimiento de las dimensiones de calidad inherente del estándar **ISO/IEC 25012**:

| Dimensión ISO/IEC 25012 | Indicador medido | Valor inicial (Staging) | Valor final (Dataset KDD) |
| :--- | :--- | :---: | :---: |
| **Completitud** | $\frac{\text{Registros sin nulos}}{\text{Registros totales}} \times 100$ | 95,00 % (95/100) | **100,00 % (87/87)** |
| **Exactitud / Validez** | Conformidad con reglas de dominio e IQR | 87,00 % | **100,00 % (87/87)** |
| **Consistencia** | Formato temporal ISO 8601 y tipado estricto | 97,00 % | **100,00 % (87/87)** |
| **Unicidad** | Claves `id_transaccion` distintas sobre el total | 98,00 % (2 nulos) | **100,00 % (87/87)** |
| **Trazabilidad / Auditabilidad** | Registro en logs y manifiesto de cuarentena | — | **100,00 % auditado** |

### 4.1. Distribución estadística de variables clave tras la limpieza ($N = 87$)

| Variable | Media | Mediana (P50) | Desv. Estándar | Mínimo | Percentil 25 | Percentil 75 | Máximo |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`importe_eur`** | 70,99 € | 73,44 € | 26,34 € | 12,50 € | 53,80 € | 85,34 € | 124,47 € |
| **`unidades`** | 3,79 | 4,00 | 1,69 | 1 | 2 | 5 | 6 |

---

## 5. Conclusiones Técnicas y Preparación para el Proceso KDD

1. **Justificación del filtrado frente a la imputación en variables críticas:** Los registros sin `id_transaccion`, con marcas temporales imposibles/futuras o con importes nulos o no positivos se descartaron a cuarentena en lugar de imputarse, ya que inventar claves primarias o importes monetarios introduciría sesgos contables y rompería la trazabilidad auditora.
2. **Elección del método IQR frente a Z-score para outliers:** Se empleó el criterio del rango intercuartílico ($Q_3 + 1{,}5 \times \text{IQR} = 138{,}61\text{ €}$) por ser un estimador no paramétrico robusto: la presencia de tres anomalías de sensor/facturación superiores a `1400 €` habría inflado artificialmente la media y la desviación estándar si se hubiera aplicado un umbral basado en Z-score, enmascarando los propios *outliers*.
3. **Alineación con el proceso KDD (*Knowledge Discovery in Databases*):** El dataset exportado en `processed/dataset_kdd_listo.csv` culmina las etapas de **Selección**, **Preprocesamiento** y **Transformación** del ciclo KDD. Al garantizar ausencia total de nulos, tipos homogéneos, unicidad de clave y eliminación de valores extremos distorsionadores, los algoritmos posteriores de **Minería de Datos** (segmentación *clustering* de cestas, regresión de ventas por categoría o detección de patrones horarios en BI) operarán sobre una distribución estadísticamente estable y libre de ruido.
'@ | Set-Content -Path "reports\INFORME_CALIDAD_KDD.md" -Encoding UTF8

