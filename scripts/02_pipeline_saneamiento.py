#!/usr/bin/env python3
"""
Fases 2 y 3: Pipeline de Saneamiento, Detección de Outliers y Perfilado KDD.
Módulo 5075 - Big Data Aplicado (RA3 / CE 3.c, CE 3.d y RA5 / CE 5.a, CE 5.b).

Funcionalidad:
1. Carga y concatena los ficheros verificados en 'staging/verified/'.
2. Valida esquema, claves obligatorias, formato ISO 8601, año en curso (2026)
   y reglas de dominio (importe_eur > 0, unidades >= 1).
3. Detecta valores atípicos (outliers) en 'importe_eur' mediante el método IQR.
4. Aísla todos los registros no conformes en 'quarantine/bad_records.csv' con su
   motivo detallado y actualiza 'output_samples/quarantine_manifest.txt'.
5. Imputa valores nulos en variables secundarias (moda en categóricas / mediana en numéricas).
6. Exporta el dataset limpio en 'processed/dataset_kdd_listo.csv'.
7. Calcula métricas de calidad ISO/IEC 25012 y las guarda en
   'output_samples/metricas_perfilado.json'.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 1. Configuración de rutas y parámetros de negocio
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_DIR = BASE_DIR / "quarantine"
QUARANTINE_TAMPERED_DIR = QUARANTINE_DIR / "tampered"
BAD_RECORDS_FILE = QUARANTINE_DIR / "bad_records.csv"
PROCESSED_DIR = BASE_DIR / "processed"
DATASET_KDD_FILE = PROCESSED_DIR / "dataset_kdd_listo.csv"
LOGS_DIR = BASE_DIR / "logs"
LOG_FILE = LOGS_DIR / "ejecucion_pipeline.log"
OUTPUT_SAMPLES_DIR = BASE_DIR / "output_samples"
QUARANTINE_MANIFEST_FILE = OUTPUT_SAMPLES_DIR / "quarantine_manifest.txt"
METRICS_JSON_FILE = OUTPUT_SAMPLES_DIR / "metricas_perfilado.json"

COLUMNAS_ESPERADAS = [
    "id_transaccion",
    "tienda",
    "timestamp",
    "categoria",
    "importe_eur",
    "unidades",
    "metodo_pago",
]
ANIO_EN_CURSO = 2026
FECHA_CORTE_MAXIMA = pd.Timestamp("2026-12-31 23:59:59")


def configurar_logger() -> logging.Logger:
    """Añade registros al log general del pipeline (logs/ejecucion_pipeline.log)."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline_kdd_saneamiento")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # mode='a' conserva la trazabilidad de la Fase 1 en el mismo fichero de log
    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setFormatter(formato)

    sh = logging.StreamHandler()
    sh.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def cargar_ficheros_staging(logger: logging.Logger) -> tuple[pd.DataFrame, list[str]]:
    """Lee y concatena todos los CSV aprobados en staging/verified/."""
    archivos_csv = sorted(STAGING_DIR.glob("*.csv"))
    if not archivos_csv:
        raise FileNotFoundError(
            f"No hay archivos CSV en {STAGING_DIR}. Ejecuta primero 01_verificar_integridad.py."
        )

    dataframes: list[pd.DataFrame] = []
    nombres: list[str] = []

    for ruta_csv in archivos_csv:
        df_parcial = pd.read_csv(ruta_csv)
        faltantes = set(COLUMNAS_ESPERADAS) - set(df_parcial.columns)
        if faltantes:
            raise ValueError(
                f"Schema drift detectado en {ruta_csv.name}. Faltan columnas: {faltantes}"
            )
        df_parcial["fichero_origen"] = ruta_csv.name
        dataframes.append(df_parcial)
        nombres.append(ruta_csv.name)
        logger.info("Cargado desde staging: %-32s (%d filas)", ruta_csv.name, len(df_parcial))

    df_total = pd.concat(dataframes, ignore_index=True)
    return df_total, nombres


def evaluar_reglas_fila(fila: pd.Series) -> list[str]:
    """
    Evalúa reglas estructurales, temporales y de dominio para una fila.
    Devuelve una lista vacía si la fila es válida o los motivos de rechazo.
    """
    motivos: list[str] = []

    # 1. Clave primaria obligatoria
    id_tx = fila["id_transaccion"]
    if pd.isna(id_tx) or str(id_tx).strip() == "":
        motivos.append("id_transaccion_nulo")

    # 2. Coherencia temporal ISO 8601 y año en curso (2026)
    ts = pd.to_datetime(fila["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if pd.isna(ts):
        motivos.append(f"timestamp_formato_invalido ({fila['timestamp']})")
    else:
        if ts.year != ANIO_EN_CURSO or ts > FECHA_CORTE_MAXIMA:
            motivos.append(f"timestamp_fuera_de_rango ({fila['timestamp']})")

    # 3. Reglas de dominio: importe_eur > 0 y unidades >= 1
    importe = pd.to_numeric(fila["importe_eur"], errors="coerce")
    if pd.isna(importe):
        motivos.append("importe_eur_nulo_o_no_numerico")
    elif importe <= 0:
        motivos.append(f"importe_eur_no_positivo ({importe})")

    unidades = pd.to_numeric(fila["unidades"], errors="coerce")
    if pd.isna(unidades):
        motivos.append("unidades_nulo_o_no_numerico")
    elif unidades < 1:
        motivos.append(f"unidades_menor_que_1 ({unidades})")

    return motivos


def main() -> None:
    logger = configurar_logger()

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("")
    logger.info("=" * 70)
    logger.info("INICIO FASE 2: LIMPIEZA, COHERENCIA Y DETECCIÓN DE OUTLIERS")
    logger.info("=" * 70)

    # 1. Carga desde staging/verified/
    df_bruto, ficheros_procesados = cargar_ficheros_staging(logger)
    total_registros_entrada = len(df_bruto)
    registros_sin_nulos_inicial = int(df_bruto[COLUMNAS_ESPERADAS].notna().all(axis=1).sum())
    tasa_completitud_inicial = (
        (registros_sin_nulos_inicial / total_registros_entrada * 100)
        if total_registros_entrada > 0
        else 0.0
    )

    logger.info(
        "Total registros concatenados: %d | Completitud inicial (sin ningún nulo): %.2f%% (%d/%d)",
        total_registros_entrada,
        tasa_completitud_inicial,
        registros_sin_nulos_inicial,
        total_registros_entrada,
    )

    # 2. Validación de reglas de esquema, tiempo y dominio
    motivos_por_fila = df_bruto.apply(evaluar_reglas_fila, axis=1)
    mascara_invalidos_dominio = motivos_por_fila.apply(lambda m: len(m) > 0)

    df_rechazados_dominio = df_bruto[mascara_invalidos_dominio].copy()
    df_rechazados_dominio["motivo_cuarentena"] = motivos_por_fila[mascara_invalidos_dominio].apply(
        lambda m: "; ".join(m)
    )

    df_candidatos = df_bruto[~mascara_invalidos_dominio].copy()
    logger.info(
        "Filtro de dominio y tiempo: %d registros válidos | %d registros enviados a cuarentena.",
        len(df_candidatos),
        len(df_rechazados_dominio),
    )

    # 3. Control de unicidad sobre id_transaccion
    duplicados_mask = df_candidatos.duplicated(subset=["id_transaccion"], keep="first")
    df_duplicados = df_candidatos[duplicados_mask].copy()
    if not df_duplicados.empty:
        df_duplicados["motivo_cuarentena"] = "id_transaccion_duplicado"
        logger.warning("Detectados %d registros duplicados por id_transaccion.", len(df_duplicados))
    df_candidatos = df_candidatos[~duplicados_mask].copy()

    # Conversión de tipos limpios antes del análisis estadístico
    df_candidatos["importe_eur"] = pd.to_numeric(df_candidatos["importe_eur"])
    df_candidatos["unidades"] = pd.to_numeric(df_candidatos["unidades"]).astype(int)
    df_candidatos["timestamp"] = pd.to_datetime(
        df_candidatos["timestamp"], format="%Y-%m-%d %H:%M:%S"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # 4. Detección estadística de outliers en importe_eur (Método IQR de Tukey)
    q1 = float(df_candidatos["importe_eur"].quantile(0.25))
    q3 = float(df_candidatos["importe_eur"].quantile(0.75))
    iqr = q3 - q1
    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    logger.info(
        "Umbrales IQR para importe_eur: Q1=%.2f | Q3=%.2f | IQR=%.2f | Rango normal=[%.2f, %.2f]",
        q1,
        q3,
        iqr,
        max(0.0, limite_inferior),
        limite_superior,
    )

    mascara_outliers = (df_candidatos["importe_eur"] < limite_inferior) | (
        df_candidatos["importe_eur"] > limite_superior
    )
    df_outliers = df_candidatos[mascara_outliers].copy()
    df_outliers["motivo_cuarentena"] = df_outliers["importe_eur"].apply(
        lambda val: f"outlier_estadistico_iqr (importe={val:.2f} > {limite_superior:.2f})"
    )

    df_limpio = df_candidatos[~mascara_outliers].copy()
    logger.info(
        "Detección de outliers: %d valores atípicos segregados | %d registros conservados.",
        len(df_outliers),
        len(df_limpio),
    )

    # 5. Consolidar todos los registros rechazados en quarantine/bad_records.csv
    df_cuarentena_total = pd.concat(
        [df_rechazados_dominio, df_duplicados, df_outliers], ignore_index=True
    )
    df_cuarentena_total.to_csv(BAD_RECORDS_FILE, index=False, encoding="utf-8")
    logger.info(
        "Registros anómalos exportados a: %s (%d filas)",
        BAD_RECORDS_FILE,
        len(df_cuarentena_total),
    )

    # 6. Imputación justificada de nulos en variables secundarias
    imputaciones_realizadas: dict[str, dict[str, str | int]] = {}
    for col_cat in ["categoria", "metodo_pago"]:
        nulos_col = int(df_limpio[col_cat].isna().sum())
        if nulos_col > 0:
            moda_val = str(df_limpio[col_cat].mode(dropna=True)[0])
            df_limpio[col_cat] = df_limpio[col_cat].fillna(moda_val)
            imputaciones_realizadas[col_cat] = {
                "estrategia": "moda_estadistica",
                "valor_imputado": moda_val,
                "registros_imputados": nulos_col,
            }
            logger.info(
                "Imputación secundaria en '%s': %d nulo(s) reemplazado(s) por la moda '%s'.",
                col_cat,
                nulos_col,
                moda_val,
            )

    # -----------------------------------------------------------------------
    # FASE 3: Exportación KDD y Perfilado de Calidad ISO/IEC 25012
    # -----------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info("INICIO FASE 3: EXPORTACIÓN KDD Y PERFILADO ISO/IEC 25012")
    logger.info("=" * 70)

    df_export = df_limpio[COLUMNAS_ESPERADAS].copy()
    df_export["importe_eur"] = df_export["importe_eur"].round(2)
    df_export.to_csv(DATASET_KDD_FILE, index=False, encoding="utf-8")
    logger.info(
        "Dataset limpio listo para BI/KDD guardado en: %s (%d registros).",
        DATASET_KDD_FILE,
        len(df_export),
    )

    # Métricas de integridad criptográfica (Fase 1)
    ficheros_validados = len(list(STAGING_DIR.glob("*.csv")))
    ficheros_rechazados = len(list(QUARANTINE_TAMPERED_DIR.glob("*.csv")))
    total_ficheros_lote = ficheros_validados + ficheros_rechazados
    tasa_integridad_ficheros = (
        (ficheros_validados / total_ficheros_lote * 100) if total_ficheros_lote > 0 else 0.0
    )

    # Métricas de calidad ISO/IEC 25012 (Fase 2-3)
    registros_finales = len(df_export)
    registros_sin_nulos_final = int(df_export.notna().all(axis=1).sum())
    tasa_completitud_final = (
        (registros_sin_nulos_final / registros_finales * 100) if registros_finales > 0 else 0.0
    )
    tasa_validez_registros = (
        (registros_finales / total_registros_entrada * 100) if total_registros_entrada > 0 else 0.0
    )
    tasa_unicidad_final = (
        (df_export["id_transaccion"].nunique() / registros_finales * 100)
        if registros_finales > 0
        else 0.0
    )

    metricas_perfilado = {
        "metadatos_ejecucion": {
            "fecha_generacion_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "estandar_referencia": "ISO/IEC 25012 - Modelo de Calidad de Datos",
            "ficheros_procesados_fase_2": ficheros_procesados,
        },
        "integridad_criptografica_fase_1": {
            "ficheros_totales_lote": total_ficheros_lote,
            "ficheros_validados_sha256": ficheros_validados,
            "ficheros_rechazados_cuarentena": ficheros_rechazados,
            "tasa_integridad_criptografica_pct": round(tasa_integridad_ficheros, 2),
        },
        "metricas_calidad_iso_25012": {
            "registros_entrada_staging": total_registros_entrada,
            "registros_rechazados_cuarentena": int(len(df_cuarentena_total)),
            "registros_validos_kdd": registros_finales,
            "tasa_completitud_inicial_pct": round(tasa_completitud_inicial, 2),
            "tasa_completitud_post_saneamiento_pct": round(tasa_completitud_final, 2),
            "tasa_validez_registros_pct": round(tasa_validez_registros, 2),
            "tasa_unicidad_id_transaccion_pct": round(tasa_unicidad_final, 2),
            "desglose_rechazos_por_regla": {
                "errores_esquema_tiempo_dominio": int(len(df_rechazados_dominio)),
                "duplicados_id": int(len(df_duplicados)),
                "outliers_estadisticos_iqr": int(len(df_outliers)),
            },
            "imputaciones_secundarias": imputaciones_realizadas,
        },
        "perfilado_estadistico_variables_clave": {
            "importe_eur": {
                "media": round(float(df_export["importe_eur"].mean()), 2),
                "mediana_p50": round(float(df_export["importe_eur"].median()), 2),
                "desviacion_estandar": round(float(df_export["importe_eur"].std()), 2),
                "minimo": round(float(df_export["importe_eur"].min()), 2),
                "percentil_25": round(float(df_export["importe_eur"].quantile(0.25)), 2),
                "percentil_75": round(float(df_export["importe_eur"].quantile(0.75)), 2),
                "maximo": round(float(df_export["importe_eur"].max()), 2),
                "umbral_superior_iqr_aplicado": round(limite_superior, 2),
            },
            "unidades": {
                "media": round(float(df_export["unidades"].mean()), 2),
                "mediana_p50": round(float(df_export["unidades"].median()), 2),
                "desviacion_estandar": round(float(df_export["unidades"].std()), 2),
                "minimo": int(df_export["unidades"].min()),
                "maximo": int(df_export["unidades"].max()),
            },
        },
    }

    with METRICS_JSON_FILE.open("w", encoding="utf-8") as jf:
        json.dump(metricas_perfilado, jf, indent=2, ensure_ascii=False)
    logger.info("Métricas de perfilado ISO/IEC 25012 guardadas en: %s", METRICS_JSON_FILE)

    # Ampliar output_samples/quarantine_manifest.txt con el detalle de la Fase 2
    with QUARANTINE_MANIFEST_FILE.open("a", encoding="utf-8") as qf:
        qf.write("\n" + "=" * 70 + "\n")
        qf.write("[FASE 2 - REGISTROS AISLADOS EN QUARANTINE/BAD_RECORDS.CSV]\n")
        qf.write(f"Total registros segregados: {len(df_cuarentena_total)}\n\n")
        for _, row in df_cuarentena_total.iterrows():
            id_str = str(row["id_transaccion"]) if pd.notna(row["id_transaccion"]) else "SIN_ID"
            qf.write(
                f"- ID: {id_str:<18} | Origen: {row['fichero_origen']:<30} | "
                f"Motivo: {row['motivo_cuarentena']}\n"
            )

    logger.info("Manifiesto de cuarentena actualizado con %d registros.", len(df_cuarentena_total))
    logger.info("PIPELINE COMPLETADO CON ÉXITO.")


if __name__ == "__main__":
    main()
