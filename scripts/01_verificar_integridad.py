#!/usr/bin/env python3
"""
Fase 1: Pasarela de Integridad Criptográfica por Lotes (Data Quality Gate).
Módulo 5075 - Big Data Aplicado (RA3 / CE 3.b).

Funcionalidad:
1. Lee el manifiesto oficial 'manifest_checksums.sha256'.
2. Calcula el digest SHA-256 real de cada fichero en 'raw_batch_202611/'.
3. Si coincide, mueve el fichero a 'staging/verified/'.
4. Si discrepa, aísla el fichero en 'quarantine/tampered/', registra una alerta
   y documenta el incidente en 'output_samples/quarantine_manifest.txt'.
"""

from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
import shutil

# ---------------------------------------------------------------------------
# 1. Configuración de rutas relativas a la raíz del proyecto
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_SAMPLES_DIR = BASE_DIR / "output_samples"

LOG_FILE = LOGS_DIR / "ejecucion_pipeline.log"
QUARANTINE_MANIFEST_FILE = OUTPUT_SAMPLES_DIR / "quarantine_manifest.txt"

CHUNK_SIZE = 65536  # 64 KiB por bloque para lectura eficiente en memoria


def configurar_logger() -> logging.Logger:
    """Configura salida simultánea a consola y a logs/ejecucion_pipeline.log."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline_kdd")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Manejador de archivo (mode='w' inicia un log limpio en cada ejecución completa)
    fh = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    fh.setFormatter(formato)

    # Manejador de terminal
    sh = logging.StreamHandler()
    sh.setFormatter(formato)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def calcular_sha256(ruta_fichero: Path) -> str:
    """Calcula el hash SHA-256 leyendo el archivo binario en bloques de 64 KiB."""
    hasher = hashlib.sha256()
    with ruta_fichero.open("rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            hasher.update(chunk)
    return hasher.hexdigest()


def leer_manifiesto(ruta_manifiesto: Path) -> list[tuple[str, str]]:
    """
    Parsea líneas con formato estándar sha256sum:
    '<hash_hex_64_caracteres>  <nombre_fichero>'
    """
    entradas: list[tuple[str, str]] = []
    with ruta_manifiesto.open("r", encoding="utf-8") as f:
        for num_linea, linea in enumerate(f, start=1):
            linea_limpia = linea.strip()
            if not linea_limpia or linea_limpia.startswith("#"):
                continue
            partes = linea_limpia.split(maxsplit=1)
            if len(partes) != 2:
                raise ValueError(
                    f"Formato inválido en manifiesto (línea {num_linea}): {linea_limpia}"
                )
            hash_esperado, nombre_archivo = partes
            # Elimina posible '*' si el manifiesto se generó con sha256sum -b
            nombre_archivo = nombre_archivo.lstrip("*")
            entradas.append((hash_esperado.lower(), nombre_archivo))
    return entradas


def main() -> None:
    logger = configurar_logger()

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("INICIO FASE 1: PASARELA DE INTEGRIDAD CRIPTOGRÁFICA (SHA-256)")
    logger.info("=" * 70)

    if not MANIFEST_PATH.exists():
        logger.error("No se encuentra el manifiesto oficial en: %s", MANIFEST_PATH)
        raise FileNotFoundError(f"Falta el manifiesto: {MANIFEST_PATH}")

    entradas = leer_manifiesto(MANIFEST_PATH)
    logger.info("Manifiesto cargado correctamente (%d ficheros declarados).", len(entradas))

    aprobados = 0
    rechazados = 0
    incidencias_cuarentena: list[str] = []

    for hash_esperado, nombre_fichero in entradas:
        ruta_origen = RAW_DIR / nombre_fichero
        ruta_staging = STAGING_DIR / nombre_fichero
        ruta_cuarentena = QUARANTINE_TAMPERED_DIR / nombre_fichero

        # Control de idempotencia por si el script se ejecuta dos veces sin restaurar raw
        if not ruta_origen.exists():
            if ruta_staging.exists() or ruta_cuarentena.exists():
                logger.warning(
                    "El fichero '%s' ya fue clasificado en una ejecución previa. "
                    "Restaura 'raw_batch_202611/' si deseas repetir desde cero.",
                    nombre_fichero,
                )
            else:
                logger.error(
                    "ALERTA: El fichero '%s' figura en el manifiesto pero NO existe en %s.",
                    nombre_fichero,
                    RAW_DIR,
                )
                rechazados += 1
            continue

        hash_calculado = calcular_sha256(ruta_origen)

        if hash_calculado == hash_esperado:
            shutil.move(str(ruta_origen), str(ruta_staging))
            aprobados += 1
            logger.info(
                "[OK] %-32s | SHA-256 verificado -> Movido a staging/verified/",
                nombre_fichero,
            )
        else:
            shutil.move(str(ruta_origen), str(ruta_cuarentena))
            rechazados += 1
            logger.error(
                "[ALERTA INTEGRIDAD] %-18s | Hash discrepante -> Movido a quarantine/tampered/",
                nombre_fichero,
            )
            logger.error("    Esperado : %s", hash_esperado)
            logger.error("    Calculado: %s", hash_calculado)

            incidencias_cuarentena.append(
                f"- Archivo: {nombre_fichero}\n"
                f"  Motivo : Discrepancia de firma criptográfica SHA-256 (posible alteración o descarga truncada)\n"
                f"  Hash esperado : {hash_esperado}\n"
                f"  Hash calculado: {hash_calculado}\n"
                f"  Destino       : quarantine/tampered/{nombre_fichero}\n"
            )

    # Generar manifiesto de auditoría de cuarentena (se ampliará en Fase 2 con filas corruptas)
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with QUARANTINE_MANIFEST_FILE.open("w", encoding="utf-8") as qf:
        qf.write("MANIFIESTO DE AUDITORÍA DE CUARENTENA — PRÁCTICA 5075 KDD\n")
        qf.write(f"Fecha de ejecución: {timestamp_utc}\n")
        qf.write("=" * 70 + "\n\n")
        qf.write("[FASE 1 - FICHEROS RECHAZADOS POR INTEGRIDAD SHA-256]\n")
        if incidencias_cuarentena:
            qf.writelines(incidencias_cuarentena)
        else:
            qf.write("Ningún fichero rechazado en esta ejecución.\n")

    total = aprobados + rechazados
    tasa_integridad = (aprobados / total * 100) if total > 0 else 0.0

    logger.info("-" * 70)
    logger.info(
        "RESUMEN FASE 1: %d/%d ficheros válidos (Tasa integridad: %.2f%%) | %d en cuarentena.",
        aprobados,
        total,
        tasa_integridad,
        rechazados,
    )
    logger.info("Manifiesto de cuarentena guardado en: %s", QUARANTINE_MANIFEST_FILE)


if __name__ == "__main__":
    main()
    