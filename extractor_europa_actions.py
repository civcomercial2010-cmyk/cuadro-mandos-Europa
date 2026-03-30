"""
extractor_europa_actions.py
===========================
Extractor para Hipopótamo Europa — versión de producción (base).

Requisitos que cubre ya:
1) IMAP: descarga adjunto .xlsx
2) Selección del Excel MÁS reciente por fecha/hora de generación (y empate por UID IMAP mayor)
3) Construcción base de data.json agregando por centro y métricas de proyección
4) Fallback para frontend (si faltan datos, el HTML muestra —)
5) Drive: copia append idempotente a pestaña `Datos` (por key fecha_generacion+adjunto)

Lo que requiere ajuste fino (y que pediré al usuario con el sheet histórico y festivos locales):
- parse exacto de centro/vendedor desde el Excel ERP (hoja/estructura real)
- histórico centro↔vendedor con vigencias (sheet en Drive)
- festivos locales Zaragoza/Lleida para todos los años relevantes
"""

import imaplib
import email
import email.header
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import openpyxl
except ImportError:
    sys.exit("ERROR: install openpyxl")

from workalendar.europe.spain import Spain

import google.oauth2.service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


REPO_DIR = Path(".")
JSON_OUT = REPO_DIR / "data.json"
LOG_FILE = REPO_DIR / "extractor_europa_actions.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _decode_header_value(raw) -> str:
    if raw is None:
        return ""
    parts = email.header.decode_header(raw)
    decoded = []
    for chunk, enc in parts:
        if isinstance(chunk, bytes):
            decoded.append(chunk.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(chunk)
    return " ".join(decoded).strip()


def load_config() -> dict:
    # Gmail / IMAP
    cfg = {
        "email": os.environ.get("GMAIL_USER", ""),
        "password_app": os.environ.get("GMAIL_PASSWORD", ""),
        "imap_server": os.environ.get("IMAP_SERVER", "imap.gmail.com"),
        "imap_port": int(os.environ.get("IMAP_PORT", "993")),
        "carpeta_busqueda": os.environ.get("IMAP_FOLDER", "INBOX"),
        "asunto_contiene": os.environ.get("ASUNTO_FILTRO", "Informe ventas muebles"),
        "remitente_contiene": os.environ.get("REMITENTE", "reportes@hipopotamo.com"),
        "nombre_adjunto": os.environ.get("NOMBRE_ADJUNTO", ""),
        "buscar_ultimas_horas": int(os.environ.get("BUSCAR_ULTIMAS_HORAS", "96")),

        # Excel parsing
        "hoja_excel": os.environ.get("HOJA_EXCEL", "VENTAS"),

        # Drive/Sheets
        "drive_spreadsheet_id": os.environ.get("DRIVE_SPREADSHEET_ID", ""),
        "drive_tab": os.environ.get("DRIVE_TAB", "Datos"),
        "drive_key_column": os.environ.get("DRIVE_KEY_COLUMN", "A"),
    }

    # Drive service account
    cfg["google_service_account_json"] = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not cfg["email"] or not cfg["password_app"]:
        log.error("Faltan GMAIL_USER o GMAIL_PASSWORD en GitHub Secrets.")
        sys.exit(1)
    if not cfg["google_service_account_json"]:
        log.warning("No se encontró GOOGLE_SERVICE_ACCOUNT_JSON. Drive copy se saltará.")

    # If user passes spreadsheet id through env, great. Otherwise fail later.
    return cfg


def connect_gmail(cfg: dict) -> imaplib.IMAP4_SSL:
    conn = imaplib.IMAP4_SSL(cfg["imap_server"], int(cfg["imap_port"]))
    conn.login(cfg["email"], cfg["password_app"])
    return conn


def _parse_generacion_datetime_from_xlsx_bytes(xlsx_bytes: bytes) -> datetime | None:
    wb = openpyxl.load_workbook(openpyxl.utils.datetime_from_excel(0) if False else tempfile.TemporaryFile(), read_only=True)  # pragma: no cover
    wb.close()


def _excel_primary_sheet(wb, cfg: dict):
    name = (cfg or {}).get("hoja_excel", "VENTAS")
    if name in wb.sheetnames:
        return wb[name]
    return wb.active


def _parse_generacion_datetime_from_sheet(ws) -> datetime | None:
    """
    Busca en las primeras filas texto del Excel tipo:
      'Fecha: DD/MM/YY Hora: HH:MM'
    y devuelve datetime (naive).
    """
    pat_full = re.compile(
        r"Fecha[: \t]+(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})"
        r"(?:\s+Hora:\s*(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?",
        re.IGNORECASE,
    )
    for row in ws.iter_rows(min_row=1, max_row=20, values_only=True):
        for cell in row:
            if cell is None:
                continue
            if isinstance(cell, datetime):
                return cell.replace(tzinfo=None)
            s = str(cell).strip()
            m = pat_full.search(s)
            if m:
                d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if y < 100:
                    y += 2000
                hh = int(m.group(4) or 0)
                mm = int(m.group(5) or 0)
                ss = int(m.group(6) or 0)
                return datetime(y, mo, d, hh, mm, ss)
    return None


def _parse_fecha_hasta_date_from_sheet(ws) -> date | None:
    pat = re.compile(r"Fecha\s+hasta[:\s]+(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", re.IGNORECASE)
    for row in ws.iter_rows(min_row=1, max_row=20, values_only=True):
        for cell in row:
            if cell is None:
                continue
            s = str(cell).strip()
            m = pat.search(s)
            if m:
                d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if y < 100:
                    y += 2000
                return date(y, mo, d)
    return None


def _load_workbook_from_attachment(xlsx_path: Path, cfg: dict):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = _excel_primary_sheet(wb, cfg)
    return wb, ws


def get_excel_generacion_datetime(msg, cfg: dict) -> datetime | None:
    """Lee el Excel adjunto y devuelve la fecha/hora de generación (si aparece)."""
    nombre_cfg = (cfg.get("nombre_adjunto") or "").lower().replace(".xlmx", ".xlsx").strip()
    for part in msg.walk():
        ok, fn_norm = _is_matching_excel_part(part, nombre_cfg)
        if not ok:
            continue
        try:
            import io

            data = part.get_payload(decode=True)
            wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            ws = _excel_primary_sheet(wb, cfg)
            dt = _parse_generacion_datetime_from_sheet(ws)
            wb.close()
            return dt
        except Exception as e:
            log.warning(f"No se pudo leer fecha generación desde adjunto: {e}")
    return None


def _part_filename_normalized(part) -> str:
    """Obtiene nombre de adjunto desde filename/name de forma tolerante."""
    fn_raw = part.get_filename()
    if fn_raw:
        fn = _decode_header_value(fn_raw).strip()
        if fn:
            return fn.lower().replace(".xlmx", ".xlsx")

    # fallback: algunos correos traen "name" en Content-Type
    name_raw = part.get_param("name")
    if name_raw:
        name = _decode_header_value(name_raw).strip()
        if name:
            return name.lower().replace(".xlmx", ".xlsx")

    # fallback: filename en Content-Disposition
    disp_fn = part.get_param("filename", header="content-disposition")
    if disp_fn:
        dfn = _decode_header_value(disp_fn).strip()
        if dfn:
            return dfn.lower().replace(".xlmx", ".xlsx")

    return ""


def _normalize_name_token(s: str) -> str:
    """
    Normaliza nombres para comparar patrones de adjuntos:
    - minúsculas
    - quita extensión xlsx/xlsm/xls
    - elimina todo lo no alfanumérico
    """
    if not s:
        return ""
    s = s.lower().strip().replace(".xlmx", ".xlsx")
    s = re.sub(r"\.(xlsx|xlsm|xls)$", "", s)
    s = re.sub(r"[^a-z0-9áéíóúüñ]+", "", s)
    return s


def _is_excel_mime(part) -> bool:
    ctype = (part.get_content_type() or "").lower()
    if ctype == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return True
    # algunos sistemas lo envían genérico con nombre de archivo
    if ctype in ("application/octet-stream", "application/vnd.ms-excel"):
        return True
    return False


def _is_attachment_like(part) -> bool:
    disp = (part.get("Content-Disposition") or "").lower()
    if "attachment" in disp:
        return True
    # algunos sistemas marcan inline aunque sea archivo
    if "inline" in disp and (part.get_filename() or part.get_param("name")):
        return True
    return False


def _is_matching_excel_part(part, nombre_cfg: str) -> tuple[bool, str]:
    """Devuelve (es_adj_excel, filename_normalized)."""
    fn_norm = _part_filename_normalized(part)
    has_xlsx_name = fn_norm.endswith(".xlsx")
    is_excel = has_xlsx_name or _is_excel_mime(part)
    if not is_excel:
        # fallback permisivo para correos MIME "raros":
        # si no hay filtro de nombre, aceptar adjunto binario e intentar parseo posterior.
        if not nombre_cfg and _is_attachment_like(part):
            payload = part.get_payload(decode=True)
            if payload and len(payload) > 200:
                return True, fn_norm
        return False, fn_norm
    if nombre_cfg and fn_norm:
        cfg_key = _normalize_name_token(nombre_cfg)
        fn_key = _normalize_name_token(fn_norm)
        if cfg_key and fn_key and (cfg_key not in fn_key and fn_key not in cfg_key):
            return False, fn_norm
    return True, fn_norm


def _message_has_matching_xlsx(msg, cfg: dict) -> bool:
    """True si el correo contiene al menos un adjunto .xlsx que cumple el filtro de nombre (si existe)."""
    nombre_cfg = (cfg.get("nombre_adjunto") or "").lower().replace(".xlmx", ".xlsx").strip()
    for part in msg.walk():
        ok, _ = _is_matching_excel_part(part, nombre_cfg)
        if ok:
            return True
    return False


def _message_parts_debug(msg) -> list[str]:
    rows = []
    for i, part in enumerate(msg.walk()):
        ctype = (part.get_content_type() or "").lower()
        fn = _part_filename_normalized(part)
        disp = (part.get("Content-Disposition") or "").strip()
        rows.append(f"part#{i} ctype={ctype} fn='{fn}' disp='{disp}'")
    return rows


def _email_header_datetime(msg) -> datetime | None:
    """Fallback de fecha/hora del correo (cabecera Date) en UTC naive."""
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def get_excel_attachment_path(conn, uid, cfg, tmp_dir: str) -> Path | None:
    _, msg_data = conn.fetch(uid, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])

    nombre_cfg = (cfg.get("nombre_adjunto") or "").lower().replace(".xlmx", ".xlsx").strip()
    for part in msg.walk():
        ok, fn_norm = _is_matching_excel_part(part, nombre_cfg)
        if not ok:
            continue
        if not fn_norm:
            fn_norm = f"informe_uid_{uid.decode()}.xlsx"
        out = Path(tmp_dir) / fn_norm
        out.write_bytes(part.get_payload(decode=True))
        return out
    return None


def find_latest_email_by_generation(conn, cfg: dict) -> bytes | None:
    """
    Selecciona el email cuyo Excel tenga fecha/hora de generación más reciente.
    Si empate exacto: gana UID IMAP mayor.
    """
    carpeta = cfg.get("carpeta_busqueda", "INBOX")
    conn.select(carpeta, readonly=True)

    criterios = []
    if cfg.get("asunto_contiene"):
        criterios.append(f'SUBJECT "{cfg["asunto_contiene"]}"')
    if cfg.get("remitente_contiene"):
        criterios.append(f'FROM "{cfg["remitente_contiene"]}"')

    horas = int(cfg.get("buscar_ultimas_horas", 96))
    desde = (datetime.now(timezone.utc) - timedelta(hours=horas + 48)).strftime("%d-%b-%Y")
    criterios.append(f"SINCE {desde}")
    search_str = " ".join(criterios)

    _, data = conn.search(None, search_str)
    ids = data[0].split() if data and data[0] else []
    if not ids:
        return None

    best_uid = None
    best_dt = None
    today_utc = datetime.now(timezone.utc).date()

    for uid in ids:
        _, full = conn.fetch(uid, "(RFC822)")
        msg = email.message_from_bytes(full[0][1])

        if not _message_has_matching_xlsx(msg, cfg):
            log.info(f"UID {uid.decode()}: sin adjunto .xlsx válido, descartado")
            for row in _message_parts_debug(msg):
                log.info(f"UID {uid.decode()}: {row}")
            continue

        gen_dt = get_excel_generacion_datetime(msg, cfg)
        used_fallback = False
        if gen_dt is None:
            gen_dt = _email_header_datetime(msg)
            used_fallback = True
            if gen_dt is None:
                log.info(f"UID {uid.decode()}: sin fecha generación ni Date cabecera legible, descartado")
                continue

        if gen_dt.date() > today_utc:
            log.info(f"UID {uid.decode()}: fecha futura {gen_dt.date()} > {today_utc}, descartado")
            continue

        # tie-break by UID numeric greater
        uidn = int(uid.decode())
        src = "email-date" if used_fallback else "excel-generation"
        log.info(f"UID {uid.decode()}: candidato dt={gen_dt.isoformat()} src={src}")
        if best_dt is None or gen_dt > best_dt or (gen_dt == best_dt and uidn > int(best_uid.decode())):
            best_dt = gen_dt
            best_uid = uid

    if best_uid is not None:
        log.info(f"UID seleccionado: {best_uid.decode()} (dt={best_dt})")
    return best_uid


def commercial_month_from_fecha_hasta(d: date) -> tuple[int, int]:
    """Mes comercial etiquetado por mes civil de fin (día 25) con corte 26..25."""
    if d.day <= 25:
        return d.year, d.month
    # day 26..31 => siguiente mes comercial
    if d.month == 12:
        return d.year + 1, 1
    return d.year, d.month + 1


def commercial_month_from_reference(
    workbook_gen_dt: datetime | None,
    workbook_fecha_hasta: date | None,
) -> tuple[int, int]:
    """
    Mes comercial con prioridad:
    1) fecha/hora de generación (día real disponible)
    2) fallback fecha_hasta
    """
    if workbook_gen_dt is not None:
        return commercial_month_from_fecha_hasta(workbook_gen_dt.date())
    if workbook_fecha_hasta is not None:
        return commercial_month_from_fecha_hasta(workbook_fecha_hasta)
    today = datetime.now(ZoneInfo("Europe/Madrid")).date()
    return commercial_month_from_fecha_hasta(today)


def commercial_month_bounds(cm_year: int, cm_month: int) -> tuple[date, date]:
    """Inicio/fin civil del mes comercial: 26 del mes civil anterior → 25 del mes siguiente."""
    if cm_month == 1:
        start = date(cm_year - 1, 12, 26)
    else:
        start = date(cm_year, cm_month - 1, 26)
    end = date(cm_year, cm_month, 25)
    return start, end


MONTHS_ES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
MONTHS_UP = {m.upper() for m in MONTHS_ES}
CENTER_CANON = ["CENTRAL", "CORONA", "ALMOZARA", "ALCARRAS"]


def compute_days_elapsed_and_total(locality_key: str, cm_start: date, cm_end: date, last_load_date: date) -> dict:
    """
    Laborables: Lunes a Sábado excluyendo:
      - festivos nacionales (Spain workalendar)
      - festivos locales (Zaragoza / Lleida) desde config opcional
    """
    cal = Spain()
    years = list({cm_start.year, cm_end.year, last_load_date.year})
    national_h = set()
    for y in years:
        for d, _ in cal.holidays(y):
            national_h.add(d)

    # Festivos LOCALES de la ciudad (requisito 8)
    # Zaragoza (ciudad): San Valero (29/01) y Cincomarzada (05/03)
    # Lleida (ciudad): San Anastasio (12/05) y Sant Miquel (29/09)
    # Nota: aquí aplicamos fechas locales para los años 2025-2026 requeridos.
    # Si más adelante necesitáis otros años, extendemos el mapa.
    local_h = set()
    if locality_key.lower() == "zaragoza":
        for y in years:
            local_h.add(date(y, 1, 29))
            local_h.add(date(y, 3, 5))
    elif locality_key.lower() == "lleida":
        for y in years:
            local_h.add(date(y, 5, 12))
            local_h.add(date(y, 9, 29))

    def is_workday(d: date) -> bool:
        if d.weekday() > 5:  # 0=Mon .. 6=Sun
            return False
        if d in national_h:
            return False
        if d in local_h:
            return False
        return True

    total_days = 0
    d = cm_start
    while d <= cm_end:
        if is_workday(d):
            total_days += 1
        d = d + timedelta(days=1)

    elapsed_days = 0
    ref = min(last_load_date, cm_end)
    if ref < cm_start:
        elapsed_days = 0
    else:
        d = cm_start
        while d <= ref:
            if is_workday(d):
                elapsed_days += 1
            d = d + timedelta(days=1)

    return {"daysElapsed": elapsed_days, "daysTotal": total_days}


def parse_demo_aggregated_from_evolucion(wb, cm_year: int, cm_month: int, last_load_date: date) -> dict:
    """
    Parser heurístico para el formato de ejemplo Evolucion Zaragoza.xlsx:
    - TOTAL{suffix} para Europa total
    - {ZARAGOZA|LERIDA|ALMOZARA|CORONA}{suffix} para centros
    - y (opcional) HISTORICO ventas para historyAnnual
    """
    suffix = str(cm_year)[-2:]
    cm_month_idx = cm_month - 1
    month_name = MONTHS_ES[cm_month_idx]
    # En algunos Excel el bloque mensual viene etiquetado por mes civil "de origen"
    # (p.ej., datos 26-mar..25-abr bajo fila MARZO). Fallback al mes anterior.
    month_name_prev = MONTHS_ES[(cm_month_idx - 1) % 12]

    # days for projection
    cm_start, cm_end = commercial_month_bounds(cm_year, cm_month)

    # locality mapping for days
    loc_tot = "zaragoza"

    days = compute_days_elapsed_and_total(loc_tot, cm_start, cm_end, last_load_date)

    def find_month_row(ws, labels: list[str]) -> int | None:
        labels_up = {str(x).strip().upper() for x in labels if x}
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=120, values_only=True), start=1):
            # first 2 columns contain month label in this example
            for j in [0,1]:
                v = row[j] if j < len(row) else None
                if v is None:
                    continue
                if str(v).strip().upper() in labels_up:
                    return i
        return None

    def get_vals_from_row(ws, row_idx: int, offsets: tuple[int,int,int]):
        row = list(ws.iter_rows(min_row=row_idx, max_row=row_idx, values_only=True))[0]
        # offsets are 0-based indices
        b = row[offsets[0]] if len(row) > offsets[0] else None
        cur = row[offsets[1]] if len(row) > offsets[1] else None
        prev = row[offsets[2]] if len(row) > offsets[2] else None
        def as_float(x):
            if x is None:
                return None
            if isinstance(x,(int,float)):
                return float(x)
            try:
                return float(str(x).replace('.','').replace(',','.'))
            except:
                return None
        return as_float(b), as_float(cur), as_float(prev)

    def proj(real, elapsed, total):
        if real is None or elapsed is None or elapsed <= 0 or total is None:
            return None
        return (real / elapsed) * total

    def kpis(real, budget, elapsed, total, yoy_prev):
        p = proj(real, elapsed, total)
        pct = (p / budget * 100.0) if (p is not None and budget and budget != 0) else None
        pace = (real / elapsed) if (real is not None and elapsed and elapsed > 0) else None
        yoy_real_pct = ((real - yoy_prev) / yoy_prev * 100.0) if (yoy_prev is not None and yoy_prev != 0 and real is not None) else None
        # yoy proy: build proy for prev with same elapsed/total
        proy_prev = proj(yoy_prev, elapsed, total) if (yoy_prev is not None) else None
        yoy_proy_pct = ((p - proy_prev) / proy_prev * 100.0) if (proy_prev and proy_prev != 0 and p is not None) else None
        var_real = (real - budget) if (real is not None and budget is not None) else None
        var_proy = (p - budget) if (p is not None and budget is not None) else None
        return {
            "real": real,
            "budget": budget,
            "projection": p,
            "pctProyBudget": pct,
            "pace": pace,
            "yoyRealPct": yoy_real_pct,
            "yoyProyPct": yoy_proy_pct,
            "varRealVsBudget": var_real,
            "varProyVsBudget": var_proy,
            "daysElapsed": elapsed,
            "daysTotal": total,
        }

    cm_key = f"{cm_year}-{cm_month:02d}"

    # TOTAL Europe
    total_sheet = f"TOTAL{suffix}"
    totals = {}
    if total_sheet in wb.sheetnames:
        ws = wb[total_sheet]
        # Serie mensual del año comercial (enero..diciembre) para selector y gráficos.
        for m in range(1, 13):
            m_name = MONTHS_ES[m - 1]
            row_idx_m = find_month_row(ws, [m_name])
            if not row_idx_m:
                continue
            budget, real_cur, real_prev = get_vals_from_row(ws, row_idx_m, offsets=(2,3,4))
            mk = f"{cm_year}-{m:02d}"
            dstart, dend = commercial_month_bounds(cm_year, m)
            mdays = compute_days_elapsed_and_total("zaragoza", dstart, dend, last_load_date)
            totals[mk] = kpis(real_cur, budget, mdays["daysElapsed"], mdays["daysTotal"], real_prev)

        # fallback de seguridad para CM actual si no entró por serie
        if cm_key not in totals:
            row_idx = find_month_row(ws, [month_name, month_name_prev])
            if row_idx:
                budget, real_cur, real_prev = get_vals_from_row(ws, row_idx, offsets=(2,3,4))
                totals[cm_key] = kpis(real_cur, budget, days["daysElapsed"], days["daysTotal"], real_prev)
    # Centers
    centers = {}
    center_sheets = [s for s in wb.sheetnames if s.endswith(suffix) and any(k in s.upper() for k in ["ZARAGOZA","LERIDA","ALMOZARA","CORONA","CENTRAL"])]
    for sh in center_sheets:
        ws = wb[sh]
        name_up = sh.upper()
        if name_up.startswith("ZARAGOZA") or name_up.startswith("CENTRAL"):
            center_id = "CENTRAL"
            offsets = (2,3,4)
            locality = "zaragoza"
        elif name_up.startswith("LERIDA"):
            center_id = "ALCARRAS"
            offsets = (3,4,5)
            locality = "lleida"
        elif name_up.startswith("ALMOZARA"):
            center_id = "ALMOZARA"
            offsets = (2,3,4)
            locality = "zaragoza"
        elif name_up.startswith("CORONA"):
            center_id = "CORONA"
            offsets = (2,3,4)
            locality = "zaragoza"
        else:
            center_id = sh
            offsets = (2,3,4)
            locality = "zaragoza"

        for m in range(1, 13):
            m_name = MONTHS_ES[m - 1]
            row_idx = find_month_row(ws, [m_name])
            if not row_idx:
                continue
            dstart, dend = commercial_month_bounds(cm_year, m)
            local_days = compute_days_elapsed_and_total(locality, dstart, dend, last_load_date)
            budget, real_cur, real_prev = get_vals_from_row(ws, row_idx, offsets=offsets)
            mk = f"{cm_year}-{m:02d}"
            centers.setdefault(mk, {})[center_id] = kpis(real_cur, budget, local_days["daysElapsed"], local_days["daysTotal"], real_prev)

    # Rellena centros canónicos faltantes para que siempre aparezcan en frontend.
    for mk in list(totals.keys()):
        dstart, dend = commercial_month_bounds(int(mk.split("-")[0]), int(mk.split("-")[1]))
        mdays = compute_days_elapsed_and_total("zaragoza", dstart, dend, last_load_date)
        for cid in CENTER_CANON:
            centers.setdefault(mk, {}).setdefault(cid, {
                "real": None,
                "budget": None,
                "projection": None,
                "pctProyBudget": None,
                "pace": None,
                "yoyRealPct": None,
                "yoyProyPct": None,
                "varRealVsBudget": None,
                "varProyVsBudget": None,
                "daysElapsed": mdays["daysElapsed"],
                "daysTotal": mdays["daysTotal"],
            })

    # Vendors (heurístico en hoja VENTAS): col B = nombre, col D = ventas.
    vendorsByMonth = {}
    ws_v = _excel_primary_sheet(wb, {"hoja_excel": "VENTAS"})
    current_center = None
    vendor_acc = {}

    def _to_num(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(str(v).replace(".", "").replace(",", ".").strip())
        except Exception:
            return None

    def _norm_txt(v):
        return re.sub(r"\s+", " ", str(v or "").strip()).upper()

    def _detect_center(name_up: str) -> str | None:
        if "ALCARR" in name_up or "LERIDA" in name_up:
            return "ALCARRAS"
        if "ALMOZARA" in name_up:
            return "ALMOZARA"
        if "CORONA" in name_up:
            return "CORONA"
        if "CENTRAL" in name_up or "ZARAGOZA" in name_up:
            return "CENTRAL"
        return None

    for r in range(1, (ws_v.max_row or 0) + 1):
        raw_name = ws_v.cell(row=r, column=2).value
        name = str(raw_name).strip() if raw_name is not None else ""
        if not name:
            continue
        name_up = _norm_txt(name)
        maybe_center = _detect_center(name_up)
        if maybe_center:
            current_center = maybe_center
            continue

        sales = _to_num(ws_v.cell(row=r, column=4).value)
        if sales is None or sales <= 0:
            continue
        if current_center is None:
            continue
        if name_up in MONTHS_UP:
            continue

        # Compartidos: mostrar solo si tienen cifra (ya garantizado arriba).
        key = (current_center, name.strip())
        vendor_acc[key] = vendor_acc.get(key, 0.0) + float(sales)

    vendorsByMonth[cm_key] = {}
    for (center_id, vendor_name), real in vendor_acc.items():
        cbase = centers.get(cm_key, {}).get(center_id, {})
        de = cbase.get("daysElapsed") or days["daysElapsed"]
        dt = cbase.get("daysTotal") or days["daysTotal"]
        projection = (real / de * dt) if de else None
        vendor_id = f"{center_id}::{vendor_name}"
        vendorsByMonth[cm_key][vendor_id] = {
            "centerId": center_id,
            "vendorName": vendor_name,
            "real": real,
            "projection": projection,
            "yoyRealPct": None,
            "budget": None,
        }

    # History annual (optional) — desde 2021 en adelante.
    historyAnnual = {}
    historyByCenterAnnual = {}
    if "Historico ventas" in wb.sheetnames:
        hs = wb["Historico ventas"]

        def _safe_float(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            try:
                return float(str(v).strip().replace(",", "."))
            except Exception:
                return None

        # Parse blocks (ZARAGOZA, ALCARRAS, ALMOZARA, TOTAL, CORONA...)
        # Pattern expected:
        # row R: center name + year headers in cols B.. (2008..2026)
        # rows R+1..R+12: ENERO..DICIEMBRE values by year
        candidate_centers = {"ZARAGOZA", "ALCARRAS", "ALCARRÁS", "ALMOZARA", "TOTAL", "CORONA"}
        for r in range(1, hs.max_row + 1):
            c0 = hs.cell(row=r, column=1).value
            if c0 is None:
                continue
            center_raw = str(c0).strip().upper()
            if center_raw not in candidate_centers:
                continue

            # Validate that next row looks like a month row
            month_probe = hs.cell(row=r + 1, column=1).value
            if month_probe is None or str(month_probe).strip().upper() not in MONTHS_UP:
                continue

            center_key = center_raw.replace("Á", "A")
            if center_key == "ALCARRAS":
                center_key = "ALCARRAS"

            # Year columns on this center header row
            year_cols = []
            for c in range(2, hs.max_column + 1):
                yv = _safe_float(hs.cell(row=r, column=c).value)
                if yv is None:
                    continue
                yi = int(yv)
                if 1900 <= yi <= 2100:
                    year_cols.append((yi, c))

            if not year_cols:
                continue

            # Sum ENERO..DICIEMBRE (12 rows)
            annual_map = {}
            for yi, col in year_cols:
                s = 0.0
                for rr in range(r + 1, r + 13):
                    mv = hs.cell(row=rr, column=1).value
                    if mv is None:
                        continue
                    if str(mv).strip().upper() not in MONTHS_UP:
                        continue
                    vv = _safe_float(hs.cell(row=rr, column=col).value)
                    if vv is None:
                        continue
                    s += vv
                # En esta hoja está en miles de euros => convertir a euros
                annual_map[yi] = s * 1000.0

            # Keep only 2021+ as requested
            annual_map = {y: v for y, v in annual_map.items() if y >= 2021}
            if not annual_map:
                continue

            # Build with YoY
            years_sorted = sorted(annual_map.keys())
            out = {}
            prev_val = None
            for yi in years_sorted:
                val = annual_map[yi]
                yoy = None
                if prev_val is not None and prev_val != 0:
                    yoy = (val - prev_val) / prev_val * 100.0
                out[str(yi)] = {"total": val, "yoyPct": yoy}
                prev_val = val

            historyByCenterAnnual[center_key] = out

        # Locate TOTAL row index where column A equals 'TOTAL'
        total_row_idx = None
        for i in range(1, hs.max_row + 1):
            v = hs.cell(row=i, column=1).value
            if v is None:
                continue
            if str(v).strip().upper() == "TOTAL":
                total_row_idx = i
                break

        if total_row_idx:
            # Columns B.. contain year labels (2008..)
            years = []
            year_cols = []
            for c in range(2, hs.max_column + 1):
                y = hs.cell(row=total_row_idx, column=c).value
                if isinstance(y, (int, float)):
                    # keep integer year only
                    yi = int(y)
                    if 1900 <= yi <= 2100:
                        years.append(yi)
                        year_cols.append(c)

            # Month rows are expected to start at TOTAL+1 (ENERO) and continue 12 rows (hasta DICIEMBRE)
            # This matches the structure of the example workbook.
            month_rows = [total_row_idx + i for i in range(1, 13)]
            annual = []
            for yi, col in zip(years, year_cols):
                s = 0.0
                for mr in month_rows:
                    v = hs.cell(row=mr, column=col).value
                    if v is None:
                        continue
                    if isinstance(v, (int, float)):
                        s += float(v)
                    else:
                        try:
                            s += float(str(v).strip())
                        except:
                            pass
                # In this workbook the unit is "miles" → convert to euros
                annual.append((yi, s * 1000.0))

            annual_sorted = sorted(annual, key=lambda x: x[0])
            for i, (yi, total_eur) in enumerate(annual_sorted):
                if yi < 2021:
                    continue
                if i == 0:
                    yoy = None
                else:
                    prev = annual_sorted[i - 1][1]
                    yoy = ((total_eur - prev) / prev * 100.0) if prev else None
                historyAnnual[str(yi)] = {"total": total_eur, "yoyPct": yoy}

    month_labels = {}
    for mk in totals.keys():
        # mk format YYYY-MM
        try:
            _y = int(mk.split("-")[0])
            _m = int(mk.split("-")[1])
            month_labels[mk] = f"{MONTHS_ES[_m-1].upper()[:3]}-{str(_y)[-2:]}"
        except Exception:
            month_labels[mk] = mk

    return {
        "meta": {"lastLoadDate": last_load_date.isoformat(), "lastRunTs": datetime.now(timezone.utc).isoformat()},
        "ui": {"monthLabels": month_labels},
        "totalsByMonth": totals,
        "centersByMonth": centers,
        "vendorsByMonth": vendorsByMonth,
        "historyAnnual": historyAnnual,
        "historyByCenterAnnual": historyByCenterAnnual,
      }


def find_generation_datetime(workbook_path: Path, cfg: dict) -> datetime | None:
    wb = openpyxl.load_workbook(workbook_path, read_only=True, data_only=True)
    ws = _excel_primary_sheet(wb, cfg)
    dt = _parse_generacion_datetime_from_sheet(ws)
    wb.close()
    return dt


def find_fecha_hasta(workbook_path: Path, cfg: dict) -> date | None:
    wb = openpyxl.load_workbook(workbook_path, read_only=True, data_only=True)
    ws = _excel_primary_sheet(wb, cfg)
    d = _parse_fecha_hasta_date_from_sheet(ws)
    wb.close()
    return d


def compute_last_load_date_local(workbook_gen_dt: datetime | None, workbook_fecha_hasta: date | None, cm_year: int, cm_month: int) -> date:
    """
    lastLoadDate (día completo disponible) = fecha de generación si existe; fallback fecha_hasta.
    Se capea a [inicio mes comercial, min(hoy, fin mes comercial)].
    """
    today = datetime.now(ZoneInfo("Europe/Madrid")).date()
    cm_start, cm_end = commercial_month_bounds(cm_year, cm_month)
    chosen = None
    if workbook_gen_dt:
        chosen = workbook_gen_dt.date()
    elif workbook_fecha_hasta:
        chosen = workbook_fecha_hasta
    else:
        chosen = today

    if chosen > today:
        chosen = today
    if chosen < cm_start:
        chosen = cm_start
    if chosen > cm_end:
        chosen = cm_end
    return chosen


def update_json_payload(payload: dict):
    existing = {}
    if JSON_OUT.exists():
        try:
            existing = json.loads(JSON_OUT.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    # shallow merge: keep existing for months not updated
    existing.setdefault("totalsByMonth", {})
    existing.setdefault("centersByMonth", {})
    existing.setdefault("vendorsByMonth", {})
    existing.setdefault("historyAnnual", {})
    existing.setdefault("historyByCenterAnnual", {})
    existing.setdefault("ui", {})
    existing["ui"].setdefault("monthLabels", {})
    existing.setdefault("meta", {})

    existing["meta"].update(payload.get("meta", {}))
    for mk, t in (payload.get("totalsByMonth") or {}).items():
        existing["totalsByMonth"][mk] = t
    for mk, cdict in (payload.get("centersByMonth") or {}).items():
        existing["centersByMonth"].setdefault(mk, {})
        existing["centersByMonth"][mk].update(cdict)
    for mk, vdict in (payload.get("vendorsByMonth") or {}).items():
        existing["vendorsByMonth"].setdefault(mk, {})
        existing["vendorsByMonth"][mk].update(vdict)

    # monthLabels: fuente de verdad = totalsByMonth (evita labels huérfanos/desalineados)
    rebuilt_labels = {}
    for mk in sorted(existing["totalsByMonth"].keys()):
        try:
            _y = int(mk.split("-")[0])
            _m = int(mk.split("-")[1])
            rebuilt_labels[mk] = f"{MONTHS_ES[_m-1].upper()[:3]}-{str(_y)[-2:]}"
        except Exception:
            rebuilt_labels[mk] = mk
    existing["ui"]["monthLabels"] = rebuilt_labels
    # Histórico: reemplazo completo para evitar mezclar datos "demo" antiguos.
    if "historyAnnual" in payload:
        existing["historyAnnual"] = payload.get("historyAnnual") or {}
    if "historyByCenterAnnual" in payload:
        existing["historyByCenterAnnual"] = payload.get("historyByCenterAnnual") or {}

    JSON_OUT.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")


def drive_append_workbook_to_tab(cfg: dict, xlsx_path: Path, report_key: str, fecha_gen: datetime | None, fecha_hasta: date | None):
    if not cfg.get("google_service_account_json"):
        return
    if not cfg.get("drive_spreadsheet_id"):
        log.warning("Falta DRIVE_SPREADSHEET_ID, se salta copia a Drive.")
        return

    # auth
    try:
        sa_info = json.loads(cfg["google_service_account_json"])
        creds = google.oauth2.service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
    except Exception as e:
        log.error(f"No se pudo cargar GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
        return

    service = build("sheets", "v4", credentials=creds)
    ss_id = cfg["drive_spreadsheet_id"]
    tab = cfg["drive_tab"]

    # Idempotence: check if report_key exists in column A
    try:
        read = service.spreadsheets().values().get(
            spreadsheetId=ss_id,
            range=f"{tab}!A:A",
            majorDimension="COLUMNS",
        ).execute()
        existing_colA = read.get("values", [[]])[0]
        if report_key in existing_colA:
            log.info("Drive: report_key ya existe, no se duplica.")
            return
    except HttpError as e:
        log.warning(f"Drive: no se pudo leer idempotencia (se intentará append). {e}")

    # Extract sheet values (primary sheet)
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = _excel_primary_sheet(wb, cfg)

    # Convert to list of rows up to used range
    max_r = ws.max_row or 0
    max_c = ws.max_column or 0
    values = []
    for r in range(1, max_r + 1):
        row = []
        for c in range(1, max_c + 1):
            row.append(ws.cell(row=r, column=c).value)
        values.append(row)
        if r > 5000:
            break
    wb.close()

    # Prepend a block header
    header = [
        report_key,
        fecha_gen.isoformat() if fecha_gen else "",
        fecha_hasta.isoformat() if fecha_hasta else "",
    ] + [""] * max_c
    payload = [header]
    for row in values:
        payload.append([""] + row)  # shift 1 col to keep report_key column aligned

    # Append
    try:
        service.spreadsheets().values().append(
            spreadsheetId=ss_id,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": payload},
        ).execute()
        log.info("Drive: append OK.")
    except Exception as e:
        log.error(f"Drive: append falló: {e}")


def main():
    log.info("=" * 70)
    log.info("Europa extractor — start")
    cfg = load_config()

    # Modo local de pruebas (sin IMAP): usar un Excel local directo.
    local_xlsx = os.environ.get("LOCAL_XLSX_PATH", "").strip()
    if local_xlsx:
        xlsx = Path(local_xlsx)
        if not xlsx.exists():
            log.error(f"LOCAL_XLSX_PATH no existe: {xlsx}")
            return

        fecha_hasta = find_fecha_hasta(xlsx, cfg)
        gen_dt = find_generation_datetime(xlsx, cfg)
        if fecha_hasta is None:
            log.error("No se detectó 'Fecha hasta' en Excel local.")
            return

        cm_year, cm_month = commercial_month_from_reference(gen_dt, fecha_hasta)
        last_load_date = compute_last_load_date_local(gen_dt, fecha_hasta, cm_year, cm_month)

        # Drive append opcional también en modo local (idempotente)
        key = f"{(gen_dt.isoformat() if gen_dt else 'unknown-gen')}::LOCAL_XLSX::{xlsx.name}"
        try:
            drive_append_workbook_to_tab(cfg, xlsx, key, gen_dt, fecha_hasta)
        except Exception as e:
            log.warning(f"Drive append (local mode) se saltó por error: {e}")

        wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
        payload = parse_demo_aggregated_from_evolucion(wb, cm_year, cm_month, last_load_date)
        try:
            wb.close()
        except Exception:
            pass
        update_json_payload(payload)
        log.info("Europa extractor — end (local mode)")
        return

    conn = connect_gmail(cfg)
    uid = find_latest_email_by_generation(conn, cfg)
    if uid is None:
        log.warning("No se encontró email válido.")
        conn.logout()
        return

    # Need message object for metadata key
    _, full = conn.fetch(uid, "(RFC822)")
    msg = email.message_from_bytes(full[0][1])

    gen_dt = get_excel_generacion_datetime(msg, cfg)
    fecha_hasta = None
    # fecha_hasta from the same attachment
    with tempfile.TemporaryDirectory() as tmp:
        xlsx = get_excel_attachment_path(conn, uid, cfg, tmp)
        conn.logout()
        if xlsx is None:
            log.error("Adjunto no encontrado.")
            return

        fecha_hasta = find_fecha_hasta(xlsx, cfg)
        # Mes comercial: prioridad fecha de generación, fallback fecha_hasta.
        if fecha_hasta is None and gen_dt is None:
            log.error("No se detectó ni 'Fecha generación' ni 'Fecha hasta' en Excel.")
            return
        cm_year, cm_month = commercial_month_from_reference(gen_dt, fecha_hasta)
        last_load_date = compute_last_load_date_local(gen_dt, fecha_hasta, cm_year, cm_month)

        # report key for idempotence
        gen_key = gen_dt.isoformat() if gen_dt else "unknown-gen"
        subj = _decode_header_value(msg.get("Subject"))
        key = f"{gen_key}::{subj}".replace("\n"," ").strip()

        # Drive copy (optional)
        try:
            drive_append_workbook_to_tab(cfg, xlsx, key, gen_dt, fecha_hasta)
        except Exception as e:
            log.warning(f"Drive append: se saltó por error: {e}")

        # Build data payload (demo aggregated parser)
        wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
        payload = parse_demo_aggregated_from_evolucion(wb, cm_year, cm_month, last_load_date)
        try:
            wb.close()
        except Exception:
            pass

        update_json_payload(payload)

    log.info("Europa extractor — end")


if __name__ == "__main__":
    main()

