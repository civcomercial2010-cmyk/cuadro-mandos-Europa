# Cuadro de mando · Hipopótamo Europa S.L.

GitHub Pages (HTML/JS estático) + `data.json` generado por extractor Python (IMAP → Excel → Google Drive → `data.json`), y actualización automática por GitHub Actions.

## Flujo
1. Cada noche llega un email con adjunto Excel del ERP.
2. `extractor_europa_actions.py` descarga el adjunto vía IMAP y selecciona el **Excel más reciente por fecha/hora de generación** (si empate, gana el UID IMAP mayor).
3. El Excel se vuelca (append) a la pestaña `Datos` de un Google Sheets/Drive (idempotente por `fecha_generacion + nombre_adjunto`).
4. El extractor agrega métricas por **centro** y genera `data.json` para el frontend.
5. El workflow hace commit/push si `data.json` cambió.

## Secrets GitHub (necesarios)
Gmail/IMAP:
- `GMAIL_USER`
- `GMAIL_PASSWORD`
- `ASUNTO_FILTRO` (opcional; regex/patrón)
- `REMITENTE` (opcional; patrón exacto o substring)

Google Drive/Sheets (Service Account):
- `GOOGLE_SERVICE_ACCOUNT_JSON` (recomendado, JSON completo en secret)

Opcionalmente:
- `DRIVE_SPREADSHEET_ID` (por defecto se espera el ID de la URL del sheet)

## Festivos locales
El cálculo de `días totales` y `días transcurridos` requiere festivos nacionales y locales:
- Zaragoza para todos los centros excepto `Alcarrás` (que usa calendario de Lleida).

Para producción real, rellena `festivos_locales.json` (ver archivo en el repo cuando lo añadamos) o completa la configuración equivalente.

## Nota importante (config ERP + mapeo centro/vendedor)
El extractor necesita:
- Nombre de la hoja a leer en el Excel ERP (por defecto `VENTAS`).
- Cómo se identifican `centro` y `vendedor` en el Excel ERP.
- (Obligatorio) sheet histórico centro↔vendedor con fechas de vigencia.

En cuanto me confirmes:
- ID y pestaña del sheet histórico,
- nombres exactos de columnas y 5 filas de ejemplo,
- y el nombre real de la hoja en el Excel ERP,
ajustamos el extractor para cumplir el 100% de los requisitos.
