"""Importa desglose vendedor 2026 (datos reales aportados por el usuario)."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data.json"


def month_vendors(pairs):
    """pairs: [(name, value), ...] — omite None; incluye 0."""
    out = []
    for name, val in pairs:
        if val is None:
            continue
        out.append({"name": name, "real": round(float(val), 2)})
    return sorted(out, key=lambda x: -x["real"])


def build_centers(month_idx, central_vals, alcarras_vals, almozara_val, corona_list=None):
    m = month_idx
    return {
        "CENTRAL": month_vendors([
            ("CECILIA MEZA", central_vals[0][m]),
            ("ANTONIO LAHUERTA", central_vals[1][m]),
            ("MARIA JOSE", central_vals[2][m]),
            ("SARA", central_vals[3][m]),
            ("J.J. IBAÑEZ", central_vals[4][m]),
        ]),
        "ALCARRAS": month_vendors([
            ("MARIA JESUS BENSENY", alcarras_vals[0][m]),
            ("GLORIA LOPEZ", alcarras_vals[1][m]),
            ("ENRIC CALVET", alcarras_vals[2][m]),
            ("ALBERT", alcarras_vals[3][m]),
        ]),
        "ALMOZARA": month_vendors([("BLANCA", almozara_val)]),
        "CORONA": corona_list or [],
    }


# Valores en euros (tablas usuario: separador de miles con punto)
CENTRAL = [
    [34359, 40462, 28611, 15686, 22981, 0],       # Cecilia
    [42936, 41713, 38383, 12269, 32059, 0],       # Toño
    [0, 32115, 25840, 16924, 0, None],            # Maria Jose
    [None, None, None, 8074, 14898, 6966],        # Sara
    [1754, 7768, 3196, 17686, 0, 0],              # Juan José
]
ALCARRAS = [
    [49995, 50993, 32107, 44920, 49316, 9866],
    [38087, 54996, 30784, 35430, 25776, 46101],
    [35265, 53804, 29726, 26740, 43788, 12380],
    [70719, 62414, 48600, 47039, 50106, 0],
]
# Almozara: una sola persona — ventas reales del histórico del cuadro
ALMOZARA_BLANCA = [23491, 31791, 20950, 15560, 33114, 9910]


def main():
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    existing = data.get("vendorsByMonth") or {}
    corona_by_month = {}
    for key, block in existing.items():
        if block.get("CORONA"):
            corona_by_month[key] = block["CORONA"]

    vbm = {}
    for month in range(1, 7):
        key = f"2026-{month:02d}"
        corona = corona_by_month.get(key, [])
        vbm[key] = build_centers(
            month - 1, CENTRAL, ALCARRAS, ALMOZARA_BLANCA[month - 1], corona
        )

    data["vendorsByMonth"] = vbm
    # Mes en curso: reflejar junio del cuadro en vendors
    data["vendors"] = vbm.get("2026-06", data.get("vendors", {}))

    DATA_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("OK vendorsByMonth 2026-01 .. 2026-06")
    for key in sorted(vbm.keys()):
        t = sum(
            sum(v["real"] for v in vbm[key].get(c, []))
            for c in ["CENTRAL", "ALCARRAS", "ALMOZARA", "CORONA"]
        )
        print(f"  {key} total vendedores: {t:,.2f}")


if __name__ == "__main__":
    main()
