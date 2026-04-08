import re
import decimal
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

_RUN_TIMESTAMP = datetime.now().strftime("%d-%m-%Y_%H.%M")

INPUT_FILES = {
    "1_Informasi_Badan_Usaha": "1_Informasi_Badan_Usaha.csv",
    "2_Susunan_Direksi":       "2_Susunan_Direksi.csv",
    "3_Pemegang_Saham":        "3_Pemegang_Saham.csv",
    "4_Daftar_Perizinan":      "4_Daftar_Perizinan.csv",
}

OUTPUT_BASE = Path("cleaned")
OUTPUT_DIR  = OUTPUT_BASE / _RUN_TIMESTAMP
OUTPUT_BASE.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
TYPO_LOG    = OUTPUT_DIR / "typo_flags.txt"

COLUMN_RENAME = {
    "Nama Perusahaan Asal": "nama_perusahaan_asal",
    "Nama Badan Usaha":     "nama_badan_usaha",
    "Kode Badan Usaha":     "kode_badan_usaha",
    "Jenis Badan Usaha":    "jenis_badan_usaha",
    "Kelurahan":            "kelurahan",
    "NPWP":                 "npwp",
    "RT/RW":                "rt_rw",
    "Kode Pos":             "kode_pos",
    "Alamat":               "alamat",
    "no.":                  "no",
    "nama direksi":         "nama_direksi",
    "mulai menjabat":       "mulai_menjabat",
    "akhir menjabat":       "akhir_menjabat",
    "jabatan":              "jabatan",
    "jenis kepemilikan":    "jenis_kepemilikan",
    "nama":                 "nama",
    "kewarganegaraan":      "kewarganegaraan",
    "asal negara":          "asal_negara",
    "persentase saham":     "persentase_saham",
    "no":                   "no",
    "nomor izin":           "nomor_izin",
    "jenis izin":           "jenis_izin",
    "tahap kegiatan":       "tahap_kegiatan",
    "golongan":             "golongan",
    "komoditas":            "komoditas",
    "luas (ha)":            "luas_ha",
    "tanggal berlaku":      "tanggal_berlaku",
    "tanggal berakhir":     "tanggal_berakhir",
    "status cnc":           "status_cnc",
    "lokasi":               "lokasi",
    "kode wiup":            "kode_wiup",
    "modi id":              "modi_id",
}

APOSTROPHE_COLS = {
    "1_Informasi_Badan_Usaha": ["npwp", "rt_rw", "kode_badan_usaha"],
    "4_Daftar_Perizinan":      ["nomor_izin", "jenis_izin", "kode_wiup", "modi_id"],
}

DATE_COLS = {
    "2_Susunan_Direksi":  ["mulai_menjabat", "akhir_menjabat"],
    "4_Daftar_Perizinan": ["tanggal_berlaku", "tanggal_berakhir"],
}

COUNTRY_MAP = {
    "indonesia": "Indonesia", "id": "Indonesia",
    "china": "China", "tiongkok, republik rakyat": "China",
    "singapore": "Singapore", "singapura": "Singapore",
    "hong kong": "Hong Kong", "hongkong": "Hong Kong",
    "india": "India", "malaysia": "Malaysia", "australia": "Australia",
    "korea, republic of": "South Korea", "korea, republik": "South Korea",
    "korea, democratic people\u2019s republic of": "North Korea",
    "united kingdom": "United Kingdom", "inggris": "United Kingdom",
    "united states": "United States", "usa": "United States",
    "united arab emirates": "United Arab Emirates", "uni emirat arab": "United Arab Emirates",
    "netherlands": "Netherlands", "belanda": "Netherlands",
    "japan": "Japan", "jepang": "Japan",
    "thailand": "Thailand", "ind/thailand": None, "ind / thailand": None,
    "virgin islands (british)": "Virgin Islands (British)",
    "cayman islands": "Cayman Islands", "cayman island": "Cayman Islands",
    "canada": "Canada", "philippines": "Philippines", "switzerland": "Switzerland",
    "bangladesh": "Bangladesh", "pakistan": "Pakistan", "france": "France",
    "germany": "Germany", "taiwan": "Taiwan", "turkey": "Turkey",
    "portugal": "Portugal", "ukraine": "Ukraine", "lithuania": "Lithuania",
    "poland": "Poland", "czech republic": "Czech Republic",
    "marshall islands": "Marshall Islands", "mauritius": "Mauritius",
    "samoa": "Samoa", "seychelles": "Seychelles",
    "syrian arab republic": "Syria", "honduras": "Honduras",
    "viet nam": "Vietnam", "jamaika": "Jamaica",
    "spanyol": "Spain", "afghanistan": "Afghanistan",
    "persero": None,
}

BULAN_ID = {
    "januari":"01","februari":"02","maret":"03","april":"04",
    "mei":"05","juni":"06","juli":"07","agustus":"08",
    "september":"09","oktober":"10","november":"11","desember":"12",
}
MONTH_EN_ABBR = {
    "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
    "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
}

PSEUDO_NULL_VALUES = {"tidak ada data","-","- / -","n/a","na","null","none",""}

_typo_log: list = []


def _flag(file_key, col, csv_row, val, desc):
    _typo_log.append(
        f"File: {file_key}.csv | Kolom: {col} | "
        f"Baris CSV: {csv_row} | Nilai: {repr(val)} | Masalah: {desc}"
    )


def write_typo_log():
    with open(TYPO_LOG, "w", encoding="utf-8") as f:
        f.write(
            f"TYPO FLAG LOG — Dihasilkan: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'='*80}\n"
            f"Catatan: nomor baris mengacu pada CSV asli "
            f"(baris 1 = header, baris 2 = data pertama)\n"
            f"{'='*80}\n\n"
        )
        if _typo_log:
            f.write("\n".join(_typo_log) + "\n")
        else:
            f.write("Tidak ada typo yang terdeteksi.\n")
    print(f"  [LOG] Log dibuat: {TYPO_LOG}  ({len(_typo_log)} entri)")

def normalize_whitespace(val):
    if not isinstance(val, str):
        return val
    val = val.replace("\r\n"," ").replace("\r"," ").replace("\n"," ")
    val = re.sub(r"[ \t]+"," ",val)
    return val.strip()

def is_pseudo_null(val):
    if val is None or (isinstance(val,float) and np.isnan(val)):
        return True
    return str(val).strip().lower() in PSEUDO_NULL_VALUES

def to_null_if_pseudo(val):
    if is_pseudo_null(val):
        return None
    return val.strip() if isinstance(val, str) else val

def strip_leading_apostrophe(val):
    if isinstance(val, str):
        return val.lstrip("'")
    return val


def pad_rt_rw(val):
    if not isinstance(val, str):
        return val
    m = re.match(r'^(.+?)\s*/\s*(.+)$', val.strip())
    if not m:
        return val
    left  = m.group(1).strip()
    right = m.group(2).strip()

    def pad_part(part):
        if re.match(r'^\d+$', part):
            return part.zfill(3)
        return part

    return f"{pad_part(left)} / {pad_part(right)}"

def clean_kode_badan_usaha(val):
    if is_pseudo_null(val):
        return None
    val = str(val).strip().lstrip("'")
    if re.fullmatch(r"\d+\.0", val):
        val = val[:-2]
    return val if val else None

def parse_date_indonesia(val):
    """
    Parse tanggal dari berbagai format sumber dan kembalikan dalam format YYYY-MM-DD
    (sesuai format PostgreSQL DATE).
    Format sumber yang ditangani:
      - 'D Bulan YYYY'       : '30 Maret 2023'   -> '2023-03-30'
      - 'D Month YYYY'       : '4 October 2021'  -> '2021-10-04'
      - 'DD-Mon-YY'          : '12-Nov-14'        -> '2014-11-12'
      - 'DD-Mon-YYYY'        : '6-Nov-2023'       -> '2023-11-06'
      - 'YYYY-MM-DD'         : '2023-03-30'       -> '2023-03-30' (sudah benar)
      - 'DD/MM/YY'           : '06/11/14'         -> '2014-11-06'
      - 'DD/MM/YYYY'         : '06/11/2023'       -> '2023-11-06'
      - 'DD-MM-YYYY'         : '30-03-2023'       -> '2023-03-30'
    """
    if is_pseudo_null(val):
        return None
    s = str(val).strip()

    m = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d+)", s)
    if m:
        day, mon_str, yr = m.group(1), m.group(2).lower(), m.group(3)
        mon = BULAN_ID.get(mon_str) or MONTH_EN_ABBR.get(mon_str[:3])
        if mon:
            return f"{yr}-{mon}-{int(day):02d}"

    m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{2,4})", s)
    if m:
        day, mon_str, yr = m.group(1), m.group(2).lower(), m.group(3)
        mon = MONTH_EN_ABBR.get(mon_str)
        if mon:
            if len(yr) == 2:
                yr = "20" + yr
            return f"{yr}-{mon}-{int(day):02d}"

    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s

    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        day, mon, yr = m.group(1), m.group(2), m.group(3)
        if len(yr) == 2:
            yr = "20" + yr
        return f"{yr}-{int(mon):02d}-{int(day):02d}"

    m = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    print(f"  [WARN] Tanggal tidak dapat di-parse: {s!r}")
    return s

def normalize_country(val):
    if is_pseudo_null(val):
        return None
    key = str(val).strip().lower()
    if key in COUNTRY_MAP:
        return COUNTRY_MAP[key]
    return str(val).strip().title()


def clean_jenis_kepemilikan(val):
    if is_pseudo_null(val):
        return None
    s = str(val).strip()
    if re.fullmatch(r"\d+", s):
        return None
    return s


def clean_no(val):
    if is_pseudo_null(val):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def clean_no_with_default(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0
    if str(val).strip().lower() in PSEUDO_NULL_VALUES:
        return 0
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return 0


def clean_decimal_fixed(val):
    if is_pseudo_null(val):
        return None
    try:
        d = decimal.Decimal(str(val).strip())
        return format(d, "f")
    except (decimal.InvalidOperation, ValueError):
        return None

def clean_numeric(val):
    if is_pseudo_null(val):
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None

DATE_RAW_COLS = {
    "2_Susunan_Direksi":  ["mulai menjabat", "akhir menjabat"],
    "4_Daftar_Perizinan": ["tanggal berlaku", "tanggal berakhir"],
}


def detect_typos_raw(df_raw, file_key):
    for col in DATE_RAW_COLS.get(file_key, []):
        if col not in df_raw.columns:
            continue
        for idx, val in df_raw[col].items():
            s = str(val).strip()
            if s.lower() in PSEUDO_NULL_VALUES:
                continue
            m = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d+)", s)
            if m and len(m.group(3)) != 4:
                _flag(file_key, col, idx + 2, s,
                      f"Tahun memiliki {len(m.group(3))} digit (bukan 4), kemungkinan typo")

    pct_col = "persentase saham"
    if pct_col in df_raw.columns:
        for idx, val in df_raw[pct_col].items():
            s = str(val).strip()
            if s.lower() in PSEUDO_NULL_VALUES:
                continue
            try:
                d = decimal.Decimal(s)
                if d < 0:
                    _flag(file_key, pct_col, idx + 2, s,
                          "persentase_saham bernilai negatif")
                elif d > 100:
                    _flag(file_key, pct_col, idx + 2, s,
                          "persentase_saham melebihi 100%")
            except decimal.InvalidOperation:
                pass

    luas_col = "luas (ha)"
    if luas_col in df_raw.columns:
        for idx, val in df_raw[luas_col].items():
            s = str(val).strip()
            if s.lower() in PSEUDO_NULL_VALUES:
                continue
            try:
                d = decimal.Decimal(s)
                if d <= 0:
                    _flag(file_key, luas_col, idx + 2, s,
                          "luas_ha bernilai nol atau negatif")
            except decimal.InvalidOperation:
                pass

def clean_dataframe(df_raw, file_key):
    print(f"\n{'='*60}")
    print(f"  Processing : {file_key}")
    print(f"  Shape awal : {df_raw.shape}")

    detect_typos_raw(df_raw, file_key)

    df = df_raw.copy()

    #rename kolom
    df = df.rename(columns=COLUMN_RENAME)
    print(f"  Kolom      : {list(df.columns)}")

    #menghapus baris (delete entire row) nama_perusahaan_asal kosong
    before = len(df)
    mask = (
        df["nama_perusahaan_asal"].isnull() |
        df["nama_perusahaan_asal"].astype(str).str.strip().str.lower().isin(PSEUDO_NULL_VALUES)
    )
    df = df[~mask].copy()
    if len(df) < before:
        print(f"  [DEL] Baris dihapus (nama_perusahaan_asal kosong): {before - len(df)}")

    #normalisasi whitespace
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].apply(lambda v: normalize_whitespace(v) if isinstance(v, str) else v)

    #menghapus leading apostrophe
    for col in APOSTROPHE_COLS.get(file_key, []):
        if col in df.columns:
            df[col] = df[col].apply(strip_leading_apostrophe)
            print(f"  [APOS] Strip apostrof: {col}")

    #zero-pad kolom rt/rw setelah apostrof dihilangkan
    if file_key == "1_Informasi_Badan_Usaha" and "rt_rw" in df.columns:
        df["rt_rw"] = df["rt_rw"].apply(pad_rt_rw)
        print("  [RTRW] Zero-pad rt_rw ke 3 digit per bagian")

    #pseudo-NULL dihilangkan
    special_cols = set()
    if file_key == "1_Informasi_Badan_Usaha":
        special_cols.add("kode_badan_usaha")
    if file_key in DATE_COLS:
        special_cols.update(DATE_COLS[file_key])
    if file_key == "3_Pemegang_Saham":
        special_cols.update(["jenis_kepemilikan","asal_negara","persentase_saham","no"])
    if file_key == "2_Susunan_Direksi":
        special_cols.add("no")
    if file_key == "4_Daftar_Perizinan":
        special_cols.update(["luas_ha","no"])

    for col in df.columns:
        if col in special_cols:
            continue
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].apply(to_null_if_pseudo)

    if "kode_badan_usaha" in df.columns:
        df["kode_badan_usaha"] = df["kode_badan_usaha"].apply(clean_kode_badan_usaha)
        print("  [DTYPE] kode_badan_usaha → VARCHAR")

    #normalisasi tanggal ke YYYY-MM-DD
    for col in DATE_COLS.get(file_key, []):
        if col in df.columns:
            df[col] = [
                parse_date_indonesia(v)
                for v in df[col]
            ]
            print(f"  [DATE] Normalisasi: {col}")

    #normalisasi nama negara
    if "asal_negara" in df.columns:
        df["asal_negara"] = df["asal_negara"].apply(normalize_country)
        print("  [COUNTRY] Normalisasi asal_negara")

    #normalisasi jenis kepemilikan
    if "jenis_kepemilikan" in df.columns:
        df["jenis_kepemilikan"] = df["jenis_kepemilikan"].apply(clean_jenis_kepemilikan)
        print("  [ENUM] jenis_kepemilikan: numerik → NULL")

    #integer nullable diubah menjadi 0 
    if "no" in df.columns:
        df["no"] = df["no"].apply(clean_no_with_default)
        df["no"] = pd.array(df["no"], dtype=pd.Int64Dtype())
        print("  [DTYPE] no → Integer nullable (pseudo-null → 0)")

    #persentase_saham
    if "persentase_saham" in df.columns:
        df["persentase_saham"] = df["persentase_saham"].apply(clean_decimal_fixed)
        print("  [DTYPE] persentase_saham → Decimal fixed-point")

    #luas_ha
    if "luas_ha" in df.columns:
        df["luas_ha"] = df["luas_ha"].apply(clean_numeric)
        print("  [DTYPE] luas_ha → Numeric nullable")

    #tambah kolom tgl_import: tanggal hari ini saat script dijalankan (format YYYY-MM-DD)
    df["tgl_import"] = datetime.now().strftime("%Y-%m-%d")
    print(f"  [IMPORT] tgl_import diisi: {datetime.now().strftime('%Y-%m-%d')} ({len(df)} baris)")

    print(f"  Shape akhir: {df.shape}")
    return df


def main():
    base_dir = Path(__file__).parent

    print(f"\nOutput folder: {OUTPUT_DIR.resolve()}")

    for file_key, filename in INPUT_FILES.items():
        input_path = base_dir / filename
        if not input_path.exists():
            print(f"[SKIP] Tidak ditemukan: {input_path}")
            continue

        df_raw = pd.read_csv(input_path, dtype=str, keep_default_na=False)
        df_clean = clean_dataframe(df_raw, file_key)

        output_path = OUTPUT_DIR / filename
        df_clean.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"  [SAVE] -> {output_path}")

    write_typo_log()
    print("\n✓ Selesai")
    print(f"  Output CSV : {OUTPUT_DIR.resolve()}")
    print(f"  Typo log   : {TYPO_LOG.resolve()}")

if __name__ == "__main__":
    main()