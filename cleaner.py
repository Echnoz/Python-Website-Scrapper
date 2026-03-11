"""
Fungsi: Membaca 5 file CSV hasil scrapping minerbaone.esdm.go.id,
        melakukan data cleaning, lalu menyimpannya kembali sebagai
        5 file CSV bersih ke dalam satu folder output.

Cleaning yang dilakukan:
  1. Standarisasi format tanggal → YYYY-MM-DD
  2. Handling NULL: "Tidak Ada Data", "Tidak ada data", "-" → nilai kosong (NULL)
  3. Data Type Casting: kolom angka di-cast ke numerik
"""

import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path

# Config
DEFAULT_INPUT_DIR  = "."
DEFAULT_OUTPUT_DIR = "output_clean"

FILE_MAP = {
    "Listing Badan Usaha":  "data_listing_badan_usaha_minerbaone.csv",
    "Informasi Badan Usaha": "1_Informasi_Badan_Usaha.csv",
    "Susunan Direksi":       "2_Susunan_Direksi.csv",
    "Pemegang Saham":        "3_Pemegang_Saham.csv",
    "Daftar Perizinan":      "4_Daftar_Perizinan.csv",
}

# Kolom yang berisi nilai angka (akan di-cast ke numerik)
NUMERIC_COLS = {
    "Daftar Perizinan":      ["luas (ha)", "no"],
    "Susunan Direksi":       ["no."],
    "Pemegang Saham":        ["no.", "persentase saham"],
    "Listing Badan Usaha":   ["No"],
}

# Kolom yang berisi tanggal (akan distandarisasi ke YYYY-MM-DD)
DATE_COLS = {
    "Susunan Direksi":  ["mulai menjabat", "akhir menjabat"],
    "Daftar Perizinan": ["tanggal berlaku", "tanggal berakhir"],
}

# Nilai yang dianggap NULL
NULL_VALUES = ["-", "Tidak Ada Data", "Tidak ada data", "tidak ada data",
               "TIDAK ADA DATA", "N/A", "n/a", "", " "]

# Mapping ID bulan
BULAN_ID = {
    "Januari": "01", "Februari": "02", "Maret": "03", "April": "04",
    "Mei": "05", "Juni": "06", "Juli": "07", "Agustus": "08",
    "September": "09", "Oktober": "10", "November": "11", "Desember": "12",
}


def parse_tanggal(nilai: str) -> str | None:
    """
    Mengubah berbagai format tanggal menjadi YYYY-MM-DD.
    Format yang diubah:
      - "30 Maret 2023"   (bahasa Indonesia)
      - "12-Nov-14"       (dd-Mon-yy, singkatan bulan Inggris)
      - "06-Nov-23"       (dd-Mon-yy)
      - "30-03-2023"      (dd-MM-yyyy)
      - "2023-03-30"      (sudah benar, tidak diubah)
    Mengembalikan None jika tidak bisa diparse
    """
    if pd.isna(nilai) or str(nilai).strip() in NULL_VALUES:
        return None

    s = str(nilai).strip()

    # Format: "30 Maret 2023" (bahasa indonesia)
    for nama_bulan, nomor_bulan in BULAN_ID.items():
        if nama_bulan in s:
            parts = s.split()
            if len(parts) == 3:
                try:
                    return f"{parts[2]}-{nomor_bulan}-{int(parts[0]):02d}"
                except ValueError:
                    return None

    # Coba parsing otomatis dengan pandas (menangani dd-Mon-yy, dd-MM-yyyy, dst)
    try:
        dt = pd.to_datetime(s, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def bersihkan_null(df: pd.DataFrame) -> pd.DataFrame:
    """Mengganti semua representasi NULL tekstual menjadi np.nan."""
    df = df.replace(NULL_VALUES, np.nan)

    # Membersihkan kolom bertipe string: strip whitespace, lalu ubah ke NaN jika kosong
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        # Hapus prefix apostrophe (') yang muncul dari excel/csv
        df[col] = df[col].str.lstrip("'")
        df[col] = df[col].replace(NULL_VALUES + ["nan", "None"], np.nan)

    return df


def cast_numerik(df: pd.DataFrame, kolom_numerik: list[str]) -> pd.DataFrame:
    """Cast kolom yang seharusnya numerik ke tipe float/int."""
    for col in kolom_numerik:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def standarisasi_tanggal(df: pd.DataFrame, kolom_tanggal: list[str]) -> pd.DataFrame:
    """Standarisasi kolom tanggal ke format YYYY-MM-DD."""
    for col in kolom_tanggal:
        if col in df.columns:
            df[col] = df[col].apply(parse_tanggal)
    return df


def proses_sheet(nama_sheet: str, df: pd.DataFrame) -> pd.DataFrame:
    """Pipeline cleaning untuk satu sheet."""
    df = bersihkan_null(df)

    if nama_sheet in DATE_COLS:
        df = standarisasi_tanggal(df, DATE_COLS[nama_sheet])

    if nama_sheet in NUMERIC_COLS:
        df = cast_numerik(df, NUMERIC_COLS[nama_sheet])

    return df


def main():
    parser = argparse.ArgumentParser(description="Clean CSV minerbaone → 5 CSV bersih")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR,
                        help="Direktori tempat file CSV berada (default: direktori saat ini)")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help="Direktori output file CSV bersih (default: output_clean/)")
    args = parser.parse_args()

    input_dir  = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  MINERBAONE DATA CLEANER")
    print("=" * 60)

    for nama_sheet, nama_file in FILE_MAP.items():
        filepath = input_dir / nama_file

        if not filepath.exists():
            print(f"\n   File tidak ditemukan, dilewati: {filepath}")
            continue

        print(f"\n Memproses: {nama_file}")
        df = pd.read_csv(filepath, dtype=str, encoding="utf-8-sig")
        baris_awal = len(df)

        df = proses_sheet(nama_sheet, df)

        # Simpan sebagai file .csv dengan nama file yang sama
        output_path = output_dir / nama_file
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"   {nama_file}: {baris_awal} baris → tersimpan di {output_path}")

    print(f"\n Selesai! Semua file tersimpan di folder: {output_dir.resolve()}/")
    print("=" * 60)


if __name__ == "__main__":
    main()