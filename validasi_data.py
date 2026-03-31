import os
import pandas as pd
from collections import Counter

FILE_MASTER    = "data_listing_badan_usaha_minerbaone.csv"
FILE_INFO      = "1_Informasi_Badan_Usaha.csv"
FILE_DIREKSI   = "2_Susunan_Direksi.csv"
FILE_SAHAM     = "3_Pemegang_Saham.csv"
FILE_PERIZINAN = "4_Daftar_Perizinan.csv"
FILE_LAPORAN   = "laporan_validasi_kelengkapan.txt"

KOLOM_NAMA_MASTER = "Nama Badan Usaha"
KOLOM_NAMA_FILE   = "Nama Perusahaan Asal"


def baca_nama_set(path):
    if not os.path.exists(path):
        return None, f"FILE TIDAK DITEMUKAN: {path}"
    try:
        df = pd.read_csv(path, dtype=str)
        if KOLOM_NAMA_FILE not in df.columns:
            return None, f"Kolom '{KOLOM_NAMA_FILE}' tidak ada"
        return (set(df[KOLOM_NAMA_FILE].dropna().astype(str).str.strip()),
                f"{len(df):,} baris")
    except Exception as e:
        return None, f"Error: {e}"


def main():
    print("=" * 65)
    print("Validasi Data")
    print("=" * 65)

    # ── 1. Baca master ──────────────────────────────────────────
    if not os.path.exists(FILE_MASTER):
        print(f"\n[ERROR] File master tidak ditemukan: {FILE_MASTER}")
        return

    df_master = pd.read_csv(FILE_MASTER, dtype=str)
    semua_nama = df_master[KOLOM_NAMA_MASTER].dropna().astype(str).str.strip().tolist()
    nama_master_set    = set(semua_nama)
    nama_master_counter = Counter(semua_nama)
    print(f"\nFile master     : {len(semua_nama):,} entri, "
          f"{len(nama_master_set):,} nama unik")

    files = {
        "1_Informasi_Badan_Usaha" : FILE_INFO,
        "2_Susunan_Direksi"       : FILE_DIREKSI,
        "3_Pemegang_Saham"        : FILE_SAHAM,
        "4_Daftar_Perizinan"      : FILE_PERIZINAN,
    }

    hasil = {}
    print()
    for label, path in files.items():
        nama_set, info = baca_nama_set(path)
        status = "✓" if nama_set is not None else "✗"
        print(f"  {status} {label}: {info}")
        hasil[label] = nama_set

    print("\n" + "=" * 65)
    print("HASIL ANALISIS")
    print("=" * 65)

    info_set = hasil.get("1_Informasi_Badan_Usaha") or set()
    hilang_dari_info = sorted(nama_master_set - info_set)

    laporan_per_file = {}
    for label, nama_set in hasil.items():
        if nama_set is None:
            laporan_per_file[label] = None
            continue
        hilang = sorted(nama_master_set - nama_set)
        laporan_per_file[label] = hilang

    print(f"\n[1] Perusahaan yang BELUM ada di 1_Informasi_Badan_Usaha "
          f"({len(hilang_dari_info)} nama):")
    if not hilang_dari_info:
        print("    → Semua perusahaan sudah tersimpan ✓")
    else:
        for i, n in enumerate(hilang_dari_info, 1):
            print(f"    {i:>4}. {n}")

    print()
    for label, hilang in laporan_per_file.items():
        if label == "1_Informasi_Badan_Usaha":
            continue
        if hilang is None:
            print(f"[!] {label}: tidak bisa dicek (file tidak ada)")
            continue
        hilang_juga_dari_info = [n for n in hilang if n in hilang_dari_info]
        hilang_hanya_file_ini = [n for n in hilang if n not in hilang_dari_info]
        print(f"[{label}]")
        print(f"  Hilang karena belum di-scrape : {len(hilang_juga_dari_info)}")
        print(f"  Ada di file 1 tapi tidak ada di sini: {len(hilang_hanya_file_ini)} ")
        if hilang_hanya_file_ini and len(hilang_hanya_file_ini) <= 30:
            for n in hilang_hanya_file_ini[:30]:
                print(f"    - {n}")
        elif hilang_hanya_file_ini:
            print(f"    (terlalu banyak, lihat file laporan untuk daftar lengkap)")
        print()

    from datetime import datetime
    with open(FILE_LAPORAN, "w", encoding="utf-8") as f:
        f.write(f"Laporan Validasi Data\n")
        f.write(f"Waktu  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Master : {len(semua_nama):,} entri, {len(nama_master_set):,} unik\n")
        f.write("=" * 65 + "\n\n")

        f.write(f"[1_Informasi_Badan_Usaha] BELUM TERSIMPAN ({len(hilang_dari_info)} nama):\n")
        for n in hilang_dari_info:
            cnt = nama_master_counter[n]
            f.write(f"  - {n}" + (f" [{cnt}x di master]" if cnt > 1 else "") + "\n")

        f.write("\n")
        for label, hilang in laporan_per_file.items():
            if label == "1_Informasi_Badan_Usaha" or hilang is None:
                continue
            hilang_hanya = [n for n in hilang if n not in hilang_dari_info]
            f.write(f"[{label}] Ada di INFO tapi tidak di file ini "
                    f"({len(hilang_hanya)} nama):\n")
            for n in hilang_hanya:
                f.write(f"  - {n}\n")
            f.write("\n")

    print(f"Laporan lengkap disimpan di {FILE_LAPORAN}")
    print("=" * 65)


if __name__ == "__main__":
    main()