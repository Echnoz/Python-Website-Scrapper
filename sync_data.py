"""
Guide:
  1. Pastikan file master sudah diperbarui dengan daftar terbaru
  2. set mode DRY_RUN ke 'False' untuk sinkronisasi file
  3. set mode DRY_RUN ke 'True' untuk menampilkan perubahan saja 

Catatan:
  Script otomatis membuat backup sebelum menghapus data
  Backup disimpan di folder 'backup_sinkronisasi/'
"""

import os
import shutil
import pandas as pd
from datetime import datetime

#Config
DRY_RUN = True   #True = hanya tampilkan perubahan tanpa eksekusi sinkronisasi

FILE_MASTER    = "data_listing_badan_usaha_minerbaone.csv"
FILE_INFO      = "1_Informasi_Badan_Usaha.csv"
FILE_DIREKSI   = "2_Susunan_Direksi.csv"
FILE_SAHAM     = "3_Pemegang_Saham.csv"
FILE_PERIZINAN = "4_Daftar_Perizinan.csv"

#kolom penghubung di setiap file hasil scraping
KOLOM_KUNCI = "Nama Perusahaan Asal" #foreign key ke file master

FOLDER_BACKUP = "backup_sinkronisasi"


#Utility
def buat_backup(daftar_file):
    timestamp = datetime.now().strftime("%d-%m-%Y_%H.%M")
    folder    = os.path.join(FOLDER_BACKUP, timestamp)
    os.makedirs(folder, exist_ok=True)

    for path_file in daftar_file:
        if os.path.exists(path_file):
            nama_file = os.path.basename(path_file)
            shutil.copy2(path_file, os.path.join(folder, nama_file))

    print(f"  Backup disimpan di: {folder}/")
    return folder


def baca_csv_aman(path_file):
    if not os.path.exists(path_file):
        return None, f"File tidak ditemukan: {path_file}"
    try:
        df = pd.read_csv(path_file)
        return df, None
    except Exception as e:
        return None, f"Gagal membaca {path_file}: {e}"


def normalisasi_nama(series):
    import re
    return series.fillna("").astype(str).apply(
        lambda x: re.sub(r'\s+', ' ', x.strip().lower())
    )


#Deteksi perusahaan yang hilang
def deteksi_perusahaan_hilang(df_master, df_info): #membandingkan daftar perusahaan antara file master dengan file data scraping

    #Nama di master (kolom 'Nama Badan Usaha')
    if 'Nama Badan Usaha' not in df_master.columns:
        print("  [ERROR] Kolom 'Nama Badan Usaha' tidak ditemukan di file master.")
        return set()

    nama_master = set(normalisasi_nama(
        df_master['Nama Badan Usaha'].dropna()
    ))

    #Nama di FILE_INFO (kolom 'Nama Perusahaan Asal')
    if KOLOM_KUNCI not in df_info.columns:
        print(f"  [ERROR] Kolom '{KOLOM_KUNCI}' tidak ditemukan di {FILE_INFO}.")
        return set()

    nama_info_raw    = df_info[KOLOM_KUNCI].dropna().unique()
    nama_info_normal = {normalisasi_nama(pd.Series([n]))[0]: n for n in nama_info_raw}

    #Perusahaan yang ada di FILE_INFO tapi tidak ada di master
    hilang_normal = set(nama_info_normal.keys()) - nama_master
    hilang_asli   = {nama_info_normal[n] for n in hilang_normal}

    return hilang_asli


def hapus_dari_file(path_file, perusahaan_hilang, dry_run=False):
    df, err = baca_csv_aman(path_file)

    if df is None:
        return {"file": path_file, "status": "skip", "alasan": err}

    if KOLOM_KUNCI not in df.columns:
        return {
            "file"   : path_file,
            "status" : "skip",
            "alasan" : f"Kolom '{KOLOM_KUNCI}' tidak ditemukan"
        }

    baris_sebelum = len(df)

    #normalisasi kolom kunci di DataFrame
    import re
    kolom_normal = df[KOLOM_KUNCI].fillna("").astype(str).apply(
        lambda x: re.sub(r'\s+', ' ', x.strip().lower())
    )

    #normalisasi set perusahaan yang akan dihapus
    hilang_normal = {
        re.sub(r'\s+', ' ', n.strip().lower()) for n in perusahaan_hilang
    }

    #Mask: True = baris yang harus dihapus
    mask_hapus  = kolom_normal.isin(hilang_normal)
    jumlah_hapus = mask_hapus.sum()
    baris_sesudah = baris_sebelum - jumlah_hapus

    if not dry_run and jumlah_hapus > 0:
        df_bersih = df[~mask_hapus].reset_index(drop=True)
        df_bersih.to_csv(path_file, index=False)

    return {
        "file"         : os.path.basename(path_file),
        "status"       : "ok",
        "sebelum"      : baris_sebelum,
        "dihapus"      : int(jumlah_hapus),
        "sesudah"      : baris_sesudah,
        "dry_run"      : dry_run,
    }


def tampilkan_ringkasan(perusahaan_hilang, hasil_per_file):
    """Tampilkan tabel ringkasan perubahan ke terminal."""
    print("\n" + "=" * 65)
    print("RINGKASAN SINKRONISASI")
    print("=" * 65)

    print(f"\nPerusahaan yang akan dihapus ({len(perusahaan_hilang)} total):")
    for i, nama in enumerate(sorted(perusahaan_hilang), 1):
        print(f"  {i:>3}. {nama}")

    print(f"\nDampak per file:")
    print(f"  {'File':<40} {'Sebelum':>8} {'Dihapus':>8} {'Sesudah':>8}")
    print(f"  {'-'*40} {'-'*8} {'-'*8} {'-'*8}")

    for h in hasil_per_file:
        if h["status"] == "skip":
            print(f"  {h['file']:<40} {'SKIP':>8}  ({h.get('alasan','')})")
        else:
            print(f"  {h['file']:<40} {h['sebelum']:>8} {h['dihapus']:>8} {h['sesudah']:>8}")

    print()

# MAIN
def main():
    print("=" * 65)
    if DRY_RUN:
        print("SINKRONISASI DATA — MODE DRY-RUN (tidak ada file yang diubah)")
    else:
        print("SINKRONISASI DATA MINERBAONE")
    print("=" * 65 + "\n")

    #membaca file master
    print("Membaca file master...")
    df_master, err = baca_csv_aman(FILE_MASTER)
    if df_master is None:
        print(f"[ERROR] {err}")
        return

    print(f"  File master: {len(df_master)} baris")

    #membaca FILE_INFO sebagai referensi perusahaan yang sudah discrape
    print("Membaca file hasil scraping...")
    df_info, err = baca_csv_aman(FILE_INFO)
    if df_info is None:
        print(f"[ERROR] {err}")
        return

    print(f"  {FILE_INFO}: {len(df_info)} baris")

    #deteksi perusahaan yang hilang dari file master
    print("Mendeteksi perusahaan yang sudah tidak ada di master...")
    perusahaan_hilang = deteksi_perusahaan_hilang(df_master, df_info)

    if not perusahaan_hilang:
        print("\n✓ Tidak ada perusahaan yang perlu dihapus")
        print("  Semua data hasil scraping sudah sinkron dengan file master")
        return

    print(f"  Ditemukan {len(perusahaan_hilang)} perusahaan yang hilang dari file master")

    semua_file = [FILE_INFO, FILE_DIREKSI, FILE_SAHAM, FILE_PERIZINAN]
    hasil_preview = [
        hapus_dari_file(f, perusahaan_hilang, dry_run=True)
        for f in semua_file
    ]

    tampilkan_ringkasan(perusahaan_hilang, hasil_preview)

    #konfirmasi jenis eksekusi
    if DRY_RUN:
        print("Mode DRY-RUN aktif — tidak ada perubahan yang dilakukan")
        print("Ubah DRY_RUN = False untuk melakukan penghapusan")
        return

    konfirmasi = input(
        "Lanjutkan penghapusan? Backup akan dibuat otomatis. (ya/tidak): "
    ).strip().lower()

    if konfirmasi != "ya":
        print("\nDibatalkan. Tidak ada file yang diubah")
        return

    #backup
    print("\nMembuat backup")
    buat_backup(semua_file)

    #penghapusan
    print("Menghapus data")
    hasil_eksekusi = [
        hapus_dari_file(f, perusahaan_hilang, dry_run=False)
        for f in semua_file
    ]

    #hasil akhir
    print("\n" + "=" * 65)
    print("HASIL EKSEKUSI")
    print("=" * 65)
    print(f"  {'File':<40} {'Dihapus':>8} {'Status':>10}")
    print(f"  {'-'*40} {'-'*8} {'-'*10}")

    total_dihapus = 0
    for h in hasil_eksekusi:
        if h["status"] == "skip":
            print(f"  {h['file']:<40} {'—':>8} {'SKIP':>10}")
        else:
            dihapus = h["dihapus"]
            total_dihapus += dihapus
            status = "✓ OK" if dihapus >= 0 else "✗ ERROR"
            print(f"  {h['file']:<40} {dihapus:>8} {status:>10}")

    print(f"\n  Total baris dihapus: {total_dihapus}")
    print(f"  Backup tersedia di folder: {FOLDER_BACKUP}/")
    print("\nSinkronisasi selesai.")


if __name__ == "__main__":
    main()