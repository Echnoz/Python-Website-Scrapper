import os
import io
import re
import time
import pandas as pd
# [R1] Tambah defaultdict — dibutuhkan untuk nospace_grup di main()
from collections import Counter, defaultdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    ElementClickInterceptedException, WebDriverException,
    StaleElementReferenceException
)
from bs4 import BeautifulSoup

# Config
MAX_RETRY        = 3
RESTART_INTERVAL = 100
BASE_URL         = "https://minerbaone.esdm.go.id/publik/badan-usaha"

FILE_MASTER    = "data_listing_badan_usaha_minerbaone.csv"
FILE_INFO      = "1_Informasi_Badan_Usaha.csv"
FILE_DIREKSI   = "2_Susunan_Direksi.csv"
FILE_SAHAM     = "3_Pemegang_Saham.csv"
FILE_PERIZINAN = "4_Daftar_Perizinan.csv"
FILE_ERROR     = "daftar_error.txt"
FILE_DIAG      = "diagnostik_html.txt"

MODE_DIAGNOSTIK  = False
DIAG_MAX_SAMPEL  = 3

# [P5] Kolom standar untuk baris placeholder "tidak ada data".
# Dipakai di _simpan_hasil() ketika parse_tabel() tidak menghasilkan data
# (tabel di website memang kosong / website tampilkan "Tidak ada data").
# Placeholder: Nama Perusahaan Asal = nama valid, Kode = kode valid,
# semua kolom data = 'Tidak ada data'.
# Urutan kolom mengikuti output parse_tabel() (P4): Nama, Kode, lalu kolom data.
KOLOM_STANDAR_DIREKSI = [
    'Nama Perusahaan Asal',
    'no.', 'nama direksi', 'mulai menjabat', 'akhir menjabat', 'jabatan',
]
KOLOM_STANDAR_SAHAM = [
    'Nama Perusahaan Asal',
    'no.', 'jenis kepemilikan', 'nama', 'kewarganegaraan',
    'asal negara', 'persentase saham',
]
KOLOM_STANDAR_PERIZINAN = [
    'Nama Perusahaan Asal',
    'no', 'nomor izin', 'jenis izin', 'tahap kegiatan', 'golongan', 'komoditas',
    'luas (ha)', 'tanggal berlaku', 'tanggal berakhir', 'status cnc',
    'lokasi', 'kode wiup', 'modi id',
]

KUNCI_PENCARIAN = [
    "Nama Badan Usaha", "Kode Badan Usaha", "Jenis Badan Usaha",
    "Kelurahan", "NPWP", "RT/RW", "Kode Pos", "Alamat"
]

# [R3] Tambah entry "Jenis Badan Usaha" ke FORMAT_NILAI.
# Field ini bisa overflow mengambil blok NPWP / RT/RW jika nilai aslinya kosong
# di HTML. Pola regex ini memastikan nilai yang diambil bukan teks numerik
# seperti NPWP, kode pos, atau label field lain.
# 4 entry lama (NPWP, Kode Badan Usaha, Kode Pos, RT/RW) tidak berubah.
FORMAT_NILAI = {
    "NPWP"            : re.compile(r"^[\d\.\-\*\/]{5,25}$"),
    "Kode Badan Usaha": re.compile(r"^\d+$"),
    "Kode Pos"        : re.compile(r"^\d{3,6}$"),
    "RT/RW"           : re.compile(r"^[\d\-]+\s*/\s*[\d\-]+$|^-$"),
    "Jenis Badan Usaha": re.compile(        # [R3] entry baru
        r"^(?!.*(NPWP|RT\s*/\s*RW|Kode\s*Pos|\d{3}\.\d{3}\.\d{3})).+",
        re.IGNORECASE
    ),
}

LABEL_BLACKLIST   = {k.lower().strip() for k in KUNCI_PENCARIAN}
SECTION_BLACKLIST = {
    "susunan direksi dan komisaris", "susunan direksi", "daftar perizinan",
    "pemegang saham", "informasi badan usaha", "informasi umum",
    "data perizinan", "data perusahaan",
}
NILAI_BLACKLIST = LABEL_BLACKLIST | SECTION_BLACKLIST
_diag_counter   = 0


# Utility
def normalisasi(teks):
    return re.sub(r'\s+', ' ', str(teks).strip().lower())


# [R2] Fungsi baru nospace().
# Menghilangkan semua spasi untuk mencocokkan nama kembar yang penulisan
# spasinya berbeda di master vs website.
# Contoh: "BARA MUSTIKA ENERGINDO" vs "BARA MUSTIKAENERGINDO"
# Dipakai di: hitung_skor() (R4), verifikasi_halaman_detail() (R5),
#             dan kembar detection di main() (R15).
def nospace(teks):
    return re.sub(r'\s+', '', str(teks).strip().lower())


# [S1] Fungsi baru sanitasi_nama().
# Membersihkan karakter whitespace tersembunyi (tab \t, newline \n, carriage
# return \r) yang bisa ada di nama perusahaan dalam file master CSV.
# Karakter ini tidak terlihat di Excel/text editor biasa, tapi menyebabkan
# dua nama yang secara visual sama diperlakukan sebagai string berbeda oleh
# Python — merusak pencocokan, resume, dan deteksi kembar.
#
# Contoh nyata di master: 'BARA MUSTIKA\tENERGINDO' vs 'BARA MUSTIKA ENERGINDO'
#
# Tiga langkah:
#   1. Ganti semua tab/newline dengan spasi tunggal
#   2. Strip spasi di kedua ujung
#   3. Normalisasi spasi ganda/berulang di tengah menjadi spasi tunggal
#
# Fungsi ini HANYA mengubah whitespace — huruf, angka, dan karakter lain
# tidak disentuh sama sekali. Nama yang sudah bersih tidak akan berubah.
def sanitasi_nama(nama):
    nama = re.sub(r'[\t\n\r]+', ' ', str(nama))  # tab/newline → spasi
    nama = nama.strip()                           # hapus spasi di ujung
    nama = re.sub(r' {2,}', ' ', nama)            # spasi ganda → tunggal
    return nama


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver, WebDriverWait(driver, 15)


def tutup_popup(driver):
    for sel in [
        "//button[contains(@class,'close') or contains(@aria-label,'Close') or contains(@aria-label,'Tutup')]",
        "//button[contains(text(),'OK') or contains(text(),'Tutup') or contains(text(),'Close')]",
        "//*[@data-dismiss='modal']",
    ]:
        try:
            el = driver.find_element(By.XPATH, sel)
            if el.is_displayed():
                driver.execute_script("arguments[0].click();", el)
                time.sleep(0.8)
        except Exception:
            pass


def tunggu_loading_selesai(wait, driver, timeout=15):
    try:
        wait.until_not(EC.presence_of_element_located((By.XPATH,
            "//*[contains(text(),'Memuat...') or contains(@class,'spinner') "
            "or contains(@class,'loading')]"
        )))
    except TimeoutException:
        pass
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except TimeoutException:
        pass


def dump_diagnostik(driver, nama, tahap):
    global _diag_counter
    if not MODE_DIAGNOSTIK or _diag_counter >= DIAG_MAX_SAMPEL:
        return
    _diag_counter += 1
    try:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        with open(FILE_DIAG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*70}\nSAMPEL #{_diag_counter} | {nama} | {tahap}\n"
                    f"URL: {driver.current_url}\n{'='*70}\n")
            for i, tabel in enumerate(soup.find_all("table")):
                f.write(f"\n[TABLE #{i}]\n{tabel.prettify()[:3000]}\n")
            f.write("\n── Elemen <tr> ──\n")
            for i, tr in enumerate(soup.find_all("tr")[:10]):
                f.write(f"[TR #{i}] {tr.get_text(separator='|', strip=True)[:200]}\n")
            f.write("\n── Semua tombol ──\n")
            for i, btn in enumerate(soup.find_all("button")[:20]):
                f.write(f"[BTN #{i}] {btn.get('class')} | {btn.get_text(strip=True)[:80]}\n")
        print(f"  [DIAG] HTML disimpan ke '{FILE_DIAG}' (sampel #{_diag_counter})")
    except Exception as e:
        print(f"  [DIAG] Gagal dump: {e}")


# Penyimpanan
def _bersih_tad(df):
    # [P3-A] Hapus baris korup di mana Nama Perusahaan Asal = 'Tidak ada data'.
    # Baris ini masuk ke CSV dari versi script lama (sebelum filter _tabel_isi_kosong).
    # Setelah masuk, penyimpanan() selalu membaca CSV lama lalu pd.concat →
    # baris TAD ikut di-concat → ditulis ulang ke CSV → akumulasi tak terbatas.
    # Solusi: buang baris TAD dari df_old SEBELUM concat, sehingga tidak pernah
    # ditulis ulang. Satu kali berjalan → semua TAD lama hilang dari semua file.
    if 'Nama Perusahaan Asal' not in df.columns:
        return df
    mask_valid = df['Nama Perusahaan Asal'].astype(str).str.strip().str.lower() != 'tidak ada data'
    return df[mask_valid].copy()


def _fillna_aman(df):
    # [P3-B] Ganti fillna global dengan fillna PER KOLOM yang mengecualikan
    # 'Nama Perusahaan Asal'. fillna global (df.fillna("Tidak ada data")) berisiko
    # mengisi NaN di kolom Nama menjadi TAD jika ada concat yang menghasilkan NaN.
    # Dengan fillna per-kolom, Nama Perusahaan Asal TIDAK PERNAH di-fillna.
    # NaN di Nama akan menyebabkan baris terlihat aneh → mudah dideteksi & debug.
    for col in df.columns:
        if col != 'Nama Perusahaan Asal':
            df[col] = df[col].fillna('Tidak ada data')
    return df


def penyimpanan(nama_file, data_baru, kunci_dedup=None):
    if not data_baru:
        return
    df_new = (pd.concat(data_baru, ignore_index=True)
              if isinstance(data_baru[0], pd.DataFrame)
              else pd.DataFrame(data_baru))
    if df_new.empty:
        return
    # [P3-B] Pastikan df_new tidak punya NaN di Nama Perusahaan Asal
    # (seharusnya tidak terjadi, tapi sebagai safety net)
    if 'Nama Perusahaan Asal' in df_new.columns:
        df_new = df_new[df_new['Nama Perusahaan Asal'].astype(str).str.strip().str.lower() != 'tidak ada data']
        df_new = df_new[df_new['Nama Perusahaan Asal'].notna()]
    if df_new.empty:
        return
    df_old = pd.DataFrame()
    if os.path.exists(nama_file):
        try:
            df_old = pd.read_csv(nama_file)
            # [P3-A] Bersihkan baris TAD dari data lama sebelum concat
            df_old = _bersih_tad(df_old)
        except Exception:
            pass
    if kunci_dedup and not df_old.empty:
        kunci_ada = [k for k in kunci_dedup
                     if k in df_new.columns and k in df_old.columns]
        if kunci_ada:
            def sidik_jari(df, cols):
                return df[cols].fillna("").astype(str).apply(
                    lambda c: c.str.lower().str.strip()
                ).agg("|".join, axis=1)
            df_new = df_new[~sidik_jari(df_new, kunci_ada).isin(
                set(sidik_jari(df_old, kunci_ada)))]
        if df_new.empty:
            return
    df_final = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new
    # [P3-B] fillna aman: tidak menyentuh kolom Nama Perusahaan Asal
    _fillna_aman(df_final).to_csv(nama_file, index=False)


# Verifikasi halaman detail (anti ghost-scraping)
def verifikasi_halaman_detail(driver, wait, nama_target):
    # [R5] Tambah nospace fallback di pengecekan kecocokan nama.
    # Sebelumnya hanya: normalisasi(nilai) == normalisasi(nama_target)
    # Tambahan: jika cek normalisasi gagal, coba nospace(nilai) == nospace(nama_target)
    # Menangani kasus kembar yang namanya berbeda penulisan spasi di website vs master.
    try:
        soup  = BeautifulSoup(driver.page_source, "html.parser")
        label = soup.find(string=re.compile(r"^\s*Nama Badan Usaha\s*:?$", re.IGNORECASE))
        if not label:
            print(f"  [!] Label 'Nama Badan Usaha' tidak ditemukan di halaman detail.")
            return False
        nilai = None
        node  = label
        for _ in range(5):
            node = node.parent
            if not node:
                break
            sib = node.find_next_sibling()
            if sib:
                t = sib.get_text(separator=" ", strip=True)
                if t and t.lower().strip() not in NILAI_BLACKLIST:
                    nilai = t
                    break
        if not nilai:
            print(f"  [!] Tidak bisa membaca nilai 'Nama Badan Usaha'.")
            return False
        # Cek 1: normalisasi biasa — sama seperti base
        cocok = normalisasi(nilai) == normalisasi(nama_target)
        # [R5] Cek 2: nospace fallback
        if not cocok:
            cocok = nospace(nilai) == nospace(nama_target)
        if not cocok:
            print(f"  [✗] GHOST: target='{nama_target}' | halaman='{nilai}'")
        return cocok
    except Exception as e:
        print(f"  [!] Error verifikasi: {e}")
        return False


# Finder tombol detail
def semua_kata_ada(nama_target, teks_baris):
    return all(k in teks_baris.lower() for k in nama_target.lower().split())


def hitung_skor(nama_normal, teks_kandidat):
    # [R4] Tambah level skor 950 untuk nospace match.
    # Menangani kembar yang namanya identik tapi berbeda penulisan spasi.
    # Skor 950 (di bawah exact match 1000) memberi prioritas tinggi
    # tanpa menggeser kandidat exact match yang memang sempurna.
    teks   = normalisasi(teks_kandidat)
    nama_n = normalisasi(nama_normal)
    if not teks:
        return 0
    rasio = len(nama_n) / len(teks)
    if teks == nama_n:                    return 1000
    if nospace(teks) == nospace(nama_n):  return 950   # [R4] nospace match
    if teks.startswith(nama_n + " "):     return int(200 * rasio)
    if nama_n in teks:                    return int(150 * rasio)
    if semua_kata_ada(nama_n, teks):      return int(100 * rasio)
    return 0


def cari_tombol_detail_untuk_nama(driver, nama):
    nama_normal = nama.strip().lower()
    soup        = BeautifulSoup(driver.page_source, "html.parser")

    # Strategi A: <table>
    semua_baris = soup.find_all("tr")
    kandidat_a  = []
    for i, baris in enumerate(semua_baris):
        if not baris.find_all("td"):
            continue
        skor = max((hitung_skor(nama_normal, td.get_text(separator=" ", strip=True))
                    for td in baris.find_all("td")), default=0)
        if skor > 0:
            kandidat_a.append((skor, i, baris.get_text(strip=True)[:60]))
    if kandidat_a:
        kandidat_a.sort(key=lambda x: (-x[0], x[1]))
        skor_top, idx_top, teks_top = kandidat_a[0]
        print(f"  [Strategi A] Skor={skor_top} baris=#{idx_top} | '{teks_top[:40]}'")
        if skor_top >= 60:
            baris_td  = [b for b in semua_baris if b.find_all("td")]
            idx_td    = next((j for j, b in enumerate(baris_td)
                              if b == semua_baris[idx_top]), idx_top)
            baris_sel = driver.find_elements(By.XPATH, "//tr[td]")
            if idx_td < len(baris_sel):
                try:
                    tombol = baris_sel[idx_td].find_element(
                        By.XPATH,
                        ".//button[contains(text(),'Detail') or "
                        "contains(@class,'btn-water-color-blue') or "
                        "contains(@class,'btn-detail') or contains(@onclick,'detail')]"
                    )
                    return tombol, skor_top
                except NoSuchElementException:
                    pass

    # Strategi B: div/card
    elemen_nama = soup.find_all(
        string=re.compile(re.escape(nama_normal[:15]), re.IGNORECASE))
    for skor_b, el in sorted(
            [(hitung_skor(nama_normal, e.strip()), e)
             for e in elemen_nama if hitung_skor(nama_normal, e.strip()) > 0],
            key=lambda x: -x[0]):
        node = el.parent
        for _ in range(6):
            if node is None:
                break
            tombol_soup = (node.find("button", string=re.compile(r"detail", re.IGNORECASE))
                           or node.find("button", class_=re.compile(r"detail|water-color-blue", re.IGNORECASE)))
            if tombol_soup:
                semua_t = driver.find_elements(By.XPATH,
                    "//button[contains(text(),'Detail') or contains(@class,'btn-water-color-blue')]")
                kandidat_b = sorted(
                    [(hitung_skor(nama_normal,
                                  t.find_element(By.XPATH, "./ancestor::tr[1]").text
                                  if t.is_displayed() else ""), t)
                     for t in semua_t if t.is_displayed()],
                    key=lambda x: -x[0])
                if kandidat_b and kandidat_b[0][0] > 0:
                    print(f"  [Strategi B] Skor={kandidat_b[0][0]}")
                    return kandidat_b[0][1], kandidat_b[0][0]
            node = node.parent

    # Strategi C: fallback
    semua_t  = driver.find_elements(By.XPATH,
        "//button[contains(text(),'Detail') or contains(@class,'btn-water-color-blue')]")
    kandidat_c = []
    for t in semua_t:
        if not t.is_displayed():
            continue
        teks = ""
        for xp in ["./ancestor::tr[1]",
                   "./ancestor::div[contains(@class,'row')][1]", "./ancestor::li[1]"]:
            try:
                teks = t.find_element(By.XPATH, xp).text; break
            except Exception:
                pass
        skor = hitung_skor(nama_normal, teks)
        if skor > 0:
            kandidat_c.append((skor, t, teks[:50]))
    if kandidat_c:
        kandidat_c.sort(key=lambda x: -x[0])
        print(f"  [Strategi C] Skor={kandidat_c[0][0]} | '{kandidat_c[0][2]}'")
        return kandidat_c[0][1], kandidat_c[0][0]
    return None, 0


# Pagination
def hitung_total_hasil(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for el in soup.find_all(string=True):
        teks = el.strip()
        for pola in [r"of\s+([\d,\.]+)\s+entr", r"dari\s+([\d,\.]+)\s+data",
                     r"dari\s+([\d,\.]+)\s+entri", r"total\s+([\d,\.]+)",
                     r"([\d,\.]+)\s+entries"]:
            m = re.search(pola, teks, re.IGNORECASE)
            if m:
                try:
                    return int(m.group(1).replace(",", "").replace(".", ""))
                except ValueError:
                    pass
    return None


def ubah_entries_per_halaman(driver, wait, target):
    xpath = ("//select[@name='DataTables_Table_0_length' or contains(@name,'_length') "
             "or contains(@class,'entries') or contains(@aria-label,'entries')]")
    try:
        dd = driver.find_element(By.XPATH, xpath)
    except NoSuchElementException:
        dd = next((s for s in driver.find_elements(By.TAG_NAME, "select")
                   if any(o.text.strip() in ("10","25","50","100")
                          for o in s.find_elements(By.TAG_NAME, "option"))), None)
        if dd is None:
            return False
    opsi = [int(o.get_attribute("value"))
            for o in dd.find_elements(By.TAG_NAME, "option")
            if (o.get_attribute("value") or "").isdigit()]
    if not opsi:
        return False
    pilih = target if target in opsi else max(opsi)
    for o in dd.find_elements(By.TAG_NAME, "option"):
        try:
            if int(o.get_attribute("value")) == pilih:
                driver.execute_script("arguments[0].selected = true;", o)
                driver.execute_script(
                    "arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", dd)
                tunggu_loading_selesai(wait, driver)
                print(f"  [Pagination] Entries per halaman diubah ke {pilih}")
                return True
        except (ValueError, TypeError):
            pass
    return False


def _cari_elemen_next(driver):
    for xp in [
        "//a[contains(@class,'next')][not(contains(@class,'disabled'))]",
        "//li[contains(@class,'next')][not(contains(@class,'disabled'))]/a",
        "//button[contains(@class,'next')][not(@disabled)][not(contains(@class,'disabled'))]",
        "//*[contains(@id,'_next')][not(contains(@class,'disabled'))]",
    ]:
        try:
            for el in driver.find_elements(By.XPATH, xp):
                if el.is_displayed():
                    return el
        except Exception:
            continue
    for teks in ["Next", "next", "›", ">", "»", "Berikutnya"]:
        for tag in ["a", "button", "span", "li"]:
            try:
                for el in driver.find_elements(
                        By.XPATH, f"//{tag}[normalize-space(text())='{teks}']"):
                    if not el.is_displayed():
                        continue
                    kelas = el.get_attribute("class") or ""
                    pk    = ""
                    try:
                        pk = el.find_element(By.XPATH, "./..").get_attribute("class") or ""
                    except Exception:
                        pass
                    if not ("disabled" in kelas or "disabled" in pk
                            or el.get_attribute("disabled")):
                        return el
            except Exception:
                continue
    return None


def navigasi_ke_halaman_berikutnya(driver, wait):
    el = _cari_elemen_next(driver)
    if el is None:
        return False
    url_sbl = driver.current_url
    try:
        baris_sbl = driver.find_element(By.XPATH, "//table//tr[td][1]").text.strip()
    except Exception:
        baris_sbl = ""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", el)
    except Exception as e:
        print(f"  [Pagination] Gagal klik Next: {e}")
        return False
    tunggu_loading_selesai(wait, driver)
    time.sleep(1.5)
    try:
        baris_ssd = driver.find_element(By.XPATH, "//table//tr[td][1]").text.strip()
    except Exception:
        baris_ssd = ""
    if url_sbl != driver.current_url or baris_sbl != baris_ssd:
        print(f"  [Pagination] ✓ Pindah ke halaman berikutnya")
        return True
    print(f"  [Pagination] ✗ Klik Next tidak mengubah halaman")
    return False


def apakah_ada_halaman_berikutnya(driver):
    return _cari_elemen_next(driver) is not None


def atur_pagination_dan_cari(driver, wait, nama):
    total = hitung_total_hasil(driver)
    if total is None:
        if apakah_ada_halaman_berikutnya(driver):
            ubah_entries_per_halaman(driver, wait, 100)
        return total
    print(f"  [Pagination] Total hasil pencarian: {total}")
    if   total <= 10: pass
    elif total <= 25: ubah_entries_per_halaman(driver, wait, 25)
    elif total <= 50: ubah_entries_per_halaman(driver, wait, 50)
    else:             ubah_entries_per_halaman(driver, wait, 100)
    return total


# Search bar
def _hitung_hasil_saat_ini(driver):
    try:
        return sum(1 for t in driver.find_elements(By.XPATH,
            "//button[contains(text(),'Detail') or "
            "contains(@class,'btn-water-color-blue')]") if t.is_displayed())
    except Exception:
        return 0


def isi_search_bar_adaptif(driver, wait, search_box, nama):
    for i in range(len(nama.strip().split()), 0, -1):
        query = " ".join(nama.strip().split()[:i])
        search_box.send_keys(Keys.CONTROL + "a")
        search_box.send_keys(Keys.DELETE)
        search_box.send_keys(query)
        tunggu_loading_selesai(wait, driver)
        time.sleep(1.5)
        jumlah = _hitung_hasil_saat_ini(driver)
        if jumlah > 0:
            if query != nama:
                print(f"  [Search] Fallback ke '{query}' → {jumlah} hasil.")
            else:
                print(f"  [Search] Query '{query}' → {jumlah} hasil ditemukan.")
            return query
        print(f"  [Search] Query '{query}' → 0 hasil.")
    print(f"  [Search] Semua variasi gagal untuk '{nama}'.")
    return nama


XPATH_SEARCH = ("//input[@type='search' or contains(@placeholder,'Cari') "
                "or contains(@placeholder,'Search')]")
XPATH_TOMBOL = ("//button[contains(text(),'Detail') or "
                "contains(@class,'btn-water-color-blue')]")


def _navigasi_ke_halaman_daftar_dan_cari(driver, wait, nama):
    """Buka BASE_URL, isi search, return True jika ada hasil."""
    driver.get(BASE_URL)
    tunggu_loading_selesai(wait, driver)
    tutup_popup(driver)
    sb = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SEARCH)))
    isi_search_bar_adaptif(driver, wait, sb, nama)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, XPATH_TOMBOL)))
        return True
    except TimeoutException:
        print(f"  [!] Tidak ada tombol Detail untuk '{nama}'.")
        dump_diagnostik(driver, nama, "tidak_ada_tombol_detail")
        return False


# Search & Validasi Perusahaan Normal (Non-kembar)
def cari_dan_validasi(driver, wait, nama):
    if not _navigasi_ke_halaman_daftar_dan_cari(driver, wait, nama):
        return False
    atur_pagination_dan_cari(driver, wait, nama)
    dump_diagnostik(driver, nama, "setelah_pencarian")

    MAX_HALAMAN = 20
    halaman_ke  = 1
    terbaik     = (None, 0)

    while halaman_ke <= MAX_HALAMAN:
        print(f"  [Pagination] Mencari di halaman #{halaman_ke}...")
        tombol, skor = cari_tombol_detail_untuk_nama(driver, nama)
        if tombol is not None:
            if skor == 1000:
                terbaik = (tombol, skor)
                break
            if skor > terbaik[1]:
                terbaik = (tombol, skor)
        if not apakah_ada_halaman_berikutnya(driver):
            break
        if not navigasi_ke_halaman_berikutnya(driver, wait):
            break
        halaman_ke += 1

    if terbaik[0] is None:
        print(f"  [!] '{nama}' tidak ditemukan di {halaman_ke} halaman.")
        return False
    print(f"  [Pagination] Menggunakan kandidat skor={terbaik[1]}.")
    return _klik_dan_verifikasi(driver, wait, terbaik[0], nama)


def _klik_dan_verifikasi(driver, wait, tombol, nama):
    """Klik tombol Detail, tunggu halaman detail, verifikasi anti ghost-scraping."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tombol)
        time.sleep(0.5)
        try:
            tombol.click()
        except ElementClickInterceptedException:
            tutup_popup(driver)
            driver.execute_script("arguments[0].click();", tombol)
    except StaleElementReferenceException:
        print(f"  [!] Tombol menjadi stale.")
        return False
    try:
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(),'Informasi Badan Usaha')]")
        ))
    except TimeoutException:
        print(f"  [!] Halaman detail tidak muncul untuk '{nama}'.")
        return False
    tunggu_loading_selesai(wait, driver)
    dump_diagnostik(driver, nama, "halaman_detail")
    return verifikasi_halaman_detail(driver, wait, nama)


# Search & Validasi Perusahaan Kembar
def baca_baris_daftar(driver, nama):
    """
    Baca semua baris di tabel halaman DAFTAR yang cocok dengan nama.
    Kolom: Nama Badan Usaha | Jenis Badan Usaha | Jenis Perizinan | Alamat

    Layout tabel website (dari screenshot):
      No | Nama Badan Usaha | Jenis Badan Usaha | Jenis Perizinan | Alamat | Aksi

    Return: list of dict dengan kunci nama, jenis_badan_usaha, jenis_perizinan,
            alamat, dom_idx (indeks <tr[td]> di DOM).
    """
    soup        = BeautifulSoup(driver.page_source, "html.parser")
    baris_td    = [b for b in soup.find_all("tr") if b.find_all("td")]
    nama_normal = normalisasi(nama)
    hasil       = []

    for dom_idx, baris in enumerate(baris_td):
        tds = [td.get_text(separator=" ", strip=True) for td in baris.find_all("td")]
        if len(tds) < 4:
            continue
        # kolom pertama = no. (angka) → skip, ambil mulai index 1
        # kolom terakhir = aksi → skip
        idx = 1 if tds[0].strip().isdigit() or tds[0].strip() == "" else 0
        if idx + 3 >= len(tds):
            continue
        if hitung_skor(nama_normal, normalisasi(tds[idx])) < 60:
            continue
        hasil.append({
            "nama"             : normalisasi(tds[idx]),
            "jenis_badan_usaha": normalisasi(tds[idx + 1]),
            "jenis_perizinan"  : normalisasi(tds[idx + 2]),
            "alamat"           : normalisasi(tds[idx + 3]),
            "dom_idx"          : dom_idx,
        })
    return hasil


def sidik_baris_daftar(baris):
    # [R7] Tambah dom_idx ke sidik sebagai tiebreaker.
    # Sebelumnya format: "jbu|izin|alamat" (3 field, dari base script).
    # Sekarang format  : "jbu|izin|alamat|dom_idx" (4 field).
    #
    # Masalah yang dipecahkan: kembar seperti MULTI MINERAL ASIA punya
    # 2 baris di tabel daftar dengan konten kolom IDENTIK setelah normalisasi
    # (hanya beda kapital "jl." vs "Jl."). Tanpa dom_idx, sidik kedua baris
    # sama → baris ke-2 selalu dianggap sudah diambil → tidak pernah di-scrape.
    # Dengan dom_idx (posisi berbeda di DOM), sidik selalu unik per baris.
    return "|".join([baris["jenis_badan_usaha"],
                     baris["jenis_perizinan"],
                     baris["alamat"],
                     str(baris["dom_idx"])])


# [R8] Fungsi baru sidik_konten_saja().
# Sidik tanpa dom_idx — hanya 3 kolom konten (jbu|izin|alamat).
# Dipakai sebagai fallback pengecekan duplikasi saat dom_idx tidak stabil
# antar reload. Jika elemen DOM berubah saat halaman dimuat ulang,
# dom_idx bisa berbeda untuk baris yang sama. Dengan sidik_konten_saja,
# entri yang sudah diambil tetap terdeteksi meski dom_idx-nya berubah.
def sidik_konten_saja(baris):
    return "|".join([baris["jenis_badan_usaha"],
                     baris["jenis_perizinan"],
                     baris["alamat"]])


def seed_sidik_dari_file(nama):
    # [R9] Ubah return type dari set menjadi Counter.
    # Base script: return set() — tidak bisa membedakan berapa kali
    # sebuah kombinasi konten sudah ada di FILE_INFO.
    #
    # Masalah dengan set: kembar identik (konten sama persis) yang ada
    # 2 baris di FILE_INFO → seed set hanya menyimpan 1 entry → saat resume,
    # KEDUA baris identik di-skip sekaligus (seharusnya hanya 1 yang di-skip,
    # kemudian baris berikutnya dengan konten sama tetap di-proses).
    #
    # Solusi Counter: Counter{"jbu||alamat": N} menyimpan jumlah kemunculan.
    # Setiap kali skip karena seed, counter dikurangi 1 sehingga baris
    # kembar identik berikutnya tidak ikut di-skip.
    #
    # Format key: "jbu||alamat" (Jenis Perizinan dikosongkan karena tidak ada
    # di FILE_INFO; dom_idx tidak disertakan karena tidak tersimpan di CSV).
    if not os.path.exists(FILE_INFO):
        return Counter()
    try:
        df   = pd.read_csv(FILE_INFO)
        mask = df["Nama Perusahaan Asal"].astype(str).str.strip() == nama.strip()
        seed_counter = Counter()
        for _, baris in df[mask].iterrows():
            jbu  = normalisasi(str(baris.get("Jenis Badan Usaha", "")))
            almt = normalisasi(str(baris.get("Alamat", "")))
            seed_counter[f"{jbu}||{almt}"] += 1
        return seed_counter
    except Exception:
        return Counter()


# [R10] Fungsi baru _kumpulkan_kandidat_kembar().
# Base script: cari_dan_validasi_kembar() langsung melakukan pencarian dan
# klik dalam satu alur — tidak ada mekanisme truncated search, sehingga
# kembar yang salah satu namanya mengandung whitespace/newline tersembunyi
# di database website tidak pernah ditemukan.
#
# Fungsi ini memisahkan tahap PENGUMPULAN dari tahap KLIK dengan cara:
# 1. Cari dengan query penuh (nama lengkap) → kumpulkan semua kandidat.
# 2. Jika kandidat yang ditemukan kurang dari 2 sidik unik, coba truncated
#    query (N-1 kata, N-2 kata, ...) untuk menemukan kembar yang tersembunyi.
#    Contoh: query "BARA MUSTIKA ENERGINDO" hanya menemukan 1 baris, tapi
#    query "BARA MUSTIKA" menemukan 2 baris (termasuk yang namanya tersimpan
#    sebagai "BARA MUSTIKA\nENERGINDO" di database website).
# 3. Setiap baris menyimpan 'query_dipakai' agar saat reload untuk klik,
#    query yang sama digunakan (penting untuk kembar whitespace).
#
# sidik_set_persistent: set sidik yang sudah diambil sesi ini.
# Memastikan baris yang sudah diambil iterasi sebelumnya tidak masuk kandidat
# → mencegah duplikasi saat kedua kembar belum ada di FILE_INFO (fresh start).
def _kumpulkan_kandidat_kembar(driver, wait, nama, sidik_set_persistent=None):
    if sidik_set_persistent is None:
        sidik_set_persistent = set()

    kata        = nama.strip().split()
    MAX_HALAMAN = 20

    def _baca_semua_halaman(nama_query):
        if not _navigasi_ke_halaman_daftar_dan_cari(driver, wait, nama_query):
            return []
        atur_pagination_dan_cari(driver, wait, nama_query)
        hasil, hal = [], 1
        while hal <= MAX_HALAMAN:
            print(f"  [Kembar] Membaca baris halaman #{hal} (query='{nama_query}')...")
            for baris in baca_baris_daftar(driver, nama):
                baris["query_dipakai"] = nama_query
                sk = sidik_baris_daftar(baris)
                hasil.append((baris["dom_idx"], hal, sk, baris))
                print(f"    DOM#{baris['dom_idx']} "
                      f"JBU='{baris['jenis_badan_usaha'][:20]}' | "
                      f"Izin='{baris['jenis_perizinan']}' | "
                      f"Alamat='{baris['alamat'][:25]}'")
            if not apakah_ada_halaman_berikutnya(driver):
                break
            if not navigasi_ke_halaman_berikutnya(driver, wait):
                break
            hal += 1
        return hasil

    def _sudah_diambil(sk, baris):
        # Cek dua level:
        # 1. sk lengkap (jbu|izin|alamat|dom_idx) — exact match
        # 2. sidik_konten_saja (tanpa dom_idx) — fallback jika dom_idx tidak stabil
        if sk in sidik_set_persistent:
            return True
        return sidik_konten_saja(baris) in sidik_set_persistent

    # Kumpulkan dengan query penuh
    kandidat        = _baca_semua_halaman(nama)
    sidik_diketahui = {sk for _, _, sk, _ in kandidat}

    # Buang yang sudah pernah diambil di iterasi sebelumnya
    kandidat = [(d, h, sk, b) for d, h, sk, b in kandidat
                if not _sudah_diambil(sk, b)]

    # Truncated search: cari kembar yang tidak muncul dengan query penuh
    for i in range(len(kata) - 1, 0, -1):
        query_pendek  = " ".join(kata[:i])
        kandidat_baru = _baca_semua_halaman(query_pendek)
        tambahan = [
            (d, h, sk, b) for d, h, sk, b in kandidat_baru
            if sk not in sidik_diketahui      # belum ditemukan di collect ini
            and not _sudah_diambil(sk, b)     # belum diambil di sesi ini
        ]
        if tambahan:
            print(f"  [Kembar] Query pendek '{query_pendek}' menemukan "
                  f"{len(tambahan)} baris tambahan.")
            kandidat.extend(tambahan)
            sidik_diketahui.update(sk for _, _, sk, _ in tambahan)
        if len(sidik_diketahui) > 1:
            break

    return kandidat


def cari_dan_validasi_kembar(driver, wait, nama, sidik_sudah_diambil):
    # [R11] Fungsi ini ditulis ulang total dari base script.
    #
    # Base script:
    # - Satu alur cari + klik, tanpa _kumpulkan_kandidat_kembar()
    # - Parameter sidik_sudah_diambil adalah set biasa
    # - Tidak ada Counter seed — resume tidak bisa bedakan kembar identik
    # - Tidak ada dom_idx di sidik — kembar konten identik tidak bisa dibedakan
    # - Tidak ada truncated search — kembar whitespace tidak ditemukan
    # - Tidak ada cek fallback sidik_konten — dom_idx tidak stabil tidak ditangani
    #
    # Versi baru:
    # - parameter sidik_sudah_diambil = tuple (set_sidik, counter_seed)
    # - Gunakan _kumpulkan_kandidat_kembar() [R10] untuk kumpulkan kandidat
    # - Pass sidik_set ke _kumpulkan agar baris yang sudah diambil tidak masuk
    #   kandidat lagi → mencegah duplikasi fresh start
    # - Pengecekan 3 lapis: (1a) sk lengkap, (1b) sk konten fallback, (2) Counter seed
    # - Setelah klik: simpan sk, sk_seed, DAN sk_konten ke sidik_set

    # Ambil komponen tuple
    sidik_set, seed_counter = sidik_sudah_diambil

    # Pass sidik_set ke _kumpulkan untuk filter baris yang sudah diambil
    semua_kandidat = _kumpulkan_kandidat_kembar(driver, wait, nama, sidik_set)

    if not semua_kandidat:
        print(f"  [!] Tidak ada baris cocok untuk '{nama}'.")
        return False

    # Log informatif
    sidik_konten_unik = {sidik_konten_saja(b) for _, _, _, b in semua_kandidat}
    if len(semua_kandidat) == 1:
        print(f"  [Kembar] Peringatan: hanya 1 baris ditemukan. "
              f"Baris kembar lainnya mungkin tidak muncul di hasil pencarian.")
    elif len(sidik_konten_unik) == 1:
        print(f"  [Kembar] Semua {len(semua_kandidat)} baris punya konten identik "
              f"— dibedakan hanya via dom_idx.")

    for dom_idx, hal_k, sk, baris_dict in semua_kandidat:
        print(f"\n  [Kembar] Coba DOM#{dom_idx} hal={hal_k} | '{sk[:60]}'")

        sk_seed   = f"{baris_dict['jenis_badan_usaha']}||{baris_dict['alamat']}"
        sk_konten = sidik_konten_saja(baris_dict)

        # Cek 1a: sidik lengkap (jbu|izin|alamat|dom_idx)
        if sk in sidik_set:
            print(f"  [Kembar] Sudah diambil (sidik lengkap) → skip.")
            continue

        # Cek 1b: sidik konten saja (tanpa dom_idx)
        # Fallback jika dom_idx berubah antar reload
        if sk_konten in sidik_set:
            print(f"  [Kembar] Sudah diambil (sidik konten, dom_idx berbeda) → skip.")
            continue

        # Cek 2: Counter seed dari FILE_INFO (resume)
        # Dikurangi 1 per skip agar kembar identik berikutnya tidak ikut di-skip
        if seed_counter.get(sk_seed, 0) > 0:
            seed_counter[sk_seed] -= 1
            print(f"  [Kembar] Sudah diambil (seed resume, sisa={seed_counter[sk_seed]}) → skip.")
            continue

        # Belum diambil — reload dengan query yang sama saat collect
        query_dipakai = baris_dict.get("query_dipakai", nama)
        if not _navigasi_ke_halaman_daftar_dan_cari(driver, wait, query_dipakai):
            continue
        atur_pagination_dan_cari(driver, wait, query_dipakai)
        for _ in range(hal_k - 1):
            if not navigasi_ke_halaman_berikutnya(driver, wait):
                break

        baris_sel = driver.find_elements(By.XPATH, "//tr[td]")
        if dom_idx >= len(baris_sel):
            print(f"  [!] DOM#{dom_idx} tidak ada ({len(baris_sel)} tersedia). Skip.")
            continue
        try:
            tombol = baris_sel[dom_idx].find_element(
                By.XPATH,
                ".//button[contains(text(),'Detail') or "
                "contains(@class,'btn-water-color-blue') or "
                "contains(@class,'btn-detail') or contains(@onclick,'detail')]"
            )
        except NoSuchElementException:
            print(f"  [!] Tombol tidak ada di DOM#{dom_idx}. Skip.")
            continue

        if _klik_dan_verifikasi(driver, wait, tombol, nama):
            sidik_set.add(sk)           # catat sidik lengkap (dengan dom_idx)
            sidik_set.add(sk_seed)      # catat sk_seed untuk pencocokan resume
            sidik_set.add(sk_konten)    # catat sidik konten (tanpa dom_idx)
                                        # → cegah duplikasi jika dom_idx berubah
            return True

    print(f"  [!] Semua {len(semua_kandidat)} kandidat sudah diambil / tidak valid.")
    return False


# Parsing

# [R6] Perbaiki bug di base script: baris 'def parse_info(soup, nama):'
# hilang dari base script yang dikirim. Di base, bagian "# Parsing" langsung
# dilanjutkan dengan body fungsi (info_dict = ...) tanpa ada deklarasi def
# di atasnya — ini akan menyebabkan SyntaxError saat script dijalankan.
# Baris def ditambahkan kembali di sini.
def parse_info(soup, nama):
    info_dict = {'Nama Perusahaan Asal': nama}
    for kunci in KUNCI_PENCARIAN:
        pola  = re.compile(rf"^\s*{re.escape(kunci)}\s*:?$", re.IGNORECASE)
        label = soup.find(string=pola)
        if label:
            nilai = None
            node  = label
            for _ in range(5):
                node = node.parent
                if not node:
                    break
                sib = node.find_next_sibling()
                if sib:
                    t = sib.get_text(separator=" ", strip=True)
                    if t and t != ':' and t.lower().strip() not in NILAI_BLACKLIST:
                        nilai = t
                        break
            if nilai is None:
                container = label.parent
                for _ in range(3):
                    if container:
                        container = container.parent
                batas = 0
                for kand in label.find_all_next(string=True):
                    batas += 1
                    if batas > 10:
                        break
                    t = kand.strip()
                    if not t or t == ':':
                        continue
                    if t.lower().strip() in NILAI_BLACKLIST:
                        break
                    if container and not container.find(string=lambda s: s == kand):
                        break
                    nilai = t
                    break
            nilai = nilai if nilai is not None else "-"
            if kunci in FORMAT_NILAI and nilai != "-":
                if not FORMAT_NILAI[kunci].match(nilai.strip()):
                    nilai = "-"
            info_dict[kunci] = (f"'{nilai}" if kunci in ("NPWP", "RT/RW")
                                and nilai != "-" else nilai)
        else:
            info_dict[kunci] = "-"
    return info_dict


# [R13] Fungsi baru _tabel_isi_kosong().
# Website menampilkan tabel tanpa data dengan struktur:
#   <thead> → header kolom valid (Nama Direksi, Jabatan, dll)
#   <tbody><tr><td colspan="N">Tidak ada data</td></tr></tbody>
#
# pd.read_html() membaca ini sebagai DataFrame dengan kolom header valid
# (lolos deteksi _KUNCI_*) tapi isi seluruh baris hanya teks sentinel.
# Tanpa filter ini, baris "tidak ada data" tersimpan ke CSV dengan
# Nama Perusahaan Asal = nama perusahaan yang valid — ini adalah noise.
#
# Return True jika semua nilai non-NaN hanya berisi teks sentinel → skip.
def _tabel_isi_kosong(df):
    # [P1-A] Perluas sentinel: tambah varian teks kosong yang dipakai website
    # selain "Tidak ada data". Sebelumnya hanya 5 string — celah: website
    # bisa menampilkan "No data available", "Belum ada data", dll yang tidak
    # tercakup → tabel kosong lolos filter → tersimpan ke CSV.
    #
    # [P1-B] Tambah cek rasio: jika >80% nilai non-NaN di tabel adalah sentinel,
    # tabel dianggap kosong. Ini menangani kasus tabel yang punya 1 baris data
    # valid di header tapi seluruh tbody berisi sentinel dengan format campuran.
    df_bersih = df.dropna(how='all')
    if df_bersih.empty:
        return True
    semua_nilai = [
        str(v).strip().lower()
        for v in df_bersih.values.flatten()
        if pd.notnull(v) and str(v).strip() != ''
    ]
    if not semua_nilai:
        return True
    # [P1-A] Sentinel diperluas — tambah varian bahasa Inggris dan variasi lain
    sentinel_tabel = {
        'tidak ada data', 'tidak ada', '-', 'memuat...', 'loading...',
        'no data available', 'no data', 'data tidak tersedia',
        'belum ada data', 'data tidak ditemukan', 'n/a', 'none',
    }
    # Cek exact: semua nilai adalah sentinel
    if all(v in sentinel_tabel for v in semua_nilai):
        return True
    # [P1-B] Cek rasio: jika tabel punya >=1 kolom DAN >80% nilai adalah sentinel
    # atau NaN → tabel dianggap kosong meski ada beberapa nilai non-sentinel
    jumlah_sentinel = sum(1 for v in semua_nilai if v in sentinel_tabel)
    if len(semua_nilai) > 0 and jumlah_sentinel / len(semua_nilai) >= 0.8:
        return True
    return False


# [R14] Tiga perubahan di parse_tabel():
#
# 1. _KUNCI_* diperluas dari string literal ke tuple kata kunci.
#    Base: 'nama direksi' in cols OR 'jabatan' in cols (hanya 2 kata kunci per tipe)
#    Baru: any(k in cols for k in _KUNCI_DIREKSI) dengan tuple lebih lengkap.
#    Lebih toleran terhadap variasi nama kolom di website.
#
# 2. Filter _tabel_isi_kosong(df) dipanggil setelah baca kolom, SEBELUM
#    append ke data_d/s/p. Tabel website yang hanya berisi "Tidak ada data"
#    dibuang sebelum masuk ke penyimpanan CSV.
#
# 3. Blok except: sebelumnya 'pass' diam-diam. Sekarang mencetak pesan error
#    agar tidak silent — membantu debug jika ada tabel yang gagal di-parse.
_KUNCI_DIREKSI   = ('nama direksi', 'jabatan', 'direksi', 'komisaris',
                    'nama direktur', 'nama komisaris')
_KUNCI_SAHAM     = ('persentase saham', 'jenis kepemilikan', 'pemegang saham',
                    'nama pemegang', 'persentase')
_KUNCI_PERIZINAN = ('nomor izin', 'tahap kegiatan', 'modi id', 'kode wiup',
                    'jenis izin', 'status cnc')


def parse_tabel(soup, nama, kode_bu="-"):
    # [P4] Tambah parameter kode_bu (Kode Badan Usaha dari parse_info).
    # Diinsert ke setiap baris tabel sebagai kolom ke-2 setelah Nama Perusahaan Asal.
    # Memungkinkan dedup DIREKSI/SAHAM/PERIZINAN membedakan kembar identik nama
    # (contoh: "MULTI MINERAL ASIA" × 2 di master) berdasarkan kode unik mereka.
    # Tanpa ini, dedup hanya melihat nama+direksi → membuang data kembar ke-2
    # jika direksinya sama persis dengan kembar ke-1.
    data_d, data_s, data_p = [], [], []
    for t in soup.find_all("table"):
        try:
            df = pd.read_html(io.StringIO(str(t)))[0]
            if df.empty or "Memuat..." in df.to_string():
                continue
            df.columns = [str(c).lower().strip() for c in df.columns]
            # [R14-2] Filter tabel kosong SEBELUM disimpan ke data_d/s/p
            if _tabel_isi_kosong(df):
                continue
            if 'jenis komoditas' in df.columns:
                df.rename(columns={'jenis komoditas': 'komoditas'}, inplace=True)
            df.insert(0, 'Nama Perusahaan Asal', nama)
            cols = str(df.columns.tolist())
            # [R14-1] Deteksi tipe tabel pakai any() + tuple kata kunci
            if any(k in cols for k in _KUNCI_DIREKSI):
                data_d.append(df)
            elif any(k in cols for k in _KUNCI_SAHAM):
                data_s.append(df)
            elif any(k in cols for k in _KUNCI_PERIZINAN):
                for col in df.columns:
                    if any(k in str(col) for k in ('izin', 'wiup', 'modi id')):
                        df[col] = df[col].apply(
                            lambda x: f"'{x}" if pd.notnull(x) else x)
                data_p.append(df)
            # else: tabel tidak dikenal → diabaikan
        except Exception as e:
            # [R14-3] Log error — sebelumnya: except: pass
            print(f"  [parse_tabel] Tabel gagal di-parse untuk '{nama}': {e}")
    return data_d, data_s, data_p


# Scraping satu perusahaan
def _buat_placeholder(nama, kode_bu, kolom_standar):
    # [P5] Buat 1 baris DataFrame placeholder untuk perusahaan yang websitenya
    # memang tidak menampilkan data (tabel kosong / "Tidak ada data" di website).
    # Placeholder = Nama valid + Kode valid + semua kolom data = 'Tidak ada data'.
    # Ini berbeda dari baris TAD lama (yang Nama-nya juga TAD) — baris ini
    # sengaja dibuat agar perusahaan tetap terwakili di semua file CSV,
    # sehingga mudah diketahui bahwa scraping sudah berjalan untuk perusahaan itu
    # dan memang tidak ada data di website.
    row = {c: 'Tidak ada data' for c in kolom_standar}
    row['Nama Perusahaan Asal'] = nama
    return pd.DataFrame([row])


def _simpan_hasil(driver, nama):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    # [P4] parse_info() dulu → ambil Kode Badan Usaha → pass ke parse_tabel
    info = parse_info(soup, nama)
    penyimpanan(FILE_INFO, [info],
                kunci_dedup=["Nama Perusahaan Asal", "NPWP", "Kode Badan Usaha"])
    kode_bu = str(info.get("Kode Badan Usaha", "-")).strip()
    data_d, data_s, data_p = parse_tabel(soup, nama, kode_bu)

    # [P5] Jika tabel tertentu kosong (website tidak punya data / tampilkan "Tidak ada data"),
    # parse_tabel() mengembalikan list kosong untuk tipe itu.
    # Simpan 1 baris placeholder agar perusahaan tetap terwakili di file CSV.
    # Placeholder hanya disimpan jika df_old belum punya baris apapun untuk perusahaan ini
    # — hal ini dijamin oleh dedup di penyimpanan() menggunakan kunci yang mencakup
    # Nama Perusahaan Asal + Kode Badan Usaha.
    if not data_d:
        data_d = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_DIREKSI)]
    if not data_s:
        data_s = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_SAHAM)]
    if not data_p:
        data_p = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_PERIZINAN)]

    # [P4→P6] Kode Badan Usaha dihapus dari kunci dedup tabel (tidak disimpan di file tabel).
    # Kunci berbasis konten sudah cukup mencegah duplikasi:
    # kembar identik nama dengan direksi/saham/izin sama persis → 1 copy tersimpan (benar).
    # kembar dengan data berbeda → sidik berbeda → keduanya tersimpan (benar).
    penyimpanan(FILE_DIREKSI,   data_d,
                kunci_dedup=["Nama Perusahaan Asal", "nama direksi", "jabatan"])
    penyimpanan(FILE_SAHAM,     data_s,
                kunci_dedup=["Nama Perusahaan Asal", "nama", "persentase saham"])
    penyimpanan(FILE_PERIZINAN, data_p,
                kunci_dedup=["Nama Perusahaan Asal", "nomor izin", "modi id"])


def scrape_perusahaan(driver, wait, nama):
    """Non-kembar: pakai cari_dan_validasi biasa (tidak berubah dari versi asli)."""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            if cari_dan_validasi(driver, wait, nama):
                _simpan_hasil(driver, nama)
                return True
            if attempt < MAX_RETRY:
                print(f"  [Percobaan {attempt}/{MAX_RETRY}] Retry dalam 5s...")
                time.sleep(5)
        except (TimeoutException, WebDriverException) as e:
            print(f"  [Percobaan {attempt}/{MAX_RETRY}] {type(e).__name__}.")
            time.sleep(5 * attempt)
        except Exception as e:
            print(f"  [Percobaan {attempt}/{MAX_RETRY}] Error: {e}")
            time.sleep(5 * attempt)
    return False


def scrape_perusahaan_kembar(driver, wait, nama, sidik_sudah_diambil):
    # [R12] Perubahan tipe parameter sidik_sudah_diambil dari set ke tuple.
    # Sebelumnya: sidik_sudah_diambil adalah set sidik jari langsung.
    # Sekarang  : tuple (set_sidik_lengkap, counter_seed).
    # Logika di fungsi ini tidak berubah — ia hanya meneruskan tuple ke
    # cari_dan_validasi_kembar() yang membongkar dan menggunakan isinya.
    """
    Kembar: bedakan entri berdasarkan sidik jari kolom tabel halaman daftar.
    sidik_sudah_diambil adalah tuple (set_sidik_lengkap, counter_seed):
      - set_sidik_lengkap: set sidik "jbu|izin|alamat|dom_idx", diupdate in-place
      - counter_seed     : Counter resume dari FILE_INFO, dikurangi in-place
    """
    for attempt in range(1, MAX_RETRY + 1):
        try:
            if cari_dan_validasi_kembar(driver, wait, nama, sidik_sudah_diambil):
                _simpan_hasil(driver, nama)
                return True
            if attempt < MAX_RETRY:
                print(f"  [Percobaan {attempt}/{MAX_RETRY}] Retry dalam 5s...")
                time.sleep(5)
        except (TimeoutException, WebDriverException) as e:
            print(f"  [Percobaan {attempt}/{MAX_RETRY}] {type(e).__name__}.")
            time.sleep(5 * attempt)
        except Exception as e:
            print(f"  [Percobaan {attempt}/{MAX_RETRY}] Error: {e}")
            time.sleep(5 * attempt)
    return False


# Main
def baca_counter_selesai():
    counter = Counter()
    if os.path.exists(FILE_INFO):
        try:
            df = pd.read_csv(FILE_INFO)
            if 'Nama Perusahaan Asal' in df.columns:
                for n in df['Nama Perusahaan Asal'].dropna().astype(str):
                    # [S1] Sanitasi nama dari CSV lama agar cocok dengan nama
                    # bersih dari master. CSV yang dibuat sebelum patch S1 bisa
                    # punya nama dengan \t (misal 'BARA MUSTIKA\tENERGINDO').
                    # Tanpa sanitasi di sini, nama kotor di CSV tidak akan
                    # ditemukan di counter → perusahaan di-scrape ulang padahal
                    # sudah selesai.
                    counter[sanitasi_nama(n.strip())] += 1
        except Exception:
            pass
    return counter


def main():
    if MODE_DIAGNOSTIK:
        print(f"{'='*60}\nMODE DIAGNOSTIK — {DIAG_MAX_SAMPEL} sampel → '{FILE_DIAG}'\n{'='*60}\n")

    try:
        df_master = pd.read_csv(FILE_MASTER)
    except FileNotFoundError:
        print(f"[ERROR] '{FILE_MASTER}' tidak ditemukan.")
        return

    semua_perusahaan = df_master['Nama Badan Usaha'].dropna().astype(str).tolist()

    # [S1] Sanitasi nama dari master: bersihkan tab/newline tersembunyi.
    # Dilakukan di sini — titik masuk tunggal — sehingga seluruh alur di bawahnya
    # (scraping, penyimpanan, deteksi kembar, resume) semuanya menerima nama bersih.
    semua_perusahaan = [sanitasi_nama(n) for n in semua_perusahaan]

    # [R15] Ubah deteksi kembar dari Counter sederhana ke nospace_grup.
    # Base script:
    #   kebutuhan = Counter(n.strip() for n in semua_perusahaan)
    #   kembar    = {n for n, k in kebutuhan.items() if k > 1}
    # → hanya mendeteksi nama yang identik PERSIS (termasuk spasi).
    #
    # Masalah: kembar yang penulisan spasinya berbeda di master CSV
    # (misalnya "BARA MUSTIKA ENERGINDO" vs "BARA MUSTIKAENERGINDO") tidak
    # terdeteksi sebagai kembar → diperlakukan sebagai perusahaan biasa
    # → masing-masing scrape sendiri tanpa sidik jari → bisa duplikasi.
    #
    # Solusi nospace_grup:
    # nospace_grup[nk] = semua varian nama dengan nospace key yang sama.
    # nospace_counter[nk] = total kemunculan di master (semua varian).
    # Kembar jika: (a) total > 1 kali di master, ATAU
    #              (b) ada lebih dari 1 varian penulisan yang berbeda.
    nospace_grup    = defaultdict(set)
    for n in semua_perusahaan:
        nospace_grup[nospace(n.strip())].add(n.strip())

    nospace_counter = Counter(nospace(n.strip()) for n in semua_perusahaan)

    kembar = set()
    for nk, jumlah in nospace_counter.items():
        if jumlah > 1 or len(nospace_grup[nk]) > 1:
            kembar.update(nospace_grup[nk])

    if kembar:
        print(f"Perusahaan kembar terdeteksi ({len(kembar)} nama):")
        for n in sorted(kembar):
            print(f"  '{n}' — total {nospace_counter[nospace(n)]}x "
                  f"(varian: {nospace_grup[nospace(n)]})")
        print()

    sudah_selesai  = baca_counter_selesai()
    sudah_diklaim  = Counter()
    daftar_antrean = []
    for n in semua_perusahaan:
        nk = n.strip()
        if sudah_diklaim[nk] < sudah_selesai[nk]:
            sudah_diklaim[nk] += 1
        else:
            daftar_antrean.append(nk)

    if not daftar_antrean:
        print("Semua data sudah selesai di scraping.")
        return

    print(f"Memulai scraping — sisa {len(daftar_antrean)} entri.")
    driver, wait = init_driver()
    sesi_counter = 0
    total_sukses = 0
    total_gagal  = 0

    # [P2] Ganti sidik_per_nama (kunci = nama asli) dengan sidik_per_nospace
    # (kunci = nospace key grup).
    #
    # MASALAH DI v6: sidik_per_nama diindeks per nama asli.
    # Jika satu grup kembar punya varian nama berbeda penulisan spasi, contoh:
    #   "BARA MUSTIKA ENERGINDO" dan "BARA MUSTIKAENERGINDO"
    # keduanya punya entry terpisah di sidik_per_nama:
    #   sidik_per_nama["BARA MUSTIKA ENERGINDO"]  = (set_A, counter_A)
    #   sidik_per_nama["BARA MUSTIKAENERGINDO"]    = (set_B, counter_B)
    # set_A dan set_B BERBEDA → iterasi pertama mengisi set_A,
    # iterasi kedua mulai dengan set_B KOSONG → klik halaman yang sama → DUPLIKAT.
    #
    # SOLUSI: gunakan nospace key sebagai kunci → semua varian nama dalam
    # satu grup berbagi SATU sidik_set dan SATU seed_counter yang sama.
    # Seed di-merge dari semua varian nama dalam grup (untuk support resume).
    sidik_per_nospace = {}
    for nk, varian_set in nospace_grup.items():
        if nospace_counter[nk] > 1 or len(varian_set) > 1:
            seed = Counter()
            for v in varian_set:
                seed.update(seed_sidik_dari_file(v))
            sidik_per_nospace[nk] = (set(), seed)

    for nama in daftar_antrean:
        sesi_counter += 1
        print(f"\n[{sesi_counter}/{len(daftar_antrean)}] Scraping: {nama}")

        if sesi_counter % RESTART_INTERVAL == 0:
            print("  [i] Restarting Chrome...")
            try:
                driver.quit()
            except Exception:
                pass
            time.sleep(3)
            driver, wait = init_driver()

        if nama in kembar:
            # [P2] Lookup via nospace key → semua varian nama dalam satu grup
            # berbagi sidik_set yang sama → mencegah duplikasi antar varian
            nk_nama    = nospace(nama)
            sidik_tuple = sidik_per_nospace[nk_nama]
            berhasil    = scrape_perusahaan_kembar(driver, wait, nama, sidik_tuple)
        else:
            berhasil = scrape_perusahaan(driver, wait, nama)

        if berhasil:
            print(f"  [✓] Berhasil disimpan")
            total_sukses += 1
        else:
            print(f"  [✗] Gagal setelah {MAX_RETRY} percobaan")
            with open(FILE_ERROR, "a", encoding="utf-8") as f:
                f.write(f"{nama}\n")
            total_gagal += 1

        if MODE_DIAGNOSTIK and sesi_counter >= DIAG_MAX_SAMPEL:
            print(f"\n[DIAG] Selesai. Buka '{FILE_DIAG}', lalu ubah MODE_DIAGNOSTIK = False")
            break

    try:
        driver.quit()
    except Exception:
        pass

    print(f"\n{'='*50}\nScraping selesai\n"
          f"  Berhasil : {total_sukses}\n"
          f"  Gagal    : {total_gagal} (lihat '{FILE_ERROR}')\n{'='*50}")


if __name__ == "__main__":
    main()