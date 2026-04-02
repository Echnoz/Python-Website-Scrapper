import os
import io
import re
import time
import math
import decimal
import pandas as pd
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

#Config
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

MODE_DIAGNOSTIK  = False #True untuk masuk ke mode diagnostik/testing
DIAG_MAX_SAMPEL  = 3

KOLOM_STANDAR_DIREKSI = [
    'Nama Perusahaan Asal', 'Kode Badan Usaha',
    'no.', 'nama direksi', 'mulai menjabat', 'akhir menjabat', 'jabatan',
]
KOLOM_STANDAR_SAHAM = [
    'Nama Perusahaan Asal', 'Kode Badan Usaha',
    'no.', 'jenis kepemilikan', 'nama', 'kewarganegaraan',
    'asal negara', 'persentase saham',
]
KOLOM_STANDAR_PERIZINAN = [
    'Nama Perusahaan Asal', 'Kode Badan Usaha',
    'no', 'nomor izin', 'jenis izin', 'tahap kegiatan', 'golongan', 'komoditas',
    'luas (ha)', 'tanggal berlaku', 'tanggal berakhir', 'status cnc',
    'lokasi', 'kode wiup', 'modi id',
]

KUNCI_PENCARIAN = [
    "Nama Badan Usaha", "Kode Badan Usaha", "Jenis Badan Usaha",
    "Kelurahan", "NPWP", "RT/RW", "Kode Pos", "Alamat"
]

FORMAT_NILAI = {
    "NPWP"            : re.compile(r"^[\d\.\-\*\/]{5,25}$"),
    "Kode Badan Usaha": re.compile(r"^\d+$"),
    "Kode Pos"        : re.compile(r"^\d{3,6}$"),
    "RT/RW"           : re.compile(r"^[\d\-]+\s*/\s*[\d\-]+$|^-$"),
    "Jenis Badan Usaha": re.compile(
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

KODE_BU_BATAS_DIGIT = 15

KOLOM_NUMERIK_SAHAM = ('persentase saham', 'persentase')

#Utility
def normalisasi(teks):
    return re.sub(r'\s+', ' ', str(teks).strip().lower())


def nospace(teks):
    return re.sub(r'\s+', '', str(teks).strip().lower())


def sanitasi_nama(nama):
    nama = re.sub(r'[\t\n\r]+', ' ', str(nama))
    nama = nama.strip()
    nama = re.sub(r' {2,}', ' ', nama)
    return nama


def lindungi_kode_bu(nilai):
    if nilai in ("-", "", None):
        return nilai
    bersih = str(nilai).strip()
    if re.match(r'^\d+$', bersih) and len(bersih) > KODE_BU_BATAS_DIGIT:
        return f"'{bersih}"
    return bersih


def format_angka_aman(nilai):
    if nilai is None:
        return nilai
    try:
        if isinstance(nilai, float) and math.isnan(nilai):
            return nilai
    except Exception:
        pass
    try:
        s = str(nilai).strip()
        if not s or s == '-':
            return nilai
        f = float(s)
        d = decimal.Decimal(str(f))
        hasil = format(d, 'f')
        if '.' in hasil:
            hasil = hasil.rstrip('0').rstrip('.')
        return hasil
    except (ValueError, TypeError, decimal.InvalidOperation):
        return nilai


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


def tunggu_tabel_detail_selesai(driver, timeout=30):
    sentinel_loading = ('memuat...', 'loading...')
    for _ in range(timeout * 2):  #cek setiap 0.5 detik
        time.sleep(0.5)
        try:
            soup   = BeautifulSoup(driver.page_source, "html.parser")
            tabel_list = soup.find_all("table")
            if not tabel_list:
                continue
            masih_loading = False
            for tabel in tabel_list:
                teks_tabel = tabel.get_text(separator=" ", strip=True).lower()
                if any(s in teks_tabel for s in sentinel_loading):
                    masih_loading = True
                    break
            if not masih_loading:
                return  #semua tabel sudah selesai dimuat
        except Exception:
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

#Penyimpanan
def _bersih_tad(df):
    if 'Nama Perusahaan Asal' not in df.columns:
        return df
    mask_valid = df['Nama Perusahaan Asal'].astype(str).str.strip().str.lower() != 'tidak ada data'
    return df[mask_valid].copy()


def _fillna_aman(df):
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
    if 'Nama Perusahaan Asal' in df_new.columns:
        df_new = df_new[df_new['Nama Perusahaan Asal'].astype(str).str.strip().str.lower() != 'tidak ada data']
        df_new = df_new[df_new['Nama Perusahaan Asal'].notna()]
    if df_new.empty:
        return
    df_old = pd.DataFrame()
    if os.path.exists(nama_file):
        try:
            dtype_override = {}
            if nama_file == FILE_INFO:
                dtype_override['Kode Badan Usaha'] = str
            df_old = pd.read_csv(nama_file, dtype=dtype_override)
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
    _fillna_aman(df_final).to_csv(nama_file, index=False)

#Verifikasi halaman detail (anti ghosts-scraping)
def verifikasi_halaman_detail(driver, wait, nama_target):
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
        cocok = normalisasi(nilai) == normalisasi(nama_target)
        if not cocok:
            cocok = nospace(nilai) == nospace(nama_target)
        if not cocok:
            print(f"  [✗] GHOST: target='{nama_target}' | halaman='{nilai}'")
        return cocok
    except Exception as e:
        print(f"  [!] Error verifikasi: {e}")
        return False

#Finder tombol detail
def semua_kata_ada(nama_target, teks_baris):
    return all(k in teks_baris.lower() for k in nama_target.lower().split())


def hitung_skor(nama_normal, teks_kandidat):
    teks   = normalisasi(teks_kandidat)
    nama_n = normalisasi(nama_normal)
    if not teks:
        return 0
    rasio = len(nama_n) / len(teks)
    if teks == nama_n:                    return 1000
    if nospace(teks) == nospace(nama_n):  return 950
    if teks.startswith(nama_n + " "):     return int(200 * rasio)
    if nama_n in teks:                    return int(150 * rasio)
    if semua_kata_ada(nama_n, teks):      return int(100 * rasio)
    return 0


def cari_tombol_detail_untuk_nama(driver, nama):
    nama_normal = nama.strip().lower()
    soup        = BeautifulSoup(driver.page_source, "html.parser")

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

#Pagination
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

#Search bar
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

#Search dan validasi perusahaan normal (non-kembar)
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
    tunggu_tabel_detail_selesai(driver)
    dump_diagnostik(driver, nama, "halaman_detail")
    return verifikasi_halaman_detail(driver, wait, nama)

#Search dan validasi perusahaan kembar
def baca_baris_daftar(driver, nama):
    soup        = BeautifulSoup(driver.page_source, "html.parser")
    baris_td    = [b for b in soup.find_all("tr") if b.find_all("td")]
    nama_normal = normalisasi(nama)
    hasil       = []

    for dom_idx, baris in enumerate(baris_td):
        tds = [td.get_text(separator=" ", strip=True) for td in baris.find_all("td")]
        if len(tds) < 4:
            continue
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
    return "|".join([baris["jenis_badan_usaha"],
                     baris["jenis_perizinan"],
                     baris["alamat"],
                     str(baris["dom_idx"])])


def sidik_konten_saja(baris):
    return "|".join([baris["jenis_badan_usaha"],
                     baris["jenis_perizinan"],
                     baris["alamat"]])


def seed_sidik_dari_file(nama):
    if not os.path.exists(FILE_INFO):
        return Counter()
    try:
        df   = pd.read_csv(FILE_INFO, dtype={'Kode Badan Usaha': str})
        mask = df["Nama Perusahaan Asal"].astype(str).str.strip() == nama.strip()
        seed_counter = Counter()
        for _, baris in df[mask].iterrows():
            jbu  = normalisasi(str(baris.get("Jenis Badan Usaha", "")))
            almt = normalisasi(str(baris.get("Alamat", "")))
            seed_counter[f"{jbu}||{almt}"] += 1
        return seed_counter
    except Exception:
        return Counter()


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
        if sk in sidik_set_persistent:
            return True
        return sidik_konten_saja(baris) in sidik_set_persistent

    kandidat        = _baca_semua_halaman(nama)
    sidik_diketahui = {sk for _, _, sk, _ in kandidat}

    kandidat = [(d, h, sk, b) for d, h, sk, b in kandidat
                if not _sudah_diambil(sk, b)]

    for i in range(len(kata) - 1, 0, -1):
        query_pendek  = " ".join(kata[:i])
        kandidat_baru = _baca_semua_halaman(query_pendek)
        tambahan = [
            (d, h, sk, b) for d, h, sk, b in kandidat_baru
            if sk not in sidik_diketahui
            and not _sudah_diambil(sk, b)
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
    sidik_set, seed_counter = sidik_sudah_diambil

    semua_kandidat = _kumpulkan_kandidat_kembar(driver, wait, nama, sidik_set)

    if not semua_kandidat:
        print(f"  [!] Tidak ada baris cocok untuk '{nama}'.")
        return False

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

        if sk in sidik_set:
            print(f"  [Kembar] Sudah diambil (sidik lengkap) → skip.")
            continue

        if sk_konten in sidik_set:
            print(f"  [Kembar] Sudah diambil (sidik konten, dom_idx berbeda) → skip.")
            continue

        if seed_counter.get(sk_seed, 0) > 0:
            seed_counter[sk_seed] -= 1
            print(f"  [Kembar] Sudah diambil (seed resume, sisa={seed_counter[sk_seed]}) → skip.")
            continue

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
            sidik_set.add(sk)
            sidik_set.add(sk_seed)
            sidik_set.add(sk_konten)
            return True

    print(f"  [!] Semua {len(semua_kandidat)} kandidat sudah diambil / tidak valid.")
    return False

#Parsing
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

            if kunci == "Kode Badan Usaha" and nilai != "-":
                nilai = lindungi_kode_bu(nilai)
            elif kunci in ("NPWP", "RT/RW") and nilai != "-":
                nilai = f"'{nilai}"

            info_dict[kunci] = nilai
        else:
            info_dict[kunci] = "-"
    return info_dict


def _tabel_isi_kosong(df):
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
    sentinel_tabel = {
        'tidak ada data', 'tidak ada', '-', 'memuat...', 'loading...',
        'no data available', 'no data', 'data tidak tersedia',
        'belum ada data', 'data tidak ditemukan', 'n/a', 'none',
    }
    if all(v in sentinel_tabel for v in semua_nilai):
        return True
    jumlah_sentinel = sum(1 for v in semua_nilai if v in sentinel_tabel)
    if len(semua_nilai) > 0 and jumlah_sentinel / len(semua_nilai) >= 0.8:
        return True
    return False


_KUNCI_DIREKSI   = ('nama direksi', 'jabatan', 'direksi', 'komisaris',
                    'nama direktur', 'nama komisaris')
_KUNCI_SAHAM     = ('persentase saham', 'jenis kepemilikan', 'pemegang saham',
                    'nama pemegang', 'persentase')
_KUNCI_PERIZINAN = ('nomor izin', 'tahap kegiatan', 'modi id', 'kode wiup',
                    'jenis izin', 'status cnc')


def parse_tabel(soup, nama, kode_bu="-"):
    data_d, data_s, data_p = [], [], []
    for t in soup.find_all("table"):
        try:
            df = pd.read_html(io.StringIO(str(t)))[0]
            if df.empty or "Memuat..." in df.to_string():
                continue
            df.columns = [str(c).lower().strip() for c in df.columns]
            if _tabel_isi_kosong(df):
                continue
            if 'jenis komoditas' in df.columns:
                df.rename(columns={'jenis komoditas': 'komoditas'}, inplace=True)
            df.insert(0, 'Nama Perusahaan Asal', nama)
            df.insert(1, 'Kode Badan Usaha', kode_bu)
            cols = str(df.columns.tolist())
            if any(k in cols for k in _KUNCI_DIREKSI):
                data_d.append(df)
            elif any(k in cols for k in _KUNCI_SAHAM):
                for col in df.columns:
                    if any(k in col for k in KOLOM_NUMERIK_SAHAM):
                        df[col] = df[col].apply(format_angka_aman)
                data_s.append(df)
            elif any(k in cols for k in _KUNCI_PERIZINAN):
                for col in df.columns:
                    if any(k in str(col) for k in ('izin', 'wiup', 'modi id')):
                        df[col] = df[col].apply(
                            lambda x: f"'{x}" if pd.notnull(x) else x)
                data_p.append(df)
        except Exception as e:
            print(f"  [parse_tabel] Tabel gagal di-parse untuk '{nama}': {e}")
    return data_d, data_s, data_p

#Scraping per-perusahaan (satu perusahaan)
def _buat_placeholder(nama, kode_bu, kolom_standar):
    row = {c: 'Tidak ada data' for c in kolom_standar}
    row['Nama Perusahaan Asal'] = nama
    row['Kode Badan Usaha'] = kode_bu
    return pd.DataFrame([row])


def _simpan_hasil(driver, nama):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    info = parse_info(soup, nama)
    penyimpanan(FILE_INFO, [info],
                kunci_dedup=["Nama Perusahaan Asal", "NPWP", "Kode Badan Usaha"])
    kode_bu = str(info.get("Kode Badan Usaha", "-")).strip()
    data_d, data_s, data_p = parse_tabel(soup, nama, kode_bu)

    if not data_d:
        data_d = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_DIREKSI)]
    if not data_s:
        data_s = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_SAHAM)]
    if not data_p:
        data_p = [_buat_placeholder(nama, kode_bu, KOLOM_STANDAR_PERIZINAN)]

    penyimpanan(FILE_DIREKSI,   data_d,
                kunci_dedup=["Nama Perusahaan Asal", "nama direksi", "jabatan"])
    penyimpanan(FILE_SAHAM,     data_s,
                kunci_dedup=["Nama Perusahaan Asal", "nama", "persentase saham"])
    penyimpanan(FILE_PERIZINAN, data_p,
                kunci_dedup=["Nama Perusahaan Asal", "nomor izin", "modi id"])


def scrape_perusahaan(driver, wait, nama):
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

#MAIN
def baca_counter_selesai():
    counter = Counter()
    if os.path.exists(FILE_INFO):
        try:
            df = pd.read_csv(FILE_INFO, dtype={'Kode Badan Usaha': str})
            if 'Nama Perusahaan Asal' in df.columns:
                for n in df['Nama Perusahaan Asal'].dropna().astype(str):
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
    semua_perusahaan = [sanitasi_nama(n) for n in semua_perusahaan]

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
            nk_nama     = nospace(nama)
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