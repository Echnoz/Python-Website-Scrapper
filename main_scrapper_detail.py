import os
import io
import re
import time
import pandas as pd
from collections import Counter
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

KUNCI_PENCARIAN = [
    "Nama Badan Usaha", "Kode Badan Usaha", "Jenis Badan Usaha",
    "Kelurahan", "NPWP", "RT/RW", "Kode Pos", "Alamat"
]

# validator format nilai per field.
# jika nilai yang terbaca tidak cocok pola ini, paksa jadi '-'
# mencegah elevator logic salah ambil teks label/heading neighbor element
# supaya nilai field yang kosong ga salah ambil 
FORMAT_NILAI = {
    "NPWP"            : re.compile(r"^[\d\.\-\*\/]{5,25}$"),
    "Kode Badan Usaha": re.compile(r"^\d+$"),
    "Kode Pos"        : re.compile(r"^\d{3,6}$"),
    "RT/RW"           : re.compile(r"^[\d\-]+\s*/\s*[\d\-]+$|^-$"),
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
def penyimpanan(nama_file, data_baru, kunci_dedup=None):
    if not data_baru:
        return
    df_new = (pd.concat(data_baru, ignore_index=True)
              if isinstance(data_baru[0], pd.DataFrame)
              else pd.DataFrame(data_baru))
    if df_new.empty:
        return
    df_old = pd.DataFrame()
    if os.path.exists(nama_file):
        try:
            df_old = pd.read_csv(nama_file)
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
    df_final.fillna("Tidak ada data").to_csv(nama_file, index=False)


# Verifikasi halaman detail (anti ghost-scraping)
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
            print(f"  [✗] GHOST: target='{nama_target}' | halaman='{nilai}'")
        return cocok
    except Exception as e:
        print(f"  [!] Error verifikasi: {e}")
        return False


# Finder tombol detail
def semua_kata_ada(nama_target, teks_baris):
    return all(k in teks_baris.lower() for k in nama_target.lower().split())


def hitung_skor(nama_normal, teks_kandidat):
    teks   = normalisasi(teks_kandidat)
    nama_n = normalisasi(nama_normal)
    if not teks:
        return 0
    rasio = len(nama_n) / len(teks)
    if teks == nama_n:                  return 1000
    if teks.startswith(nama_n + " "):  return int(200 * rasio)
    if nama_n in teks:                  return int(150 * rasio)
    if semua_kata_ada(nama_n, teks):    return int(100 * rasio)
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
            alamat, urutan_dom (indeks <tr[td]> di DOM — stabil antar reload).
    """
    soup        = BeautifulSoup(driver.page_source, "html.parser")
    baris_td    = [b for b in soup.find_all("tr") if b.find_all("td")]
    nama_normal = normalisasi(nama)
    hasil       = []

    for dom_idx, baris in enumerate(baris_td):
        tds = [td.get_text(separator=" ", strip=True) for td in baris.find_all("td")]
        if len(tds) < 4:
            continue
        #kolom pertama = no. (angka) → skip, ambil mulai index 1
        #kolom terakhir = aksi → skip
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
    """
    Sidik jari baris halaman daftar dari 3 kolom pembeda.
    Nama dikecualikan karena pasti sama untuk semua kembar.
    """
    return "|".join([baris["jenis_badan_usaha"],
                     baris["jenis_perizinan"],
                     baris["alamat"]])


def seed_sidik_dari_file(nama):
    """
    Saat resume: baca sidik yang sudah tersimpan di FILE_INFO untuk nama kembar ini.
    Gunakan Jenis Badan Usaha + Alamat (Jenis Perizinan tidak ada di FILE_INFO,
    jadi field tengah dikosongkan agar formatnya tetap 3-bagian).
    """
    if not os.path.exists(FILE_INFO):
        return set()
    try:
        df  = pd.read_csv(FILE_INFO)
        mask = df["Nama Perusahaan Asal"].astype(str).str.strip() == nama.strip()
        sidik_set = set()
        for _, baris in df[mask].iterrows():
            jbu  = normalisasi(str(baris.get("Jenis Badan Usaha", "")))
            almt = normalisasi(str(baris.get("Alamat", "")))
            sidik_set.add(f"{jbu}||{almt}")   # Jenis Perizinan dikosongkan
        return sidik_set
    except Exception:
        return set()


def cari_dan_validasi_kembar(driver, wait, nama, sidik_sudah_diambil):
    """
    Navigasi ke halaman detail entri KEMBAR yang belum diambil.

    Cara kerja:
    1. Cari nama → kumpulkan semua baris cocok dari halaman daftar (multi-halaman)
    2. Tiap baris punya sidik jari: Jenis Badan Usaha | Jenis Perizinan | Alamat
       — dibaca dari tabel halaman DAFTAR, sebelum klik Detail
    3. Klik baris pertama yang sidiknya belum ada di sidik_sudah_diambil
    4. sidik_sudah_diambil diupdate in-place setelah klik berhasil

    Catatan seed saat resume: sidik yang diseed dari FILE_INFO menggunakan format
    "jbu||alamat" (tanpa Jenis Perizinan karena tidak tersimpan di FILE_INFO).
    Jika sidik dari halaman daftar ada Jenis Perizinan-nya, perbandingannya
    dilakukan dengan dua cara: format lengkap DAN format tanpa Jenis Perizinan.
    """
    if not _navigasi_ke_halaman_daftar_dan_cari(driver, wait, nama):
        return False
    atur_pagination_dan_cari(driver, wait, nama)

    MAX_HALAMAN    = 20
    halaman_ke     = 1
    semua_kandidat = []   # (dom_idx, halaman_ke, sidik, baris_dict)

    while halaman_ke <= MAX_HALAMAN:
        print(f"  [Kembar] Membaca baris halaman #{halaman_ke}...")
        for baris in baca_baris_daftar(driver, nama):
            sk = sidik_baris_daftar(baris)
            semua_kandidat.append((baris["dom_idx"], halaman_ke, sk, baris))
            print(f"    DOM#{baris['dom_idx']} "
                  f"JBU='{baris['jenis_badan_usaha'][:20]}' | "
                  f"Izin='{baris['jenis_perizinan']}' | "
                  f"Alamat='{baris['alamat'][:25]}'")
        if not apakah_ada_halaman_berikutnya(driver):
            break
        if not navigasi_ke_halaman_berikutnya(driver, wait):
            break
        halaman_ke += 1

    if not semua_kandidat:
        print(f"  [!] Tidak ada baris cocok untuk '{nama}'.")
        return False

    for dom_idx, hal_k, sk, _ in semua_kandidat:
        print(f"\n  [Kembar] Coba DOM#{dom_idx} hal={hal_k} | '{sk[:60]}'")

        # cek sidik format lengkap (jbu|izin|alamat)
        # atau format seed resume (jbu||alamat) — karena FILE_INFO tidak punya 'Jenis Perizinan'
        bagian  = sk.split("|")
        sk_seed = f"{bagian[0]}||{bagian[2]}" if len(bagian) == 3 else sk
        if sk in sidik_sudah_diambil or sk_seed in sidik_sudah_diambil:
            print(f"  [Kembar] Sudah diambil → skip.")
            continue

        # reload halaman daftar, navigasi ke halaman yang benar, lalu klik baris ini
        if not _navigasi_ke_halaman_daftar_dan_cari(driver, wait, nama):
            continue
        atur_pagination_dan_cari(driver, wait, nama)
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
            sidik_sudah_diambil.add(sk)      #update in-place: format lengkap
            sidik_sudah_diambil.add(sk_seed)  #update in-place: format seed juga
            return True

    print(f"  [!] Semua {len(semua_kandidat)} kandidat sudah diambil / tidak valid.")
    return False


# Parsing
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


def parse_tabel(soup, nama):
    data_d, data_s, data_p = [], [], []
    for t in soup.find_all("table"):
        try:
            df = pd.read_html(io.StringIO(str(t)))[0]
            if df.empty or "Memuat..." in df.to_string():
                continue
            df.columns = [str(c).lower().strip() for c in df.columns]
            if 'jenis komoditas' in df.columns:
                df.rename(columns={'jenis komoditas': 'komoditas'}, inplace=True)
            df.insert(0, 'Nama Perusahaan Asal', nama)
            cols = str(df.columns.tolist())
            if 'nama direksi' in cols or 'jabatan' in cols:
                data_d.append(df)
            elif 'persentase saham' in cols or 'jenis kepemilikan' in cols:
                data_s.append(df)
            elif 'nomor izin' in cols or 'tahap kegiatan' in cols or 'modi id' in cols:
                for col in df.columns:
                    if any(k in str(col) for k in ('izin', 'wiup', 'modi id')):
                        df[col] = df[col].apply(
                            lambda x: f"'{x}" if pd.notnull(x) else x)
                data_p.append(df)
        except Exception:
            pass
    return data_d, data_s, data_p


# Scraping satu perusahaan
def _simpan_hasil(driver, nama):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    penyimpanan(FILE_INFO, [parse_info(soup, nama)],
                kunci_dedup=["Nama Perusahaan Asal", "NPWP", "Kode Badan Usaha"])
    data_d, data_s, data_p = parse_tabel(soup, nama)
    penyimpanan(FILE_DIREKSI,   data_d,
                kunci_dedup=["Nama Perusahaan Asal", "nama direksi", "jabatan"])
    penyimpanan(FILE_SAHAM,     data_s,
                kunci_dedup=["Nama Perusahaan Asal", "nama pemegang saham", "persentase saham"])
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
    """
    Kembar: bedakan entri berdasarkan sidik jari kolom tabel halaman daftar
    (Jenis Badan Usaha | Jenis Perizinan | Alamat).
    sidik_sudah_diambil diupdate in-place di dalam cari_dan_validasi_kembar.
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
                    counter[n.strip()] += 1
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
    kebutuhan        = Counter(n.strip() for n in semua_perusahaan)
    kembar           = {n for n, k in kebutuhan.items() if k > 1}

    if kembar:
        print(f"Perusahaan kembar terdeteksi ({len(kembar)} nama):")
        for n in sorted(kembar):
            print(f"  '{n}' — {kebutuhan[n]}x")
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

    #sidik_per_nama: {nama_kembar: set sidik_jari}
    #seed dari FILE_INFO untuk mendukung resume di tengah jalan
    sidik_per_nama = {n: seed_sidik_dari_file(n) for n in kembar}

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
            sidik_set = sidik_per_nama[nama]
            berhasil  = scrape_perusahaan_kembar(driver, wait, nama, sidik_set)
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