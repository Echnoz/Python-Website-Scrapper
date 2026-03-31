import io
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup

def main():
    print("Mulai Scraping (100 list/page)")
    
    driver = webdriver.Chrome()
    url = "https://minerbaone.esdm.go.id/publik/badan-usaha"
    driver.get(url)

    semua_data = []
    halaman = 1

    try:
        #tunggu maksimal 20 detik untuk loading awal
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        #ubah dropdown menjadi 100 list
        try:
            print("Mengubah tampilan menjadi 100 list per halaman")
            dropdown_element = driver.find_element(By.XPATH, "//select") 
            dropdown = Select(dropdown_element)
            dropdown.select_by_value("100") 
            time.sleep(10)  #jeda 10 detik

            for _ in range(60):
                time.sleep(0.5)
                try:
                    soup_cek = BeautifulSoup(driver.page_source, "html.parser")
                    tabel_cek = soup_cek.find("table")
                    if tabel_cek:
                        jumlah_baris = len(tabel_cek.find("tbody").find_all("tr"))
                        if jumlah_baris > 10:
                            break  #tabel sudah reload ke 100 baris
                except Exception:
                    pass

            print("Load halaman berhasil")
        except Exception:
            print("Gagal mengubah dropdown (back to default)")

        #LOOPING PAGINASI
        while True:
            print(f"Scraping data halaman {halaman}")
            
            time.sleep(10)  
            
            #ambil HTML dan cari tabel
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tabel_html = soup.find("table")
            
            if tabel_html:
                html_string = str(tabel_html)
                df_sementara = pd.read_html(io.StringIO(html_string))[0]
                semua_data.append(df_sementara)

                try:
                    tanda_baris_pertama = tabel_html.find("tbody").find("tr").get_text(strip=True)
                except Exception:
                    tanda_baris_pertama = ""

            else:
                print("Tabel tidak ditemukan di halaman ini")
                break
            
            #LOGIKA NAVIGASI (untuk pindah ke halaman selanjutnya)
            try:
                xpath_tombol = (
                    "//li[contains(@class, 'next') and not(contains(@class, 'disabled'))]/a | "
                    "//a[contains(text(), 'Selanjutnya')] | "
                    "//button[contains(@class, 'next')]"
                )
                
                #memaksa selenium menunggu maksimal 10 detik sampai tombol 'next' benar-benar muncul di HTML
                tombol_next = wait.until(EC.presence_of_element_located((By.XPATH, xpath_tombol)))
                
                #cek apakah tombol disabled (halaman terakhir)
                try:
                    parent = tombol_next.find_element(By.XPATH, "./parent::*")
                    if "disabled" in parent.get_attribute("class"):
                        print("Sudah mencapai halaman terakhir")
                        break
                except:
                    pass 
                
                #scroll ke tengah dan klik
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tombol_next)
                time.sleep(1) 
                driver.execute_script("arguments[0].click();", tombol_next)

                #maks tunggu 30 detik, cek setiap 0.5 detik.
                if tanda_baris_pertama:
                    for _ in range(60):
                        time.sleep(0.5)
                        try:
                            soup_cek = BeautifulSoup(driver.page_source, "html.parser")
                            tabel_cek = soup_cek.find("table")
                            if tabel_cek:
                                tanda_baru = tabel_cek.find("tbody").find("tr").get_text(strip=True)
                                if tanda_baru != tanda_baris_pertama:
                                    break  #halaman baru sudah ke-load
                        except Exception:
                            pass
                
                halaman += 1
                
            except Exception as e:
                #print error jika gagal
                print(f"Scraping berhenti (tidak ada tombol navigasi lagi atau sudah halaman terakhir)")
                break
                
    except Exception as e:
        print(f"Scraping gagal/error {e}")
        
    finally:
        driver.quit()
        print("Browser ditutup")
        
        if semua_data:
            df_final = pd.concat(semua_data, ignore_index=True)
            if 'Aksi' in df_final.columns:
                df_final = df_final.drop(columns=['Aksi'])
            
            nama_file = "data_listing_badan_usaha_minerbaone.csv"
            df_final.to_csv(nama_file, index=False)
            print(f"\n Scraping selesai. Total data: {len(df_final)} '{nama_file}'.")

if __name__ == "__main__":
    main()