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
            print("Mengubah tampilan menjadi 100 data per halaman...")
            dropdown_element = driver.find_element(By.XPATH, "//select") 
            dropdown = Select(dropdown_element)
            dropdown.select_by_value("100") 
            time.sleep(10)  #jeda 10 detik
            print("Load halaman berhasil")
        except Exception:
            print("Gagal mengubah dropdown, lanjut dengan default")

        #LOOPING PAGINASI
        while True:
            print(f"Scrapping data dari halaman {halaman}")
            
            time.sleep(10)  
            
            #ambil HTML dan cari tabel
            soup = BeautifulSoup(driver.page_source, "html.parser")
            tabel_html = soup.find("table")
            
            if tabel_html:
                html_string = str(tabel_html)
                df_sementara = pd.read_html(io.StringIO(html_string))[0]
                semua_data.append(df_sementara)
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
                
                halaman += 1
                
            except Exception as e:
                #print error yang sebenarnya jika gagal, jangan disembunyikan
                print(f"Scrapping berhenti (tidak menemukan tombol navigasi lagi atau sudah halaman terakhir)")
                break
                
    except Exception as e:
        print(f"Scrapping gagal/error {e}")
        
    finally:
        driver.quit()
        print("Sesi browser ditutup")
        
        if semua_data:
            df_final = pd.concat(semua_data, ignore_index=True)
            if 'Aksi' in df_final.columns:
                df_final = df_final.drop(columns=['Aksi'])
            
            nama_file = "data_listing_badan_usaha_minerbaone.csv"
            df_final.to_csv(nama_file, index=False)
            print(f"\n Scrappiing selesai. Total data: {len(df_final)} '{nama_file}'.")

if __name__ == "__main__":
    main()