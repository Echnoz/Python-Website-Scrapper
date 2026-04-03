Proyek ini digunakan untuk melakukan scraping website https://minerbaone.esdm.go.id/publik/badan-usaha

FUNGSI SCRIPT
1. main_scraper_master.py
   
   Mengambil data list badan usaha yang terdaftar di halaman depan portal publik badan usaha website minerbaone
   <img width="1917" height="893" alt="image" src="https://github.com/user-attachments/assets/2437255e-95f8-4687-9875-593be386303c" />

2. main_scraper_detail.py
   
   Mengambil data terkait informasi detail badan usaha
   <img width="1920" height="902" alt="image" src="https://github.com/user-attachments/assets/18cb8872-e2d8-4b3e-8712-c874a11ac546" />

3. sync_data.py
   
   Sinkronisasi data antara file master dengan file data hasil scraping. Digunakan untuk menghapus data perusahaan yang sudah tidak ada/hilang dari file master secara otomatis

4. validasi_data.py

   Melakukan validasi antara data di yang ada di file hasil scraping dengan file master. Digunakan untuk menemukan data perusahaan yang belum di scraping

5. data_cleaning.py

   Melakukan pembersihan (cleaning) atau normalisasi data hasil scraping sebelum di import ke database. Script ini digunakan dengan tujuan agar data hasil scraping bisa langsung masuk ke produksi dan tidak perlu melalui proses staging lagi

TAMBAHAN

- backup_master.bat

  Digunakan untuk mengotomatisasi backup file master (data_listing_badan_usaha_minerbaone.csv)

  
- backup_data.bat

  Digunakan untuk mengotomatisasi backup file-file data hasil scraping
