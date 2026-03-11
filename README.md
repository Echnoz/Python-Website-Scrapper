Proyek ini digunakan untuk melakukan scraping website https://minerbaone.esdm.go.id/publik/badan-usaha

FUNGSI SCRIPT
1. main_scrapper_master.py
   
   Mengambil data list badan usaha yang terdaftar di halaman depan portal publik badan usaha website minerbaone
   <img width="1917" height="893" alt="image" src="https://github.com/user-attachments/assets/2437255e-95f8-4687-9875-593be386303c" />

2. main_scrapper_detail.py
   
   Mengambil data terkait informasi detail badan usaha
   <img width="1920" height="902" alt="image" src="https://github.com/user-attachments/assets/18cb8872-e2d8-4b3e-8712-c874a11ac546" />

3. sync_scrapper_detail.py
   
   Sinkronisasi data antara file master dengan file data hasil scrapping. digunakan untuk otomatis menghapus data perusahaan yang sudah tidak ada/hilang dari file master
   
4. cleaner_py
   
   Membersihkan dan menggabungkan data ke dalam satu folder untuk lebih mudah diolah/di migrasi ke database 
