@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

set FILE_1=1_Informasi_Badan_Usaha.csv
set FILE_2=2_Susunan_Direksi.csv
set FILE_3=3_Pemegang_Saham.csv
set FILE_4=4_Daftar_Perizinan.csv
set BACKUP_ROOT=backup_file_data

for /f "tokens=*" %%T in ('powershell -NoProfile -Command "Get-Date -Format 'dd-MM-yyyy_HH.mm'"') do set TIMESTAMP=%%T

set BACKUP_FOLDER=%BACKUP_ROOT%\%TIMESTAMP%

::cek semua file
set ADA_ERROR=0
for %%F in ("%FILE_1%" "%FILE_2%" "%FILE_3%" "%FILE_4%") do (
    if not exist %%F (
        echo [ERROR] File tidak ditemukan: %%~F
        set ADA_ERROR=1
    )
)
if "%ADA_ERROR%"=="1" (
    echo.
    echo Proses dibatalkan, tidak ada file yang dibackup
    pause
    exit /b 1
)

::buat folder backup
if not exist "%BACKUP_ROOT%" mkdir "%BACKUP_ROOT%"
mkdir "%BACKUP_FOLDER%"

::backup semua file
set GAGAL_BACKUP=0
for %%F in ("%FILE_1%" "%FILE_2%" "%FILE_3%" "%FILE_4%") do (
    copy /Y %%F "%BACKUP_FOLDER%\%%~nxF" >nul 2>&1
    if !errorlevel! neq 0 (
        echo [ERROR] Gagal membackup: %%~nxF
        set GAGAL_BACKUP=1
    ) else (
        echo [OK] Backup: %%~nxF
    )
)

if "%GAGAL_BACKUP%"=="1" (
    echo.
    echo [ERROR] Sebagian backup gagal
    echo         Periksa folder: %BACKUP_FOLDER%
    pause
    exit /b 1
)

echo.
echo Semua file berhasil dibackup ke: %BACKUP_FOLDER%
echo.
echo Backup selesai
pause