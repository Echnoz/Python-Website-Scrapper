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

:: â”€â”€ CEK SEMUA FILE ADA â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
set ADA_ERROR=0
for %%F in ("%FILE_1%" "%FILE_2%" "%FILE_3%" "%FILE_4%") do (
    if not exist %%F (
        echo [ERROR] File tidak ditemukan: %%~F
        set ADA_ERROR=1
    )
)
if "%ADA_ERROR%"=="1" (
    echo.
    echo Proses dibatalkan. Tidak ada file yang dibackup atau dihapus
    pause
    exit /b 1
)

:: â”€â”€ BUAT FOLDER BACKUP â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if not exist "%BACKUP_ROOT%" mkdir "%BACKUP_ROOT%"
mkdir "%BACKUP_FOLDER%"

:: â”€â”€ BACKUP SEMUA FILE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
    echo [ERROR] Sebagian backup gagal. Tidak ada file yang dihapus.
    echo         Periksa folder: %BACKUP_FOLDER%
    pause
    exit /b 1
)

echo.
echo Semua file berhasil dibackup ke: %BACKUP_FOLDER%

:: â”€â”€ HAPUS SEMUA FILE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo.
set GAGAL_HAPUS=0
for %%F in ("%FILE_1%" "%FILE_2%" "%FILE_3%" "%FILE_4%") do (
    del /F /Q %%F
    if !errorlevel! neq 0 (
        echo [ERROR] Gagal menghapus: %%~nxF
        set GAGAL_HAPUS=1
    ) else (
        echo [OK] Dihapus: %%~nxF
    )
)

if "%GAGAL_HAPUS%"=="1" (
    echo.
    echo [ERROR] Backup berhasil TAPI sebagian file gagal dihapus.
    pause
    exit /b 1
)

echo.
echo Backup selesai
pause