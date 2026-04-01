@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

set TARGET_FILE=data_listing_badan_usaha_minerbaone.csv
set BACKUP_ROOT=backup_file_master

for /f "tokens=*" %%T in ('powershell -NoProfile -Command "Get-Date -Format 'dd-MM-yyyy_HH.mm'"') do set TIMESTAMP=%%T

set BACKUP_FOLDER=%BACKUP_ROOT%\%TIMESTAMP%

if not exist "%TARGET_FILE%" (
    echo [ERROR] File tidak ditemukan
    pause
    exit /b 1
)

if not exist "%BACKUP_ROOT%" mkdir "%BACKUP_ROOT%"
mkdir "%BACKUP_FOLDER%"

copy /Y "%TARGET_FILE%" "%BACKUP_FOLDER%\%TARGET_FILE%" >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Gagal membuat backup & file belum terhapus
    pause
    exit /b 1
)

echo [OK] Backup berhasil disimpan di:
echo      %BACKUP_FOLDER%\%TARGET_FILE%

del /F /Q "%TARGET_FILE%"
if %errorlevel% neq 0 (
    echo [ERROR] Backup berhasil TAPI file belum terhapus 
    pause
    exit /b 1
)

echo [OK] File master berhasil dihapus: %TARGET_FILE%
echo.
echo Backup selesai
pause