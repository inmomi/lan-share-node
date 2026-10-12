@echo off
chcp 65001 >nul
cd /d "%~dp0"

if "%PORT%"=="" set PORT=8080
if "%UPLOAD_DIR%"=="" set UPLOAD_DIR=%~dp0uploads

echo Starting LAN Share on port %PORT% ...
echo Upload folder: %UPLOAD_DIR%
echo.

where node >nul 2>nul
if %errorlevel%==0 (
  node server.js
) else (
  "C:\Users\Administrator\.workbuddy\binaries\node\versions\22.22.2-6\node.exe" server.js
)

pause
