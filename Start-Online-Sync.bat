@echo off
title Krishan POS - Free Realtime Multi-Device Sync
color 0b
echo ========================================================
echo   KRISHAN POS - 100%% FREE MULTI-DEVICE REALTIME SYNC
echo   (No Credit Cards - Zero Monthly Cost)
echo ========================================================
echo.

:: Check Node.js
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Node.js is not installed!
    pause
    exit /b
)

echo [1/3] Starting Local Backend Server...
start "Krishan POS Server" /min cmd /c "node server.js"
timeout /t 2 >nul

echo [2/3] Opening POS on this Computer...
start http://localhost:3000

echo [3/3] Starting Online Multi-Device Sync Engine...
echo.
node start-sync.js
pause
