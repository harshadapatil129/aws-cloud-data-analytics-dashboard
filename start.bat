@echo off
title Cloud-Based Data Analytics Dashboard
echo ============================================================
echo Starting Cloud-Based Data Analytics Platform
echo ============================================================

set "PATH=%LOCALAPPDATA%\Programs\node-v20.18.0-win-x64;%PATH%"

echo [1/2] Launching Backend API Server on Port 5000...
start "Analytics Backend (Port 5000)" cmd /k "cd /d %~dp0 && node backend/src/server.js"

timeout /t 2 /nobreak >nul

echo [2/2] Launching Frontend Dashboard on Port 3000...
start "Analytics Frontend (Port 3000)" cmd /k "cd /d %~dp0\frontend && npm run dev"

timeout /t 3 /nobreak >nul

echo ============================================================
echo Servers Started!
echo Opening Dashboard at http://localhost:3000
echo ============================================================
start http://localhost:3000
