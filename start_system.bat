@echo off
title Post-Trade Processing System
setlocal

REM =================================================
REM PATHS (adjust if your folder names are different)
REM =================================================
set PRODUCER_PATH=producer\trade_producer.py
set VALIDATOR_PATH=consumers\validation_enrichment_consumer.py
set PNL_PATH=consumers\pnl_risk_consumer.py
set REPORT_PATH=consumers\reporting_consumer.py

REM =================================================
REM Launch Python consumers in smaller windows
REM =================================================

echo 🚀 Starting Complete Post-Trade Processing System...

start "PnL + Risk Consumer" /D "%CD%" cmd /k "mode con: cols=80 lines=20 & python %PNL_PATH%"
timeout /t 1 >nul
start "Validation & Enrichment Consumer" /D "%CD%" cmd /k "mode con: cols=80 lines=20 & python %VALIDATOR_PATH%"
timeout /t 1 >nul
start "Reporting Consumer" /D "%CD%" cmd /k "mode con: cols=80 lines=20 & python %REPORT_PATH%"

REM =================================================
REM Launch producer after 5 seconds
REM =================================================
echo Waiting 5 seconds before starting producer...
timeout /t 5 >nul
start "Trade Producer" /D "%CD%" cmd /k "mode con: cols=80 lines=20 & python %PRODUCER_PATH%"

echo.
echo =======================================================
echo ✅ All consumers and producer are running in separate windows.
echo Reports will be saved in reports\ folder
echo =======================================================

REM =================================================
REM Wait for user input to stop all processes
REM =================================================
:ASK_EXIT
set /p userInput=Do you want to stop all consumers and producer? (Y/N): 
if /I "%userInput%"=="Y" goto STOP_ALL
if /I "%userInput%"=="N" goto ASK_EXIT
echo Please enter Y or N.
goto ASK_EXIT

:STOP_ALL
echo Stopping all Python processes...
taskkill /IM python.exe /F >nul 2>&1
echo All consumers and producer stopped.
pause
exit
