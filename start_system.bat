@echo off
title Post-Trade Processing System
setlocal

REM =================================================
REM LOCK PYTHON VERSION
REM =================================================
set PYTHON=C:\Users\Harsh\AppData\Local\Programs\Python\Python310\python.exe

REM =================================================
REM SCRIPT PATHS
REM =================================================
set PRODUCER=producer\trade_producer.py
set VALIDATOR=consumers\validation_enrichment_consumer.py
set PNL=consumers\pnl_risk_consumer.py
set REPORT=consumers\reporting_consumer.py

echo.
echo ================================================
echo 🚀 Starting Post-Trade Processing System
echo Using Python:
echo %PYTHON%
echo ================================================
echo.

REM =================================================
REM START CONSUMERS
REM =================================================

start "PnL + Risk Consumer" cmd /k ^
"%PYTHON% -u %PNL%"

timeout /t 1 >nul

start "Validation & Enrichment Consumer" cmd /k ^
"%PYTHON% -u %VALIDATOR%"

timeout /t 1 >nul

start "Reporting Consumer" cmd /k ^
"%PYTHON% -u %REPORT%"

REM =================================================
REM START PRODUCER
REM =================================================

echo Waiting 5 seconds before starting producer...
timeout /t 5 >nul

start "Trade Producer" cmd /k ^
"%PYTHON% -u %PRODUCER%"

echo.
echo ================================================
echo ✅ All services launched.
echo Close individual windows to stop services.
echo ================================================
echo.

pause
exit
