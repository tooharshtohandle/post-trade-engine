@echo off
title Benchmark System - Post-Trade Engine
setlocal

REM =================================================
REM CONFIGURATION
REM =================================================
set PYTHON=C:\Users\Harsh\AppData\Local\Programs\Python\Python310\python.exe

REM Benchmark Configuration
set /p TPS="Enter target TPS (or press Enter for MAX speed): "
set /p DURATION="Enter duration in seconds (or press Enter to use total trades): "

if "%DURATION%"=="" (
    set /p TOTAL="Enter total number of trades: "
    set BENCHMARK_MODE=--total %TOTAL%
) else (
    if "%TPS%"=="" (
        set BENCHMARK_MODE=--duration %DURATION%
    ) else (
        set BENCHMARK_MODE=--duration %DURATION% --tps %TPS%
    )
)

REM Script paths
set VALIDATOR=consumers\validation_enrichment_consumer.py
set PNL=consumers\pnl_risk_consumer.py
set BENCHMARK_PRODUCER=benchmarking\benchmark_producer.py
set METRICS_COLLECTOR=benchmarking\metrics_collector.py
set VERIFY_RESULTS=benchmarking\verify_results.py

echo.
echo ================================================
echo 🚀 BENCHMARK SYSTEM - POST-TRADE ENGINE
echo ================================================
echo Benchmark Mode: %BENCHMARK_MODE%
echo Using Python: %PYTHON%
echo ================================================
echo.

REM =================================================
REM START CONSUMERS
REM =================================================

echo ⏳ Starting consumers...

start "Validation & Enrichment Consumer" cmd /k ^
"%PYTHON% -u %VALIDATOR%"

timeout /t 2 >nul

start "PnL + Risk Consumer" cmd /k ^
"%PYTHON% -u %PNL%"

timeout /t 2 >nul

echo ✅ Consumers started
echo.

REM =================================================
REM START METRICS COLLECTOR
REM =================================================

echo ⏳ Starting metrics collector...

start "Metrics Collector" cmd /k ^
"%PYTHON% -u %METRICS_COLLECTOR%"

timeout /t 2 >nul

echo ✅ Metrics collector started
echo.

REM =================================================
REM START BENCHMARK PRODUCER
REM =================================================

echo ⏳ Starting benchmark producer in 3 seconds...
timeout /t 3 >nul

echo.
echo ================================================
echo 🔥 BENCHMARK STARTING NOW!
echo ================================================
echo.

REM Run benchmark producer in foreground (blocking)
"%PYTHON%" -u %BENCHMARK_PRODUCER% %BENCHMARK_MODE%

echo.
echo ================================================
echo ✅ BENCHMARK COMPLETE!
echo ================================================
echo.

REM =================================================
REM VERIFICATION
REM =================================================

echo ⏳ Waiting 5 seconds for final trades to process...
timeout /t 5 >nul

echo.
echo 🔍 Running verification...
echo.

"%PYTHON%" -u %VERIFY_RESULTS%

echo.
echo ================================================
echo 📊 BENCHMARK SESSION COMPLETE
echo ================================================
echo.
echo Next steps:
echo   1. Review the verification report above
echo   2. Check logs in the logs/ directory
echo   3. Close the consumer windows manually
echo   4. Run another benchmark if needed
echo.
echo ================================================
echo.

pause