@echo off
title Benchmark System - Post-Trade Engine
setlocal

REM =================================================
REM CONFIGURATION - EDIT THESE VALUES
REM =================================================
set PYTHON=python
set DURATION=30
set TPS=

REM If TPS is empty, it runs at MAX speed
REM If you want to set TPS, change to: set TPS=5000

REM =================================================
REM PATHS
REM =================================================
set VALIDATOR=consumers\validation_enrichment_consumer.py
set PNL=consumers\pnl_risk_consumer.py
set BENCHMARK_PRODUCER=benchmarking\benchmark_producer.py
set METRICS_COLLECTOR=benchmarking\metrics_collector.py
set VERIFY_RESULTS=benchmarking\verify_results.py

echo.
echo ================================================
echo BENCHMARK SYSTEM - POST-TRADE ENGINE
echo ================================================
echo Duration: %DURATION% seconds
if "%TPS%"=="" (
    echo TPS: MAX SPEED
    set BENCHMARK_ARGS=--duration %DURATION%
) else (
    echo TPS: %TPS%
    set BENCHMARK_ARGS=--duration %DURATION% --tps %TPS%
)
echo ================================================
echo.

REM =================================================
REM START CONSUMERS
REM =================================================

echo Starting consumers...
echo.

start "Validation Consumer" cmd /k "%PYTHON% -u %VALIDATOR%"
timeout /t 2 /nobreak >nul

start "PnL Consumer" cmd /k "%PYTHON% -u %PNL%"
timeout /t 2 /nobreak >nul

echo Consumers started!
echo.

REM =================================================
REM START METRICS COLLECTOR
REM =================================================

echo Starting metrics collector...
echo.

start "Metrics Collector" cmd /k "%PYTHON% -u %METRICS_COLLECTOR%"
timeout /t 2 /nobreak >nul

echo Metrics collector started!
echo.

REM =================================================
REM START BENCHMARK PRODUCER
REM =================================================

echo Starting benchmark in 3 seconds...
timeout /t 3 /nobreak >nul

echo.
echo ================================================
echo BENCHMARK STARTING NOW!
echo ================================================
echo.

REM Run benchmark producer in foreground
%PYTHON% -u %BENCHMARK_PRODUCER% %BENCHMARK_ARGS%

echo.
echo ================================================
echo BENCHMARK COMPLETE!
echo ================================================
echo.

REM =================================================
REM VERIFICATION
REM =================================================

echo Waiting 5 seconds for final trades to process...
timeout /t 5 /nobreak >nul

echo.
echo Running verification...
echo.

%PYTHON% -u %VERIFY_RESULTS%

echo.
echo ================================================
echo BENCHMARK SESSION COMPLETE
echo ================================================
echo.
echo Close the consumer windows manually or press Ctrl+C in each
echo.

pause