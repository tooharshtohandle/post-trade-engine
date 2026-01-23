@echo off
title Benchmark System - Post-Trade Engine
setlocal enabledelayedexpansion

REM =================================================
REM CONFIGURATION
REM =================================================
set PYTHON=python

REM Script paths
set VALIDATOR=consumers\validation_enrichment_consumer.py
set PNL=consumers\pnl_risk_consumer.py
set BENCHMARK_PRODUCER=benchmarking\benchmark_producer.py
set METRICS_COLLECTOR=benchmarking\metrics_collector.py
set VERIFY_RESULTS=benchmarking\verify_results.py

echo.
echo ================================================
echo BENCHMARK SYSTEM - POST-TRADE ENGINE
echo ================================================
echo.

REM =================================================
REM GET USER INPUT
REM =================================================

set TPS=
set DURATION=
set TOTAL=

echo Configuration Options:
echo.
echo 1. Run at MAX SPEED for a duration (e.g., 30 seconds)
echo 2. Run at TARGET TPS for a duration (e.g., 5000 TPS for 60 seconds)
echo 3. Send EXACT number of trades (e.g., 100,000 trades)
echo.

set /p CHOICE="Select option (1, 2, or 3): "

if "%CHOICE%"=="1" (
    set /p DURATION="Enter duration in seconds: "
    set BENCHMARK_ARGS=--duration !DURATION!
    echo.
    echo Mode: MAX SPEED for !DURATION! seconds
)

if "%CHOICE%"=="2" (
    set /p TPS="Enter target TPS: "
    set /p DURATION="Enter duration in seconds: "
    set BENCHMARK_ARGS=--duration !DURATION! --tps !TPS!
    echo.
    echo Mode: !TPS! TPS for !DURATION! seconds
)

if "%CHOICE%"=="3" (
    set /p TOTAL="Enter total number of trades: "
    set BENCHMARK_ARGS=--total !TOTAL!
    echo.
    echo Mode: Exactly !TOTAL! trades at MAX SPEED
)

echo ================================================
echo.

REM =================================================
REM START CONSUMERS
REM =================================================

echo Starting consumers...
echo.

start "Validation & Enrichment Consumer" cmd /k "%PYTHON% -u %VALIDATOR%"
timeout /t 2 /nobreak >nul

start "PnL + Risk Consumer" cmd /k "%PYTHON% -u %PNL%"
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

REM Run benchmark producer in foreground (blocking)
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
echo Next steps:
echo   1. Review the verification report above
echo   2. Check logs in the logs/ directory
echo   3. Close the consumer windows manually (Ctrl+C in each)
echo   4. Run another benchmark if needed
echo.
echo ================================================
echo.

pause