@echo off
rem PYSPARK_PYTHON for Windows (see FAILURES.md: "Python worker crashes on Windows"). Needs env var PYSPARK_REAL_PYTHON.
rem Spark runs "python -m pyspark.worker". That crashes. win_worker.py starts the worker another way.
if "%2"=="pyspark.worker" ("%PYSPARK_REAL_PYTHON%" "%~dp0win_worker.py") else ("%PYSPARK_REAL_PYTHON%" %*)
