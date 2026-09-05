@echo off
cd /d D:\AAKASH\RecoverSense\backend
D:\AAKASH\RecoverSense\.venv\Scripts\python.exe -m unittest discover -s tests -v > D:\AAKASH\RecoverSense\test_results.txt 2>&1
echo EXIT_CODE: %ERRORLEVEL% >> D:\AAKASH\RecoverSense\test_results.txt
