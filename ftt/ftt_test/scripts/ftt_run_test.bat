@echo off
rem @(#) $Id$
rem ===============================================================================
rem  This will test writing a test tape.
rem ===============================================================================

rem ===============================================================================
rem  Get the input data file to run as %1. If the file doesn't exist quit.

if "%FTT_DIR%" == "" goto notset

if "%1" == "" goto usage

set file=%FTT_DIR%\ftt_test\scripts\%1.dat


if not exist "%file%" goto no_file



rem 
rem ===============================================================================
rem  Get the tape device by looking in %2. If that's not set,
rem  try FTT_TAPE. If that's not set either, then exit.

if not "%2" == "" set FTT_TAPE=%2

if "%FTT_TAPE%" == "" goto usage


rem ===============================================================================
 
ftt_test < %file%

goto end

rem ===============================================================================

:notset

echo.
echo FTT_DIR varable is not set
echo.
goto end

:no_file

echo.
echo file %file% does not exists
echo.
echo.
goto end

:usage

echo.
echo Usage:
echo       ftt_run_test [input data filename] ([drive])
echo.
echo       Drive name is optional if FTT_TAPE is set.
echo.

:end

set file=
