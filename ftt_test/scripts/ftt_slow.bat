@echo off

rem @(#) $Id$
rem ============================================================================
rem test routine to run all of the ftt tests.
rem this will be VERY slow
rem it requires no human intervention; however, at the end the
rem tape may be unloaded if the user specified "notape"
rem ============================================================================

set align=0
set unload=0
set dev=

if "%1" == "-a" set align=1
if "%1" == "-u" set unload=1
if not "%1" == "" if not "%1" == "-u" if not "%1" == "-a" set dev=%1

if not "%dev%" == "" if not "%2" == "" goto usage


if "%2" == "-a" set align=1
if "%2" == "-u" set unload=1
if not "%2" == "" if not "%2" == "-u" if not "%2" == "-a" set dev=%2


if not "%dev%" == "" if not "%3" == "" goto usage

if not "%3" == "" set dev=%3

if not "%4" == "" goto usage


rem 
rem ===============================================================================
rem  Get the tape device by looking in %2. If that's not set,
rem  try FTT_TAPE. If that's not set either, then exit.

if not "%dev%" == "" set FTT_TAPE=%dev%

if "%FTT_TAPE%" == "" goto usage

rem ==============================================================================

echo.===============================================================
echo doing all of the fast ftt tests using $FTT_TAPE. This will be 
echo VERY slow. It may take several hours
echo.===============================================================

echo.===============================================================
echo write full test
echo.===============================================================
call ftt_run_test full

echo.===============================================================
echo erase test
echo.===============================================================
call ftt_run_test erase

echo.===============================================================
echo delay test
echo.===============================================================
call ftt_run_test delay

echo.===============================================================
echo fast tests
echo ===============================================================
call ftt_fast %1 %2 %3

goto end

:usage

echo.
echo  ftt_slow [-u] [-a] [devfile]
echo		-u = do unload test at end (will leave tape unloaded)
echo 		-a = do alignment tests (may cause scsi resets)
echo		[devfile] = name of device to use. FTT_TAPE is default
echo.

:end

rem ========================================================================
rem unset variables
rem ========================================================================

set align=
set unload=
set dev=
