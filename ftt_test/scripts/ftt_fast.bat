@echo off

rem 
rem @(#) $Id$
rem ============================================================================
rem test routine to run all of the reasonably fast ftt tests.
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
echo doing all of the [reasonably] fast ftt tests using %FTT_TAPE%

if  %align% == 0 goto next

echo.===============================================================
echo align test - may cause scsi resets!
echo.===============================================================
call ftt_run_test align


:next
echo.===============================================================
echo mode test
echo.===============================================================
call ftt_run_test mode

echo.===============================================================
echo close test
echo.===============================================================
call ftt_run_test close

echo.===============================================================
echo label test
echo.===============================================================
call ftt_run_test label

echo.===============================================================
echo stats test
echo.===============================================================
call ftt_run_test stats

echo.===============================================================
echo 2fm test
echo.===============================================================
call ftt_run_test 2fm

echo.===============================================================
echo.write test
echo.===============================================================
call ftt_run_test twrite

echo.===============================================================
echo read_only test
echo.===============================================================
call ftt_run_test read_only

echo.===============================================================
echo verify test
echo.===============================================================
call ftt_run_test verify

echo.===============================================================
echo position test
echo.===============================================================
call ftt_run_test position

echo.===============================================================
echo async test
echo.===============================================================
call ftt_run_test async

echo.===============================================================
echo root test
echo.===============================================================
call ftt_run_test root

echo.===============================================================
echo describe test
echo.===============================================================
echo not for NT

if %unload% == 0 goto skip

echo.===============================================================
echo notape test
echo.===============================================================
call ftt_run_test notape


goto end

:usage

echo.
echo  ftt_fast [-u] [-a] [devfile]
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
