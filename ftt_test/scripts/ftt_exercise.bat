@echo off

rem @(#) $Id$
rem ============================================================================
rem input to ftt_test routine to exercise a drive
rem ============================================================================

rem 
rem ===============================================================================
rem  Get the tape device by looking in %2. If that's not set,
rem  try FTT_TAPE. If that's not set either, then exit.

if not "%1" == "" set FTT_TAPE=%1

if "%FTT_TAPE%" == "" goto usage

rem ============================================================================

echo.
echo.===============================================================
echo  excersizing %FTT_TAPE%
echo.
echo.===============================================================
echo write test
echo.===============================================================
echo.
call ftt_run_test twrite 

echo.===============================================================
echo verify test
echo.===============================================================
call ftt_run_test verify

echo.===============================================================
echo position test
echo.===============================================================
call ftt_run_test position

echo.===============================================================
echo delay test
echo.===============================================================
call ftt_run_test delay

goto end

:usage

echo.
echo Usage:
echo       ftt_exercise ([drive])
echo.
echo       Drive name is optional if FTT_TAPE is set.
echo.

:end