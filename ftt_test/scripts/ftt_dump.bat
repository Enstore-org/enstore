@echo off

rem
rem @(#) $Id$
rem 
rem ===============================================================================
rem This will dump a tape - used in recovering overwritten data tapes
rem ===============================================================================
 
rem 
rem ===============================================================================
rem  Get the tape device by looking in %2. If that's not set,
rem  try FTT_TAPE. If that's not set either, then exit.

if not "%1" == "" set FTT_TAPE=%1

if "%FTT_TAPE%" == "" goto usage

rem ===============================================================================



call ftt_run_test dump 

goto end

:usage
echo.
echo Usage:
echo        ftt_dump [device] 
echo.
echo Device is not needed if FTT_TAPE is set
echo.

:end

