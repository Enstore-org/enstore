@echo off

rem
rem @(#) $Id$
rem 
rem 

if "%1" == "" goto usage
if "%2" == "" goto usage

echo ftt_open %1											>	tmp.dat 
echo ftt_rewind												>>	tmp.dat
echo ftt_write_vol_label -type FTT_ANSI_HEADER -label %2	>>  tmp.dat
echo ftt_rewind												>>	tmp.dat
echo ftt_close												>>  tmp.dat
echo quit													>>  tmp.dat


ftt_test < tmp.dat

del tmp.dat

goto end

:usage

echo.
echo Usage:
echo        ftt_ansi_prelabel [device] [label]
echo.

:end
