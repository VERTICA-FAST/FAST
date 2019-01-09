#!/bin/ksh
export DAYTIME=`date +%Y%m%d_%H%M%S`
export DUMMY_PATH=/opt/FAST/sh/DUMMY_SCRIPT
export SH_PATH=/opt/FAST/sh

#export DUMMY_PATH=/home/omtsmgr/FAST/sh/DUMMY_SCRIPT
#export SH_PATH=/home/omtsmgr/FAST/sh


# Activating to Original name

#mv (option) filename1.ext filename2.ext

echo "Renaming the DUUMY file to Original file."

mv $SH_PATH/VRT_TIDAL_TRIGGER_CORP_BI_GTM_DAILY_EXTRACT_AND_UPLOAD_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_CORP_BI_GTM_DAILY_EXTRACT_AND_UPLOAD.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_CORP_BI_DAILY_EXTRACT_AND_UPLOAD_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_CORP_BI_DAILY_EXTRACT_AND_UPLOAD.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_NETSUITE_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_NETSUITE.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_CALLIDUS_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_CALLIDUS.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_WORKDAY_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_WORKDAY.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_APTTUS_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_APTTUS.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_PARDOT_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_PARDOT.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_NETSUITEOPENAIR_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_NETSUITEOPENAIR.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_HIGHRADIUS_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_HIGHRADIUS.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_SFDC_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_SFDC.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_AUDT_HIST_UPDATE_STATS_REFRESH_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_AUDT_HIST_UPDATE_STATS_REFRESH.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_WORKDAY_without_zero_byte_dependency_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_WORKDAY_without_zero_byte_dependency.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_ZUORA_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_ZUORA.ksh
mv $SH_PATH/VRT_TIDAL_TRIGGER_OPRTNTY_TREND_REFRESH_DUMMY.ksh $SH_PATH/VRT_TIDAL_TRIGGER_OPRTNTY_TREND_REFRESH.ksh
mv $SH_PATH/Duplicate_Checker_DUMMY.ksh $SH_PATH/Duplicate_Checker.ksh
mv $SH_PATH/Special_Cases_DUMMY.ksh $SH_PATH/Special_Cases.ksh

echo " Activation completed"

exit 0

