#!/bin/ksh

# This shell script invokes the vsql file to compare the mule audit table loaded in vertica with the vertica stage to base audit table to generate the report. 
# To be run once in a day

export DAY=`date +%d_%m_%Y`
export DAYTIME='date +%d-%m-%Y-%H:%M:%S'
export SUB_AREA=AUDIT_REPORT

case "`hostname`" in 
	"g2u2002c") ENV=DEV
	;;
	"mc4t01126") ENV=DEV
	;;
	"mc4t01145") ENV=SIT
	;;
	"mc4t01165") ENV=ITG
	;;
	"mc4t01146") ENV=PRD
esac

export CONFIG=/opt/FAST/config
export REPORTDIR=/opt/FAST/misc/AUDIT_REPORT
export HOST=$(grep $ENV $CONFIG/cred.config | cut -f 4 -d :)
export USER=$(grep $ENV $CONFIG/cred.config | cut -f 2 -d :)
export PASSWORD=$(grep $ENV $CONFIG/cred.config | cut -f 3 -d :)
export DB=$(grep $ENV $CONFIG/cred.config | cut -f 6 -d :)
export PORT=$(grep $ENV $CONFIG/cred.config | cut -f 5 -d :)

ksh /opt/FAST/sh/ZERO_BYTE_FILE_CHECK.ksh

echo "`$DAYTIME` : $SUB_AREA : ####################Script started########################" >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
echo "`$DAYTIME` : $SUB_AREA : Directory paths for $SUB_AREA are set." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
echo "`$DAYTIME` : $SUB_AREA : Extracted DB login related info from config file." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log


echo "`$DAYTIME` : $SUB_AREA : Checking database connectivity" >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log

/opt/Vertica/bin/vsql -l -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log                  
if [ $? -eq 0 ]                                                                 						### Checking the result code to check connectivity
then
	echo "`$DAYTIME` : $SUB_AREA : Processing the Mule audit and Vertica table and preparing the results....... " >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
	/opt/Vertica/bin/vsql -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD -E -e -a --echo-all <$REPORTDIR/AUDIT_REPORT.sql >> $REPORTDIR/$SUB_AREA"_Script_Log_"$DAY.log 2>>$REPORTDIR/$SUB_AREA"_Script_Log_"$DAY.log
	if [ $? -eq 0 ]
	then
		echo "`$DAYTIME` : $SUB_AREA : Processing the Mule audit and Vertica table successfully completed " >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
		for sub in `cat $REPORTDIR"/AUDIT_REPORT_LIST.txt"`; do
			echo "`$DAYTIME` : $SUB_AREA : Extracting the report into csv file for $sub." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
			/opt/Vertica/bin/vsql -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD -F ',' -P footer=off -A -c "SELECT * FROM swt_rpt_stg.EVENTS_STORAGE where upper(SUBJECT_AREA)='$sub' ORDER BY CYCLE;"> $REPORTDIR/REPORTS/$sub"_"$SUB_AREA"_"$DAY.csv 2>>$REPORTDIR/$SUB_AREA"_Script_Log_"$DAY.log
			if [ $? -eq 0 ] 
			then
				echo "`$DAYTIME` : $SUB_AREA : Extraction successful." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
			else
				echo "`$DAYTIME` : $SUB_AREA : Extraction failed." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
				echo -e "Hi Team,\n\nAudit report for $sub failed and please check the log to see the failure reasons.\n\nThis is an automatic email and do not respond.\n" | mailx -s "Audit report extraction for $sub failed for $DAY" ssit-sw-rep@hpe.com
			fi
		done
		/opt/Vertica/bin/vsql -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD -F ',' -P footer=off -A -c "SELECT * FROM swt_rpt_stg.EVENTS_STORAGE_SUMMARY ORDER BY subject_area, cycle;"> $REPORTDIR/REPORTS/$SUB_AREA"_SUMMARY_"$DAY.csv 2>>$REPORTDIR/$SUB_AREA"_Script_Log_"$DAY.log
		if [ $? -eq 0 ] 
		then
			echo "`$DAYTIME` : $SUB_AREA : Summary extraction successful." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
		else
			echo "`$DAYTIME` : $SUB_AREA : Summary extraction failed." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
			echo -e "Hi Team,\n\nAudit report summary failed and please check the log to see the failure reasons.\n\nThis is an automatic email and do not respond.\n" | mailx -s "Audit report summary extraction failed for $DAY" ssit-sw-rep@hpe.com
		fi
		export file_count=`find $REPORTDIR/REPORTS/*.csv -maxdepth 1 -type f | wc -l 2>>$REPORTDIR/$SUB_AREA"_Script_Log_"$DAY.log`
		if [ $file_count -eq 0 ]
		then
			echo "`$DAYTIME` : $SUB_AREA : There are no extracts to process. May be the report extraction failed." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
		else
			for file in `ls $REPORTDIR/REPORTS/*.csv`; do
			echo " -a $file" >> $REPORTDIR/REPORTS/mail_attach_list.txt
			done
		fi
		/opt/Vertica/bin/vsql -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD -F ',' -Atc "select distinct SUBJECT_AREA,EXEC_DATE,CYCLE from swt_rpt_stg.FAILED_TABLES_SUMMARY where SUBJECT_AREA='NetSuite' order by SUBJECT_AREA,EXEC_DATE,CYCLE" >$REPORTDIR"/"error_details.txt
		
		if [ -s $REPORTDIR"/"error_details.txt ]
		then
			echo -e "Hi All,\n There were some issues for the tables while trying to load the data. The source of the error and other details are given below :\n" >>$REPORTDIR"/"mail_body.txt
			for line in `cat $REPORTDIR"/"error_details.txt`; do
				export sub_area=`echo $line | cut -f 1 -d ,`
				export day=`echo $line | cut -f 2 -d ,`
				export cycle=`echo $line | cut -f 3 -d ,`
				/opt/Vertica/bin/vsql -h $HOST -p $PORT -d $DB -U $USER -w $PASSWORD -F '=' -x -Atc "select * from swt_rpt_stg.FAILED_TABLES_SUMMARY where SUBJECT_AREA ='$sub_area' and EXEC_DATE='$day' and CYCLE=$cycle order by SUBJECT_AREA,EXEC_DATE,CYCLE" >>$REPORTDIR"/"mail_body.txt
				echo "----------------------------------" >>$REPORTDIR"/"mail_body.txt
			done
			echo -e "\n If there are queries regarding this, please send an email to ssit-sw-rep@hpe.com \nThis is an automatic email and do not respond." >>$REPORTDIR"/"mail_body.txt
			cat $REPORTDIR"/"mail_body.txt | mailx -s "Few errors detected during the data load" `cat $REPORTDIR/REPORTS/mail_attach_list.txt` `cat $CONFIG/audit_report_recipient.txt`
			if [ $? -eq 0 ]
			then
				rm $REPORTDIR"/"mail_body.txt
			else
				echo -e "Hi All,\n\nLatency report mail notification failed. Please check" | mailx -s "Error sending Latency report failures notification mail" ssit-sw-rep@hpe.com
			fi
		else
			echo -e "Hi Team,\n\nPlease find the attached audit reports for different subject areas for today. If there is any missing report, please check if you have received any error email regarding the failure.\n\nThis is an automatic email and do not respond.\n" | mailx -s "Audit report for $DAY" `cat $REPORTDIR/REPORTS/mail_attach_list.txt` `cat $CONFIG/audit_report_recipient.txt`
		fi
	else
		echo -e "Hi Team,\n\nProcessing the mule audit and vertica audit encountered some error. \n\nThis is an automatic email and do not respond.\n" | mailx -s "Audit report extraction failed for $DAY" ssit-sw-rep@hpe.com
	fi	
else
	echo "`$DAYTIME` : $SUB_AREA : Unable to establish connection with database." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
	echo "Unable to establish connection with database. Please check. This is an auto-generated email. Do not reply." | mailx -s "$DAY : Error connecting $ENV Database while trying to run $SUB_AREA" `cat $CONFIG/mail_recipient.txt`
	if [ $? -eq 0 ]                                                                 				### Checking the result code to check status of mail 
	then
		echo "`$DAYTIME` : $SUB_AREA : Status email has been sent." >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
	else
		echo "`$DAYTIME` : $SUB_AREA : Error in sending mail. Please check mail functionality" >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
	fi
fi

echo "`$DAYTIME` : $SUB_AREA : Moving the files to archive and cleaning up archive directory" >> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log
mv $REPORTDIR/REPORTS/*.* $REPORTDIR/ARCHIVE/
find $REPORTDIR/ARCHIVE/ -type f -mtime +7 -exec rm -f {} \;>> $REPORTDIR"/"$SUB_AREA"_Script_Log_"$DAY.log

exit 0



