/****
****Script Name	  : SF_Task.sql
****Description   : Incremental data load for SF_Task
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_Task";

/* Inserting values into Audit table  */

INSERT INTO swt_rpt_stg.FAST_LD_AUDT
(
SUBJECT_AREA
,TBL_NM
,LD_DT
,START_DT_TIME
,END_DT_TIME
,SRC_REC_CNT
,TGT_REC_CNT
,COMPLTN_STAT
)
select 'SFDC','SF_Task',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".SF_Task_Hist SELECT * from "swt_rpt_stg".SF_Task;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_Task where id in (
select id from swt_rpt_stg.SF_Task group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_Task where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_Task.id=t2.id and swt_rpt_stg.SF_Task.auto_id<t2.auto_id);

Commit;


CREATE LOCAL TEMP TABLE SF_Task_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_Task)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_Task;

CREATE LOCAL TEMP TABLE SF_Task_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_Task)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_Task_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_Task_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;



/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_Task_Hist
(
AccountId
,ActivityDate
,CallDisposition
,CallDurationInSeconds
,CallObject
,CallType
,CreatedById
,CreatedDate
,CurrencyIsoCode
,Description
,Id
,IsArchived
,IsClosed
,IsDeleted
,IsHighPriority
,IsRecurrence
,IsReminderSet
,LastModifiedById
,LastModifiedDate
,OwnerId
,Priority
,RecurrenceActivityId
,RecurrenceDayOfMonth
,RecurrenceDayOfWeekMask
,RecurrenceEndDateOnly
,RecurrenceInstance
,RecurrenceInterval
,RecurrenceMonthOfYear
,RecurrenceRegeneratedType
,RecurrenceStartDateOnly
,RecurrenceTimeZoneSidKey
,RecurrenceType
,ReminderDateTime
,ringdna__Abandoned_Call__c
,ringdna__Automated_Voicemail_Used__c
,ringdna__Automated_Voicemail__c
,ringdna__Called_Back__c
,ringdna__Call_Connected__c
,ringdna__Call_Direction__c
,ringdna__Call_Disposition__c
,ringdna__Call_Duration_min__c
,ringdna__Call_Duration__c
,ringdna__Call_Hour_Of_Day_Local__c
,ringdna__Call_Rating__c
,ringdna__Call_Recording_URL__c
,ringdna__Call_Recording__c
,ringdna__Created_by_RingDNA__c
,ringdna__Local_Presence_Num__c
,ringdna__Local_Presence__c
,ringdna__Queue_Hold_Time__c
,ringdna__Queue__c
,ringdna__Supervisor_Notes__c
,ringdna__Voicemail__c
,Status
,Subject
,SWT_Abandoned_Call__c
,SWT_Automated_Voicemail_Used__c
,SWT_Automated_Voicemail__c
,SWT_Called_Back__c
,SWT_Call_Direction__c
,SWT_Call_Disposition__c
,SWT_Call_Duration__c
,SWT_Call_Hour_of_Day_Local__c
,SWT_Call_Rating__c
,SWT_Call_Recording_URL__c
,SWT_Call_Result__c
,SWT_Call_Type__c
,SWT_Created_by_Ring_DNA__c
,SWT_Date_Initiated__c
,SWT_Local_Presence_Number__c
,SWT_Local_Presence__c
,SWT_Origin__c
,SWT_Pre_sales_Subtype__c
,SWT_Queue_Hold_Time__c
,SWT_Queue__c
,SWT_Reason_Closed__c
,SWT_Voicemail__c
,SystemModstamp
,TaskSubtype
,WhatCount
,WhatId
,WhoCount
,WhoId
,SWT_Approval_Type__c
,SWT_Completion_Date__c
,SWT_Referral_partner__c
,SWT_Sourced_Partner__c
,RecordTypeId
,LD_DT
,SWT_INS_DT
,d_source
)
select
AccountId
,ActivityDate
,CallDisposition
,CallDurationInSeconds
,CallObject
,CallType
,CreatedById
,CreatedDate
,CurrencyIsoCode
,Description
,Id
,IsArchived
,IsClosed
,IsDeleted
,IsHighPriority
,IsRecurrence
,IsReminderSet
,LastModifiedById
,LastModifiedDate
,OwnerId
,Priority
,RecurrenceActivityId
,RecurrenceDayOfMonth
,RecurrenceDayOfWeekMask
,RecurrenceEndDateOnly
,RecurrenceInstance
,RecurrenceInterval
,RecurrenceMonthOfYear
,RecurrenceRegeneratedType
,RecurrenceStartDateOnly
,RecurrenceTimeZoneSidKey
,RecurrenceType
,ReminderDateTime
,ringdna__Abandoned_Call__c
,ringdna__Automated_Voicemail_Used__c
,ringdna__Automated_Voicemail__c
,ringdna__Called_Back__c
,ringdna__Call_Connected__c
,ringdna__Call_Direction__c
,ringdna__Call_Disposition__c
,ringdna__Call_Duration_min__c
,ringdna__Call_Duration__c
,ringdna__Call_Hour_Of_Day_Local__c
,ringdna__Call_Rating__c
,ringdna__Call_Recording_URL__c
,ringdna__Call_Recording__c
,ringdna__Created_by_RingDNA__c
,ringdna__Local_Presence_Num__c
,ringdna__Local_Presence__c
,ringdna__Queue_Hold_Time__c
,ringdna__Queue__c
,ringdna__Supervisor_Notes__c
,ringdna__Voicemail__c
,Status
,Subject
,SWT_Abandoned_Call__c
,SWT_Automated_Voicemail_Used__c
,SWT_Automated_Voicemail__c
,SWT_Called_Back__c
,SWT_Call_Direction__c
,SWT_Call_Disposition__c
,SWT_Call_Duration__c
,SWT_Call_Hour_of_Day_Local__c
,SWT_Call_Rating__c
,SWT_Call_Recording_URL__c
,SWT_Call_Result__c
,SWT_Call_Type__c
,SWT_Created_by_Ring_DNA__c
,SWT_Date_Initiated__c
,SWT_Local_Presence_Number__c
,SWT_Local_Presence__c
,SWT_Origin__c
,SWT_Pre_sales_Subtype__c
,SWT_Queue_Hold_Time__c
,SWT_Queue__c
,SWT_Reason_Closed__c
,SWT_Voicemail__c
,SystemModstamp
,TaskSubtype
,WhatCount
,WhatId
,WhoCount
,WhoId
,SWT_Approval_Type__c
,SWT_Completion_Date__c
,SWT_Referral_partner__c
,SWT_Sourced_Partner__c
,RecordTypeId
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_Task WHERE id in
(SELECT STG.id FROM SF_Task_stg_Tmp_Key STG JOIN SF_Task_base_Tmp
ON STG.id = SF_Task_base_Tmp.id AND STG.LastModifiedDate >= SF_Task_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."SF_Task_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_Task WHERE id in
(SELECT STG.id FROM SF_Task_stg_Tmp_Key STG JOIN SF_Task_base_Tmp
ON STG.id = SF_Task_base_Tmp.id AND STG.LastModifiedDate >= SF_Task_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_Task
(
AccountId
,ActivityDate
,CallDisposition
,CallDurationInSeconds
,CallObject
,CallType
,CreatedById
,CreatedDate
,CurrencyIsoCode
,Description
,Id
,IsArchived
,IsClosed
,IsDeleted
,IsHighPriority
,IsRecurrence
,IsReminderSet
,LastModifiedById
,LastModifiedDate
,OwnerId
,Priority
,RecurrenceActivityId
,RecurrenceDayOfMonth
,RecurrenceDayOfWeekMask
,RecurrenceEndDateOnly
,RecurrenceInstance
,RecurrenceInterval
,RecurrenceMonthOfYear
,RecurrenceRegeneratedType
,RecurrenceStartDateOnly
,RecurrenceTimeZoneSidKey
,RecurrenceType
,ReminderDateTime
,ringdna__Abandoned_Call__c
,ringdna__Automated_Voicemail_Used__c
,ringdna__Automated_Voicemail__c
,ringdna__Called_Back__c
,ringdna__Call_Connected__c
,ringdna__Call_Direction__c
,ringdna__Call_Disposition__c
,ringdna__Call_Duration_min__c
,ringdna__Call_Duration__c
,ringdna__Call_Hour_Of_Day_Local__c
,ringdna__Call_Rating__c
,ringdna__Call_Recording_URL__c
,ringdna__Call_Recording__c
,ringdna__Created_by_RingDNA__c
,ringdna__Local_Presence_Num__c
,ringdna__Local_Presence__c
,ringdna__Queue_Hold_Time__c
,ringdna__Queue__c
,ringdna__Supervisor_Notes__c
,ringdna__Voicemail__c
,Status
,Subject
,SWT_Abandoned_Call__c
,SWT_Automated_Voicemail_Used__c
,SWT_Automated_Voicemail__c
,SWT_Called_Back__c
,SWT_Call_Direction__c
,SWT_Call_Disposition__c
,SWT_Call_Duration__c
,SWT_Call_Hour_of_Day_Local__c
,SWT_Call_Rating__c
,SWT_Call_Recording_URL__c
,SWT_Call_Result__c
,SWT_Call_Type__c
,SWT_Created_by_Ring_DNA__c
,SWT_Date_Initiated__c
,SWT_Local_Presence_Number__c
,SWT_Local_Presence__c
,SWT_Origin__c
,SWT_Pre_sales_Subtype__c
,SWT_Queue_Hold_Time__c
,SWT_Queue__c
,SWT_Reason_Closed__c
,SWT_Voicemail__c
,SystemModstamp
,TaskSubtype
,WhatCount
,WhatId
,WhoCount
,WhoId
,SWT_Approval_Type__c
,SWT_Completion_Date__c
,SWT_Referral_partner__c
,SWT_Sourced_Partner__c
,RecordTypeId
,SWT_INS_DT
)
SELECT DISTINCT
AccountId
,ActivityDate
,CallDisposition
,CallDurationInSeconds
,CallObject
,CallType
,CreatedById
,CreatedDate
,CurrencyIsoCode
,Description
,SF_Task_stg_Tmp.Id
,IsArchived
,IsClosed
,IsDeleted
,IsHighPriority
,IsRecurrence
,IsReminderSet
,LastModifiedById
,SF_Task_stg_Tmp.LastModifiedDate
,OwnerId
,Priority
,RecurrenceActivityId
,RecurrenceDayOfMonth
,RecurrenceDayOfWeekMask
,RecurrenceEndDateOnly
,RecurrenceInstance
,RecurrenceInterval
,RecurrenceMonthOfYear
,RecurrenceRegeneratedType
,RecurrenceStartDateOnly
,RecurrenceTimeZoneSidKey
,RecurrenceType
,ReminderDateTime
,ringdna__Abandoned_Call__c
,ringdna__Automated_Voicemail_Used__c
,ringdna__Automated_Voicemail__c
,ringdna__Called_Back__c
,ringdna__Call_Connected__c
,ringdna__Call_Direction__c
,ringdna__Call_Disposition__c
,ringdna__Call_Duration_min__c
,ringdna__Call_Duration__c
,ringdna__Call_Hour_Of_Day_Local__c
,ringdna__Call_Rating__c
,ringdna__Call_Recording_URL__c
,ringdna__Call_Recording__c
,ringdna__Created_by_RingDNA__c
,ringdna__Local_Presence_Num__c
,ringdna__Local_Presence__c
,ringdna__Queue_Hold_Time__c
,ringdna__Queue__c
,ringdna__Supervisor_Notes__c
,ringdna__Voicemail__c
,Status
,Subject
,SWT_Abandoned_Call__c
,SWT_Automated_Voicemail_Used__c
,SWT_Automated_Voicemail__c
,SWT_Called_Back__c
,SWT_Call_Direction__c
,SWT_Call_Disposition__c
,SWT_Call_Duration__c
,SWT_Call_Hour_of_Day_Local__c
,SWT_Call_Rating__c
,SWT_Call_Recording_URL__c
,SWT_Call_Result__c
,SWT_Call_Type__c
,SWT_Created_by_Ring_DNA__c
,SWT_Date_Initiated__c
,SWT_Local_Presence_Number__c
,SWT_Local_Presence__c
,SWT_Origin__c
,SWT_Pre_sales_Subtype__c
,SWT_Queue_Hold_Time__c
,SWT_Queue__c
,SWT_Reason_Closed__c
,SWT_Voicemail__c
,SystemModstamp
,TaskSubtype
,WhatCount
,WhatId
,WhoCount
,WhoId
,SWT_Approval_Type__c
,SWT_Completion_Date__c
,SWT_Referral_partner__c
,SWT_Sourced_Partner__c
,RecordTypeId
,SYSDATE
FROM SF_Task_stg_Tmp JOIN SF_Task_stg_Tmp_Key ON SF_Task_stg_Tmp.id= SF_Task_stg_Tmp_Key.id AND SF_Task_stg_Tmp.LastModifiedDate=SF_Task_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_Task BASE
WHERE SF_Task_stg_Tmp.id = BASE.id);


/* Inserting new audit entry with all stats */

INSERT INTO swt_rpt_stg.FAST_LD_AUDT
(
SUBJECT_AREA
,TBL_NM
,LD_DT
,START_DT_TIME
,END_DT_TIME
,SRC_REC_CNT
,TGT_REC_CNT
,COMPLTN_STAT
)
select 'SFDC','SF_Task',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_Task where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */

/*DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_Task' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_Task' and  COMPLTN_STAT = 'N');
Commit;
*/

select do_tm_task('mergeout','swt_rpt_stg.SF_Task_Hist');
select do_tm_task('mergeout','swt_rpt_base.SF_Task');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_Task');


