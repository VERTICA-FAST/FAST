/****
****Script Name	  : NS_Accounting_Periods.sql
****Description   : Incremental data load for NS_Accounting_Periods
****/

/* Setting timing on**/
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count,sysdate st from "swt_rpt_stg"."NS_Accounting_Periods";

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
select 'NETSUITE','NS_Accounting_Periods',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Accounting_Periods_Hist SELECT * from "swt_rpt_stg".NS_Accounting_Periods;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select accounting_period_id,fiscal_calendar_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Accounting_Periods where (accounting_period_id,fiscal_calendar_id) in (
select accounting_period_id,fiscal_calendar_id from swt_rpt_stg.NS_Accounting_Periods group by accounting_period_id,fiscal_calendar_id ,date_last_modified having count(1)>1)
group by accounting_period_id,fiscal_calendar_id );


delete from swt_rpt_stg.NS_Accounting_Periods where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Accounting_Periods.accounting_period_id=t2.accounting_period_id and swt_rpt_stg.NS_Accounting_Periods.fiscal_calendar_id=t2.fiscal_calendar_id and swt_rpt_stg.NS_Accounting_Periods.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE NS_Accounting_Periods_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Accounting_Periods)
SEGMENTED BY HASH(accounting_period_id,fiscal_calendar_id,date_last_modified) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.NS_Accounting_Periods;


CREATE LOCAL TEMP TABLE NS_Accounting_Periods_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT accounting_period_id,fiscal_calendar_id,date_last_modified FROM swt_rpt_base.NS_Accounting_Periods)
SEGMENTED BY HASH(accounting_period_id,fiscal_calendar_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Accounting_Periods_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT accounting_period_id,fiscal_calendar_id, max(date_last_modified) as date_last_modified FROM NS_Accounting_Periods_stg_Tmp group by accounting_period_id,fiscal_calendar_id)
SEGMENTED BY HASH(accounting_period_id,fiscal_calendar_id,date_last_modified) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Accounting_Periods_Hist
(
accounting_period_id
,closed
,closed_accounts_payable
,closed_accounts_receivable
,closed_all
,closed_on
,closed_payroll
,date_last_modified
,ending
,fiscal_calendar_id
,full_name
,isinactive
,locked_accounts_payable
,locked_accounts_receivable
,locked_all
,locked_payroll
,name
,parent_id
,quarter
,starting
,year_0
,year_id
,LD_DT
,SWT_INS_DT
,d_source
)
select
accounting_period_id
,closed
,closed_accounts_payable
,closed_accounts_receivable
,closed_all
,closed_on
,closed_payroll
,date_last_modified
,ending
,fiscal_calendar_id
,full_name
,isinactive
,locked_accounts_payable
,locked_accounts_receivable
,locked_all
,locked_payroll
,name
,parent_id
,quarter
,starting
,year_0
,year_id
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Accounting_Periods WHERE EXISTS
(SELECT 1 FROM NS_Accounting_Periods_stg_Tmp_Key STG
WHERE STG.accounting_period_id = NS_Accounting_Periods.accounting_period_id AND STG.fiscal_calendar_id = NS_Accounting_Periods.fiscal_calendar_id AND STG.date_last_modified >= NS_Accounting_Periods.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Accounting_Periods_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Accounting_Periods WHERE EXISTS
(SELECT 1 FROM NS_Accounting_Periods_stg_Tmp_Key STG
WHERE STG.accounting_period_id = "swt_rpt_base".NS_Accounting_Periods.accounting_period_id And STG.fiscal_calendar_id="swt_rpt_base".NS_Accounting_Periods.fiscal_calendar_id AND STG.date_last_modified >= "swt_rpt_base".NS_Accounting_Periods.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Accounting_Periods
(
accounting_period_id
,closed
,closed_accounts_payable
,closed_accounts_receivable
,closed_all
,closed_on
,closed_payroll
,date_last_modified
,ending
,fiscal_calendar_id
,full_name
,isinactive
,locked_accounts_payable
,locked_accounts_receivable
,locked_all
,locked_payroll
,name
,parent_id
,quarter
,starting
,year_0
,year_id
,SWT_INS_DT
)
SELECT DISTINCT 
NS_Accounting_Periods_stg_Tmp.accounting_period_id
,closed
,closed_accounts_payable
,closed_accounts_receivable
,closed_all
,closed_on
,closed_payroll
,NS_Accounting_Periods_stg_Tmp.date_last_modified
,ending
,NS_Accounting_Periods_stg_Tmp.fiscal_calendar_id
,full_name
,isinactive
,locked_accounts_payable
,locked_accounts_receivable
,locked_all
,locked_payroll
,name
,parent_id
,quarter
,starting
,year_0
,year_id
,SYSDATE AS SWT_INS_DT
FROM NS_Accounting_Periods_stg_Tmp JOIN NS_Accounting_Periods_stg_Tmp_Key ON NS_Accounting_Periods_stg_Tmp.accounting_period_id= NS_Accounting_Periods_stg_Tmp_Key.accounting_period_id AND NS_Accounting_Periods_stg_Tmp.fiscal_calendar_id= NS_Accounting_Periods_stg_Tmp_Key.fiscal_calendar_id AND NS_Accounting_Periods_stg_Tmp.date_last_modified=NS_Accounting_Periods_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Accounting_Periods BASE
WHERE NS_Accounting_Periods_stg_Tmp.accounting_period_id= BASE.accounting_period_id AND NS_Accounting_Periods_stg_Tmp.fiscal_calendar_id= BASE.fiscal_calendar_id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Accounting_Periods' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Accounting_Periods' and  COMPLTN_STAT = 'N');
*/

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
select 'NETSUITE','NS_Accounting_Periods',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Accounting_Periods where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT DO_TM_TASK('mergeout',  'swt_rpt_base.NS_Accounting_Periods');
SELECT DO_TM_TASK('mergeout',  'swt_rpt_stg.NS_Accounting_Periods_Hist');
select ANALYZE_STATISTICS('swt_rpt_base.NS_Accounting_Periods');


