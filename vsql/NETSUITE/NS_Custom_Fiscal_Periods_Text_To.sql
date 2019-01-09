/****
*** Script Name	  : NS_Custom_Fiscal_Periods_Text_To.sql
****Description   : Incremental data load for NS_Custom_Fiscal_Periods_Text_To
****/

/* Setting timing on**/
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Custom_Fiscal_Periods_Text_To";

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
select 'NETSUITE','NS_Custom_Fiscal_Periods_Text_To',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Custom_Fiscal_Periods_Text_To_Hist SELECT * from "swt_rpt_stg".NS_Custom_Fiscal_Periods_Text_To;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select custom_fiscal_periods_text__id,max(auto_id) as auto_id from swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To where custom_fiscal_periods_text__id in (
select custom_fiscal_periods_text__id from swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To group by custom_fiscal_periods_text__id,last_modified_date having count(1)>1)
group by custom_fiscal_periods_text__id);

delete from swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To.custom_fiscal_periods_text__id=t2.custom_fiscal_periods_text__id and swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To.auto_id<t2. auto_id);

COMMIT;

CREATE LOCAL TEMP TABLE NS_Custom_Fiscal_Periods_Text_To_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To)
SEGMENTED BY HASH(custom_fiscal_periods_text__id,last_modified_date) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To;

CREATE LOCAL TEMP TABLE NS_Custom_Fiscal_Periods_Text_To_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT custom_fiscal_periods_text__id,last_modified_date FROM swt_rpt_base.NS_Custom_Fiscal_Periods_Text_To)
SEGMENTED BY HASH(custom_fiscal_periods_text__id,last_modified_date) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT custom_fiscal_periods_text__id, max(last_modified_date) as last_modified_date FROM NS_Custom_Fiscal_Periods_Text_To_stg_Tmp group by custom_fiscal_periods_text__id)
SEGMENTED BY HASH(custom_fiscal_periods_text__id,last_modified_date) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To_Hist
(
date_created
,custom_fiscal_periods_text__ex
,custom_fiscal_periods_text__id
,custom_fiscal_periods_text__na
,is_inactive
,last_modified_date
,netsuite_accounting_period_nu
,netsuite_accounting_period_te
,netsuite_accounting_period_ye
,parent_id
,LD_DT
,SWT_INS_DT
,d_source
)
select
date_created
,custom_fiscal_periods_text__ex
,custom_fiscal_periods_text__id
,custom_fiscal_periods_text__na
,is_inactive
,last_modified_date
,netsuite_accounting_period_nu
,netsuite_accounting_period_te
,netsuite_accounting_period_ye
,parent_id
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Custom_Fiscal_Periods_Text_To WHERE custom_fiscal_periods_text__id in
(SELECT STG.custom_fiscal_periods_text__id FROM NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key STG JOIN NS_Custom_Fiscal_Periods_Text_To_base_Tmp
ON STG.custom_fiscal_periods_text__id = NS_Custom_Fiscal_Periods_Text_To_base_Tmp.custom_fiscal_periods_text__id AND STG.last_modified_date >= NS_Custom_Fiscal_Periods_Text_To_base_Tmp.last_modified_date);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Custom_Fiscal_Periods_Text_To_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Custom_Fiscal_Periods_Text_To WHERE custom_fiscal_periods_text__id in
(SELECT STG.custom_fiscal_periods_text__id FROM NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key STG JOIN NS_Custom_Fiscal_Periods_Text_To_base_Tmp
ON STG.custom_fiscal_periods_text__id = NS_Custom_Fiscal_Periods_Text_To_base_Tmp.custom_fiscal_periods_text__id AND STG.last_modified_date >= NS_Custom_Fiscal_Periods_Text_To_base_Tmp.last_modified_date);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Custom_Fiscal_Periods_Text_To
(
date_created
,custom_fiscal_periods_text__ex
,custom_fiscal_periods_text__id
,custom_fiscal_periods_text__na
,is_inactive
,last_modified_date
,netsuite_accounting_period_nu
,netsuite_accounting_period_te
,netsuite_accounting_period_ye
,parent_id
,SWT_INS_DT
)
SELECT DISTINCT
date_created
,custom_fiscal_periods_text__ex
,NS_Custom_Fiscal_Periods_Text_To_stg_Tmp.custom_fiscal_periods_text__id
,custom_fiscal_periods_text__na
,is_inactive
,NS_Custom_Fiscal_Periods_Text_To_stg_Tmp.last_modified_date
,netsuite_accounting_period_nu
,netsuite_accounting_period_te
,netsuite_accounting_period_ye
,parent_id
,SYSDATE AS SWT_INS_DT
FROM NS_Custom_Fiscal_Periods_Text_To_stg_Tmp JOIN NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key ON NS_Custom_Fiscal_Periods_Text_To_stg_Tmp.custom_fiscal_periods_text__id= NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key.custom_fiscal_periods_text__id AND NS_Custom_Fiscal_Periods_Text_To_stg_Tmp.last_modified_date=NS_Custom_Fiscal_Periods_Text_To_stg_Tmp_Key.last_modified_date
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Custom_Fiscal_Periods_Text_To BASE
WHERE NS_Custom_Fiscal_Periods_Text_To_stg_Tmp.custom_fiscal_periods_text__id = BASE.custom_fiscal_periods_text__id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Custom_Fiscal_Periods_Text_To' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Custom_Fiscal_Periods_Text_To' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Custom_Fiscal_Periods_Text_To',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Custom_Fiscal_Periods_Text_To where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Custom_Fiscal_Periods_Text_To');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Custom_Fiscal_Periods_Text_To_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_Custom_Fiscal_Periods_Text_To');




