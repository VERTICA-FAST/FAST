/****
****Script Name	  : NS_Income_Accounts.sql
****Description   : Incremental data load for NS_Income_Accounts
****/

/* Setting timing on**/
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Income_Accounts";

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
select 'NETSUITE','NS_Income_Accounts',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Income_Accounts_Hist SELECT * from "swt_rpt_stg".NS_Income_Accounts;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select income_account_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Income_Accounts where income_account_id in (
select income_account_id from swt_rpt_stg.NS_Income_Accounts group by income_account_id,date_last_modified having count(1)>1)
group by income_account_id);

delete from swt_rpt_stg.NS_Income_Accounts where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Income_Accounts.income_account_id=t2.income_account_id and swt_rpt_stg.NS_Income_Accounts.auto_id<t2. auto_id);

COMMIT;

CREATE LOCAL TEMP TABLE NS_Income_Accounts_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Income_Accounts)
SEGMENTED BY HASH(income_account_id,date_last_modified) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_Income_Accounts;

CREATE LOCAL TEMP TABLE NS_Income_Accounts_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT income_account_id,date_last_modified FROM swt_rpt_base.NS_Income_Accounts)
SEGMENTED BY HASH(income_account_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Income_Accounts_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT income_account_id, max(date_last_modified) as date_last_modified FROM NS_Income_Accounts_stg_Tmp group by income_account_id)
SEGMENTED BY HASH(income_account_id,date_last_modified) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Income_Accounts_Hist
(
account_number
,comments
,current_balance
,date_last_modified
,desription
,full_name
,income_account_extid
,income_account_id
,isinactive
,is_including_child_subs
,is_summary
,legal_name
,name
,parent_id
,LD_DT
,SWT_INS_DT
,d_source
)
select
account_number
,comments
,current_balance
,date_last_modified
,desription
,full_name
,income_account_extid
,income_account_id
,isinactive
,is_including_child_subs
,is_summary
,legal_name
,name
,parent_id
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Income_Accounts WHERE income_account_id in
(SELECT STG.income_account_id FROM NS_Income_Accounts_stg_Tmp_Key STG JOIN NS_Income_Accounts_base_Tmp
ON STG.income_account_id = NS_Income_Accounts_base_Tmp.income_account_id AND STG.date_last_modified >= NS_Income_Accounts_base_Tmp.date_last_modified);




/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Income_Accounts_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Income_Accounts WHERE income_account_id in
(SELECT STG.income_account_id FROM NS_Income_Accounts_stg_Tmp_Key STG JOIN NS_Income_Accounts_base_Tmp
ON STG.income_account_id = NS_Income_Accounts_base_Tmp.income_account_id AND STG.date_last_modified >= NS_Income_Accounts_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Income_Accounts
(
account_number
,comments
,current_balance
,date_last_modified
,desription
,full_name
,income_account_extid
,income_account_id
,isinactive
,is_including_child_subs
,is_summary
,legal_name
,name
,parent_id
,SWT_INS_DT
)
SELECT DISTINCT
account_number
,comments
,current_balance
,NS_Income_Accounts_stg_Tmp.date_last_modified
,desription
,full_name
,income_account_extid
,NS_Income_Accounts_stg_Tmp.income_account_id
,isinactive
,is_including_child_subs
,is_summary
,legal_name
,name
,parent_id
,SYSDATE AS SWT_INS_DT
FROM NS_Income_Accounts_stg_Tmp JOIN NS_Income_Accounts_stg_Tmp_Key ON NS_Income_Accounts_stg_Tmp.income_account_id= NS_Income_Accounts_stg_Tmp_Key.income_account_id AND NS_Income_Accounts_stg_Tmp.date_last_modified=NS_Income_Accounts_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Income_Accounts BASE
WHERE NS_Income_Accounts_stg_Tmp.income_account_id = BASE.income_account_id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Income_Accounts' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Income_Accounts' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Income_Accounts',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Income_Accounts where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Income_Accounts');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Income_Accounts_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_Income_Accounts');


