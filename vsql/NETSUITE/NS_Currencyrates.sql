/****
****Script Name	  : NS_Currencyrates.sql
****Description   : Incremental data load for NS_Currencyrates
****/

/* Setting timing on**/
\timing
/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Currencyrates";

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
select 'NETSUITE','NS_Currencyrates',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Currencyrates_Hist SELECT * from "swt_rpt_stg".NS_Currencyrates;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select currencyrate_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Currencyrates where currencyrate_id in (
select currencyrate_id from swt_rpt_stg.NS_Currencyrates group by currencyrate_id,date_last_modified having count(1)>1)
group by currencyrate_id);


delete from swt_rpt_stg.NS_Currencyrates where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Currencyrates.currencyrate_id=t2.currencyrate_id and swt_rpt_stg.NS_Currencyrates.auto_id<t2.auto_id);

commit;


CREATE LOCAL TEMP TABLE NS_Currencyrates_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Currencyrates)
SEGMENTED BY HASH(currencyrate_id,date_last_modified) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.NS_Currencyrates;

CREATE LOCAL TEMP TABLE NS_Currencyrates_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT currencyrate_id,date_last_modified FROM swt_rpt_base.NS_Currencyrates)
SEGMENTED BY HASH(currencyrate_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Currencyrates_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT currencyrate_id, max(date_last_modified) as date_last_modified FROM NS_Currencyrates_stg_Tmp group by currencyrate_id)
SEGMENTED BY HASH(currencyrate_id,date_last_modified) ALL NODES;

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Currencyrates_Hist
(
anchor_currency_id
,base_currency_id
,currency_id
,currencyrate_id
,currencyrate_provider_id
,date_effective
,date_last_modified
,exchange_rate
,is_anchor_only
,update_method_id
,LD_DT
,SWT_INS_DT
,d_source
)
select
anchor_currency_id
,base_currency_id
,currency_id
,currencyrate_id
,currencyrate_provider_id
,date_effective
,date_last_modified
,exchange_rate
,is_anchor_only
,update_method_id
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Currencyrates WHERE currencyrate_id in
(SELECT STG.currencyrate_id FROM NS_Currencyrates_stg_Tmp_Key STG JOIN NS_Currencyrates_base_Tmp
ON STG.currencyrate_id = NS_Currencyrates_base_Tmp.currencyrate_id AND STG.date_last_modified >= NS_Currencyrates_base_Tmp.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Currencyrates_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Currencyrates WHERE currencyrate_id in
(SELECT STG.currencyrate_id FROM NS_Currencyrates_stg_Tmp_Key STG JOIN NS_Currencyrates_base_Tmp
ON STG.currencyrate_id = NS_Currencyrates_base_Tmp.currencyrate_id AND STG.date_last_modified >= NS_Currencyrates_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Currencyrates
(
anchor_currency_id
,base_currency_id
,currency_id
,currencyrate_id
,currencyrate_provider_id
,date_effective
,date_last_modified
,exchange_rate
,is_anchor_only
,update_method_id
,SWT_INS_DT
)
SELECT DISTINCT 
anchor_currency_id
,base_currency_id
,currency_id
,NS_Currencyrates_stg_Tmp.currencyrate_id
,currencyrate_provider_id
,date_effective
,NS_Currencyrates_stg_Tmp.date_last_modified
,exchange_rate
,is_anchor_only
,update_method_id
,sysdate as SWT_INS_DT
FROM NS_Currencyrates_stg_Tmp JOIN NS_Currencyrates_stg_Tmp_Key ON NS_Currencyrates_stg_Tmp.currencyrate_id= NS_Currencyrates_stg_Tmp_Key.currencyrate_id AND NS_Currencyrates_stg_Tmp.date_last_modified=NS_Currencyrates_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Currencyrates BASE
WHERE NS_Currencyrates_stg_Tmp.currencyrate_id = BASE.currencyrate_id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Currencyrates' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Currencyrates' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Currencyrates',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Currencyrates where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Currencyrates');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Currencyrates_Hist');
select ANALYZE_STATISTICS('swt_rpt_base.NS_Currencyrates');


