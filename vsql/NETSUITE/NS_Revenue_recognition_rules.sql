/****
****Script Name	  : NS_Revenue_recognition_rules.sql
****Description   : Incremental data load for NS_Revenue_recognition_rules
****/
/* Setting timing on**/
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Revenue_recognition_rules";

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
select 'NETSUITE','NS_Revenue_recognition_rules',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Revenue_recognition_rules_Hist SELECT * from "swt_rpt_stg".NS_Revenue_recognition_rules;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select rev_rec_rule_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Revenue_recognition_rules where rev_rec_rule_id in (
select rev_rec_rule_id from swt_rpt_stg.NS_Revenue_recognition_rules group by rev_rec_rule_id,date_last_modified having count(1)>1)
group by rev_rec_rule_id);

delete from swt_rpt_stg.NS_Revenue_recognition_rules where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Revenue_recognition_rules.rev_rec_rule_id=t2.rev_rec_rule_id and swt_rpt_stg.NS_Revenue_recognition_rules.auto_id<t2. auto_id);

COMMIT; 

CREATE LOCAL TEMP TABLE NS_Revenue_recognition_rules_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Revenue_recognition_rules)
SEGMENTED BY HASH(rev_rec_rule_id,date_last_modified) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_Revenue_recognition_rules;

CREATE LOCAL TEMP TABLE NS_Revenue_recognition_rules_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT rev_rec_rule_id,date_last_modified FROM swt_rpt_base.NS_Revenue_recognition_rules)
SEGMENTED BY HASH(rev_rec_rule_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Revenue_recognition_rules_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT rev_rec_rule_id, max(date_last_modified) as date_last_modified FROM NS_Revenue_recognition_rules_stg_Tmp group by rev_rec_rule_id)
SEGMENTED BY HASH(rev_rec_rule_id,date_last_modified) ALL NODES;




/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Revenue_recognition_rules_Hist
(
amount_source
,date_created
,date_last_modified
,end_date_change_impact
,end_date_source
,initial_amount
,name
,period_offset
,recognition_period
,reforecast_method
,rev_rec_rule_id
,rule_method
,start_date_source
,start_offset
,term_in_days
,term_in_months
,LD_DT
,SWT_INS_DT
,d_source
)
select
amount_source
,date_created
,date_last_modified
,end_date_change_impact
,end_date_source
,initial_amount
,name
,period_offset
,recognition_period
,reforecast_method
,rev_rec_rule_id
,rule_method
,start_date_source
,start_offset
,term_in_days
,term_in_months
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Revenue_recognition_rules WHERE rev_rec_rule_id in
(SELECT STG.rev_rec_rule_id FROM NS_Revenue_recognition_rules_stg_Tmp_Key STG JOIN NS_Revenue_recognition_rules_base_Tmp
ON STG.rev_rec_rule_id = NS_Revenue_recognition_rules_base_Tmp.rev_rec_rule_id AND STG.date_last_modified >= NS_Revenue_recognition_rules_base_Tmp.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Revenue_recognition_rules_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Revenue_recognition_rules WHERE rev_rec_rule_id in
(SELECT STG.rev_rec_rule_id FROM NS_Revenue_recognition_rules_stg_Tmp_Key STG JOIN NS_Revenue_recognition_rules_base_Tmp
ON STG.rev_rec_rule_id = NS_Revenue_recognition_rules_base_Tmp.rev_rec_rule_id AND STG.date_last_modified >= NS_Revenue_recognition_rules_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Revenue_recognition_rules
(
amount_source
,date_created
,date_last_modified
,end_date_change_impact
,end_date_source
,initial_amount
,name
,period_offset
,recognition_period
,reforecast_method
,rev_rec_rule_id
,rule_method
,start_date_source
,start_offset
,term_in_days
,term_in_months
,SWT_INS_DT
)
SELECT DISTINCT
amount_source
,date_created
,NS_Revenue_recognition_rules_stg_Tmp.date_last_modified
,end_date_change_impact
,end_date_source
,initial_amount
,name
,period_offset
,recognition_period
,reforecast_method
,NS_Revenue_recognition_rules_stg_Tmp.rev_rec_rule_id
,rule_method
,start_date_source
,start_offset
,term_in_days
,term_in_months
,SYSDATE AS SWT_INS_DT
FROM NS_Revenue_recognition_rules_stg_Tmp JOIN NS_Revenue_recognition_rules_stg_Tmp_Key ON NS_Revenue_recognition_rules_stg_Tmp.rev_rec_rule_id= NS_Revenue_recognition_rules_stg_Tmp_Key.rev_rec_rule_id AND NS_Revenue_recognition_rules_stg_Tmp.date_last_modified=NS_Revenue_recognition_rules_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Revenue_recognition_rules BASE
WHERE NS_Revenue_recognition_rules_stg_Tmp.rev_rec_rule_id = BASE.rev_rec_rule_id);




/* Deleting partial audit entry */

/*DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Revenue_recognition_rules' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Revenue_recognition_rules' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Revenue_recognition_rules',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Revenue_recognition_rules where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Revenue_recognition_rules');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Revenue_recognition_rules_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_Revenue_recognition_rules');


