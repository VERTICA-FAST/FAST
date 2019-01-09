/****
****Script Name	  : NS_Accounts.sql
****Description   : Incremental data load for NS_Accounts
****/
/* Setting timing on**/
\timing
/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Accounts";

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
select 'NETSUITE','NS_Accounts',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Accounts_Hist SELECT * from "swt_rpt_stg".NS_Accounts;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select account_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Accounts where account_id in (
select account_id from swt_rpt_stg.NS_Accounts group by account_id,date_last_modified having count(1)>1)
group by account_id);


delete from swt_rpt_stg.NS_Accounts where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Accounts.account_id=t2.account_id and swt_rpt_stg.NS_Accounts.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE NS_Accounts_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Accounts)
SEGMENTED BY HASH(account_id,date_last_modified) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.NS_Accounts;


CREATE LOCAL TEMP TABLE NS_Accounts_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT account_id,date_last_modified FROM swt_rpt_base.NS_Accounts)
SEGMENTED BY HASH(account_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Accounts_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT account_id, max(date_last_modified) as date_last_modified FROM NS_Accounts_stg_Tmp group by account_id)
SEGMENTED BY HASH(account_id,date_last_modified) ALL NODES;

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Accounts_Hist
(
account_extid
,account_id
,accountnumber
,cashflow_rate_type
,category_1099_misc
,category_1099_misc_mthreshold
,class_id
,currency_id
,date_last_modified
,deferral_account_id
,department_id
,description
,full_description
,full_name
,general_rate_type
,is_balancesheet
,is_included_in_elimination
,is_included_in_reval
,is_including_child_subs
,is_leftside
,is_summary
,isinactive
,legal_name
,location_id
,name
,openbalance
,parent_id
,type_name
,type_sequence
,functional_area_id
,idt__default_expense_account
,idt__default_liability_accoun
,net_monetary_asset_account
,treasury_entity_sub
,ap_payment
,OPENAIR_COST_CATEGORY_ID
,LD_DT
,SWT_INS_DT
,d_source
)
select
account_extid
,account_id
,accountnumber
,cashflow_rate_type
,category_1099_misc
,category_1099_misc_mthreshold
,class_id
,currency_id
,date_last_modified
,deferral_account_id
,department_id
,description
,full_description
,full_name
,general_rate_type
,is_balancesheet
,is_included_in_elimination
,is_included_in_reval
,is_including_child_subs
,is_leftside
,is_summary
,isinactive
,legal_name
,location_id
,name
,openbalance
,parent_id
,type_name
,type_sequence
,functional_area_id
,idt__default_expense_account
,idt__default_liability_accoun
,net_monetary_asset_account
,treasury_entity_sub
,ap_payment
,OPENAIR_COST_CATEGORY_ID
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Accounts WHERE account_id in
(SELECT STG.account_id FROM NS_Accounts_stg_Tmp_Key STG JOIN NS_Accounts_base_Tmp
ON STG.account_id = NS_Accounts_base_Tmp.account_id AND STG.date_last_modified >= NS_Accounts_base_Tmp.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Accounts_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Accounts WHERE account_id in
(SELECT STG.account_id FROM NS_Accounts_stg_Tmp_Key STG JOIN NS_Accounts_base_Tmp
ON STG.account_id = NS_Accounts_base_Tmp.account_id AND STG.date_last_modified >= NS_Accounts_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Accounts
(
account_extid
,account_id
,accountnumber
,cashflow_rate_type
,category_1099_misc
,category_1099_misc_mthreshold
,class_id
,currency_id
,date_last_modified
,deferral_account_id
,department_id
,description
,full_description
,full_name
,general_rate_type
,is_balancesheet
,is_included_in_elimination
,is_included_in_reval
,is_including_child_subs
,is_leftside
,is_summary
,isinactive
,legal_name
,location_id
,name
,openbalance
,parent_id
,type_name
,type_sequence
,functional_area_id
,idt__default_expense_account
,idt__default_liability_accoun
,net_monetary_asset_account
,treasury_entity_sub
,ap_payment
,OPENAIR_COST_CATEGORY_ID
,SWT_INS_DT
)
SELECT DISTINCT 
account_extid
,NS_Accounts_stg_Tmp.account_id
,accountnumber
,cashflow_rate_type
,category_1099_misc
,category_1099_misc_mthreshold
,class_id
,currency_id
,NS_Accounts_stg_Tmp.date_last_modified
,deferral_account_id
,department_id
,description
,full_description
,full_name
,general_rate_type
,is_balancesheet
,is_included_in_elimination
,is_included_in_reval
,is_including_child_subs
,is_leftside
,is_summary
,isinactive
,legal_name
,location_id
,name
,openbalance
,parent_id
,type_name
,type_sequence
,functional_area_id
,idt__default_expense_account
,idt__default_liability_accoun
,net_monetary_asset_account
,treasury_entity_sub
,ap_payment
,OPENAIR_COST_CATEGORY_ID
,sysdate as SWT_INS_DT
FROM NS_Accounts_stg_Tmp JOIN NS_Accounts_stg_Tmp_Key ON NS_Accounts_stg_Tmp.account_id= NS_Accounts_stg_Tmp_Key.account_id AND NS_Accounts_stg_Tmp.date_last_modified=NS_Accounts_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Accounts BASE
WHERE NS_Accounts_stg_Tmp.account_id = BASE.account_id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Accounts' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Accounts' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Accounts',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Accounts where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

SELECT DO_TM_TASK('mergeout',  'swt_rpt_base.NS_Accounts');
SELECT DO_TM_TASK('mergeout',  'swt_rpt_stg.NS_Accounts_Hist');
select ANALYZE_STATISTICS('swt_rpt_base.NS_Accounts');




