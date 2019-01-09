/****
****Script Name	  : NS_FAM_Asset_Type.sql
****Description   : Incremental data load for NS_FAM_Asset_Type
****/


/**setting timing on**/
\timing

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_FAM_Asset_Type";

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
select 'NETSUITE','NS_FAM_Asset_Type',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;


INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_FAM_Asset_Type_Hist SELECT * from "swt_rpt_stg".NS_FAM_Asset_Type;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select fam_asset_type_id,max(auto_id) as auto_id from swt_rpt_stg.NS_FAM_Asset_Type where fam_asset_type_id in (
select fam_asset_type_id from swt_rpt_stg.NS_FAM_Asset_Type group by fam_asset_type_id,last_modified_date having count(1)>1)
group by fam_asset_type_id);

delete from swt_rpt_stg.NS_FAM_Asset_Type where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_FAM_Asset_Type.fam_asset_type_id=t2.fam_asset_type_id and swt_rpt_stg.NS_FAM_Asset_Type.auto_id<t2. auto_id);

COMMIT; 


CREATE LOCAL TEMP TABLE NS_FAM_Asset_Type_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_FAM_Asset_Type)
SEGMENTED BY HASH(fam_asset_type_id,last_modified_date) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_FAM_Asset_Type;

CREATE LOCAL TEMP TABLE NS_FAM_Asset_Type_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT fam_asset_type_id,last_modified_date FROM swt_rpt_base.NS_FAM_Asset_Type)
SEGMENTED BY HASH(fam_asset_type_id,last_modified_date) ALL NODES;


CREATE LOCAL TEMP TABLE NS_FAM_Asset_Type_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT fam_asset_type_id, max(last_modified_date) as last_modified_date FROM NS_FAM_Asset_Type_stg_Tmp group by fam_asset_type_id)
SEGMENTED BY HASH(fam_asset_type_id,last_modified_date) ALL NODES;



/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_FAM_Asset_Type_Hist
(
accounting_method_id
,asset_account_id
,asset_account_last_checked
,asset_lifetime
,asset_type_convention_1_id
,asset_type_convention_2_id
,asset_type_convention_3_id
,asset_type_convention_4_id
,asset_type_convention_5_id
,asset_type_convention_6_id
,asset_type_convention_7_id
,asset_type_convention_8_id
,asset_type_convention_9_id
,asset_type_depreciation_met_id
,asset_type_depreciation_met__0
,asset_type_depreciation_met__1
,asset_type_depreciation_met__2
,asset_type_depreciation_met__3
,asset_type_depreciation_met__4
,asset_type_depreciation_met__5
,asset_type_depreciation_met__6
,asset_type_depreciation_met__7
,asset_type_lifetime_1
,asset_type_lifetime_2
,asset_type_lifetime_3
,asset_type_lifetime_4
,asset_type_lifetime_5
,asset_type_lifetime_6
,asset_type_lifetime_7
,asset_type_lifetime_8
,asset_type_lifetime_9
,asset_type_residual_percentag
,asset_type_residual_percenta_0
,asset_type_residual_percenta_1
,asset_type_residual_percenta_2
,asset_type_residual_percenta_3
,asset_type_residual_percenta_4
,asset_type_residual_percenta_5
,asset_type_residual_percenta_6
,asset_type_residual_percenta_7
,asset_type_store_history
,convention_id
,custodian_id
,date_created
,depreciation_account_id
,depreciation_active_id
,depreciation_charge_account_id
,depreciation_rules_id
,description
,disposal_cost_account_id
,disposal_item_id
,fam_asset_type_extid
,fam_asset_type_id
,fam_asset_type_name
,financial_year_start_id
,include_in_reports
,inspection
,inspection_period
,is_inactive
,last_modified_date
,parent_id
,residual_percentage
,revision_rules_id
,supplier
,warranty
,warranty_period
,write_down_account_id
,write_off_account_id
,LD_DT
,SWT_INS_DT
,d_source
)
select
accounting_method_id
,asset_account_id
,asset_account_last_checked
,asset_lifetime
,asset_type_convention_1_id
,asset_type_convention_2_id
,asset_type_convention_3_id
,asset_type_convention_4_id
,asset_type_convention_5_id
,asset_type_convention_6_id
,asset_type_convention_7_id
,asset_type_convention_8_id
,asset_type_convention_9_id
,asset_type_depreciation_met_id
,asset_type_depreciation_met__0
,asset_type_depreciation_met__1
,asset_type_depreciation_met__2
,asset_type_depreciation_met__3
,asset_type_depreciation_met__4
,asset_type_depreciation_met__5
,asset_type_depreciation_met__6
,asset_type_depreciation_met__7
,asset_type_lifetime_1
,asset_type_lifetime_2
,asset_type_lifetime_3
,asset_type_lifetime_4
,asset_type_lifetime_5
,asset_type_lifetime_6
,asset_type_lifetime_7
,asset_type_lifetime_8
,asset_type_lifetime_9
,asset_type_residual_percentag
,asset_type_residual_percenta_0
,asset_type_residual_percenta_1
,asset_type_residual_percenta_2
,asset_type_residual_percenta_3
,asset_type_residual_percenta_4
,asset_type_residual_percenta_5
,asset_type_residual_percenta_6
,asset_type_residual_percenta_7
,asset_type_store_history
,convention_id
,custodian_id
,date_created
,depreciation_account_id
,depreciation_active_id
,depreciation_charge_account_id
,depreciation_rules_id
,description
,disposal_cost_account_id
,disposal_item_id
,fam_asset_type_extid
,fam_asset_type_id
,fam_asset_type_name
,financial_year_start_id
,include_in_reports
,inspection
,inspection_period
,is_inactive
,last_modified_date
,parent_id
,residual_percentage
,revision_rules_id
,supplier
,warranty
,warranty_period
,write_down_account_id
,write_off_account_id
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_FAM_Asset_Type WHERE fam_asset_type_id in
(SELECT STG.fam_asset_type_id FROM NS_FAM_Asset_Type_stg_Tmp_Key STG JOIN NS_FAM_Asset_Type_base_Tmp
ON STG.fam_asset_type_id = NS_FAM_Asset_Type_base_Tmp.fam_asset_type_id AND STG.last_modified_date >= NS_FAM_Asset_Type_base_Tmp.last_modified_date);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_FAM_Asset_Type_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_FAM_Asset_Type WHERE fam_asset_type_id in
(SELECT STG.fam_asset_type_id FROM NS_FAM_Asset_Type_stg_Tmp_Key STG JOIN NS_FAM_Asset_Type_base_Tmp
ON STG.fam_asset_type_id = NS_FAM_Asset_Type_base_Tmp.fam_asset_type_id AND STG.last_modified_date >= NS_FAM_Asset_Type_base_Tmp.last_modified_date);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_FAM_Asset_Type
(
accounting_method_id
,asset_account_id
,asset_account_last_checked
,asset_lifetime
,asset_type_convention_1_id
,asset_type_convention_2_id
,asset_type_convention_3_id
,asset_type_convention_4_id
,asset_type_convention_5_id
,asset_type_convention_6_id
,asset_type_convention_7_id
,asset_type_convention_8_id
,asset_type_convention_9_id
,asset_type_depreciation_met_id
,asset_type_depreciation_met__0
,asset_type_depreciation_met__1
,asset_type_depreciation_met__2
,asset_type_depreciation_met__3
,asset_type_depreciation_met__4
,asset_type_depreciation_met__5
,asset_type_depreciation_met__6
,asset_type_depreciation_met__7
,asset_type_lifetime_1
,asset_type_lifetime_2
,asset_type_lifetime_3
,asset_type_lifetime_4
,asset_type_lifetime_5
,asset_type_lifetime_6
,asset_type_lifetime_7
,asset_type_lifetime_8
,asset_type_lifetime_9
,asset_type_residual_percentag
,asset_type_residual_percenta_0
,asset_type_residual_percenta_1
,asset_type_residual_percenta_2
,asset_type_residual_percenta_3
,asset_type_residual_percenta_4
,asset_type_residual_percenta_5
,asset_type_residual_percenta_6
,asset_type_residual_percenta_7
,asset_type_store_history
,convention_id
,custodian_id
,date_created
,depreciation_account_id
,depreciation_active_id
,depreciation_charge_account_id
,depreciation_rules_id
,description
,disposal_cost_account_id
,disposal_item_id
,fam_asset_type_extid
,fam_asset_type_id
,fam_asset_type_name
,financial_year_start_id
,include_in_reports
,inspection
,inspection_period
,is_inactive
,last_modified_date
,parent_id
,residual_percentage
,revision_rules_id
,supplier
,warranty
,warranty_period
,write_down_account_id
,write_off_account_id
,SWT_INS_DT
)
SELECT DISTINCT
accounting_method_id
,asset_account_id
,asset_account_last_checked
,asset_lifetime
,asset_type_convention_1_id
,asset_type_convention_2_id
,asset_type_convention_3_id
,asset_type_convention_4_id
,asset_type_convention_5_id
,asset_type_convention_6_id
,asset_type_convention_7_id
,asset_type_convention_8_id
,asset_type_convention_9_id
,asset_type_depreciation_met_id
,asset_type_depreciation_met__0
,asset_type_depreciation_met__1
,asset_type_depreciation_met__2
,asset_type_depreciation_met__3
,asset_type_depreciation_met__4
,asset_type_depreciation_met__5
,asset_type_depreciation_met__6
,asset_type_depreciation_met__7
,asset_type_lifetime_1
,asset_type_lifetime_2
,asset_type_lifetime_3
,asset_type_lifetime_4
,asset_type_lifetime_5
,asset_type_lifetime_6
,asset_type_lifetime_7
,asset_type_lifetime_8
,asset_type_lifetime_9
,asset_type_residual_percentag
,asset_type_residual_percenta_0
,asset_type_residual_percenta_1
,asset_type_residual_percenta_2
,asset_type_residual_percenta_3
,asset_type_residual_percenta_4
,asset_type_residual_percenta_5
,asset_type_residual_percenta_6
,asset_type_residual_percenta_7
,asset_type_store_history
,convention_id
,custodian_id
,date_created
,depreciation_account_id
,depreciation_active_id
,depreciation_charge_account_id
,depreciation_rules_id
,description
,disposal_cost_account_id
,disposal_item_id
,fam_asset_type_extid
,NS_FAM_Asset_Type_stg_Tmp.fam_asset_type_id
,fam_asset_type_name
,financial_year_start_id
,include_in_reports
,inspection
,inspection_period
,is_inactive
,NS_FAM_Asset_Type_stg_Tmp.last_modified_date
,parent_id
,residual_percentage
,revision_rules_id
,supplier
,warranty
,warranty_period
,write_down_account_id
,write_off_account_id
,SYSDATE AS SWT_INS_DT
FROM NS_FAM_Asset_Type_stg_Tmp JOIN NS_FAM_Asset_Type_stg_Tmp_Key ON NS_FAM_Asset_Type_stg_Tmp.fam_asset_type_id= NS_FAM_Asset_Type_stg_Tmp_Key.fam_asset_type_id AND NS_FAM_Asset_Type_stg_Tmp.last_modified_date=NS_FAM_Asset_Type_stg_Tmp_Key.last_modified_date
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_FAM_Asset_Type BASE
WHERE NS_FAM_Asset_Type_stg_Tmp.fam_asset_type_id = BASE.fam_asset_type_id);

/* Deleting partial audit entry */

/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_FAM_Asset_Type' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_FAM_Asset_Type' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_FAM_Asset_Type',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_FAM_Asset_Type where SWT_INS_DT::date = sysdate::date),'Y';

Commit;



SELECT PURGE_TABLE('swt_rpt_base.NS_FAM_Asset_Type');
SELECT PURGE_TABLE('swt_rpt_stg.NS_FAM_Asset_Type_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_FAM_Asset_Type');


