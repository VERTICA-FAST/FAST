/****
****Script Name   : SF_HPSW_Add_Activities_to_Plan__c.sql
****Description   : Incremental data load for SF_HPSW_Add_Activities_to_Plan__c
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_HPSW_Add_Activities_to_Plan__c";

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
select 'SFDC','SF_HPSW_Add_Activities_to_Plan__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c_Hist SELECT * from swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c where id in (
select id from swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c.id=t2.id and swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c.auto_id<t2.auto_id);

Commit; 

CREATE LOCAL TEMP TABLE SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c;

CREATE LOCAL TEMP TABLE SF_HPSW_Add_Activities_to_Plan__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_HPSW_Add_Activities_to_Plan__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c_Hist
(
CurrencyIsoCode
,Name
,HPSW_Activity_Description__c
,HPSW_Anticipated_Customers_Attending__c
,HPSW_Bridgeplan_Activity_Name__c
,HPSW_Campaign__c
,HPSW_Campaign_City__c
,HPSW_Campaign_End_Date__c
,HPSW_Campaign_Fiscal_Quarter__c
,HPSW_Campaign_Quarter__c
,HPSW_Campaign_Start_Date__c
,HPSW_Existing_Opportunity__c
,HPSW_Existing_Opportunity_Value__c
,Fiscal_Quarter__c
,HPSW_Meeting_Date__c
,HPSW_Rep_Plan__c
,HPSW_Sales_Target_Pipeline_Build__c
,HPSW_Target_Account_Name__c
,Id
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,LD_DT
,SWT_INS_DT
,d_source
)
select
CurrencyIsoCode
,Name
,HPSW_Activity_Description__c
,HPSW_Anticipated_Customers_Attending__c
,HPSW_Bridgeplan_Activity_Name__c
,HPSW_Campaign__c
,HPSW_Campaign_City__c
,HPSW_Campaign_End_Date__c
,HPSW_Campaign_Fiscal_Quarter__c
,HPSW_Campaign_Quarter__c
,HPSW_Campaign_Start_Date__c
,HPSW_Existing_Opportunity__c
,HPSW_Existing_Opportunity_Value__c
,Fiscal_Quarter__c
,HPSW_Meeting_Date__c
,HPSW_Rep_Plan__c
,HPSW_Sales_Target_Pipeline_Build__c
,HPSW_Target_Account_Name__c
,Id
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_HPSW_Add_Activities_to_Plan__c WHERE id in
(SELECT STG.id FROM SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key STG JOIN SF_HPSW_Add_Activities_to_Plan__c_base_Tmp
ON STG.id = SF_HPSW_Add_Activities_to_Plan__c_base_Tmp.id AND STG.LastModifiedDate >= SF_HPSW_Add_Activities_to_Plan__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_HPSW_Add_Activities_to_Plan__c_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_HPSW_Add_Activities_to_Plan__c WHERE id in
(SELECT STG.id FROM SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key STG JOIN SF_HPSW_Add_Activities_to_Plan__c_base_Tmp
ON STG.id = SF_HPSW_Add_Activities_to_Plan__c_base_Tmp.id AND STG.LastModifiedDate >= SF_HPSW_Add_Activities_to_Plan__c_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_HPSW_Add_Activities_to_Plan__c
(
CurrencyIsoCode
,Name
,HPSW_Activity_Description__c
,HPSW_Anticipated_Customers_Attending__c
,HPSW_Bridgeplan_Activity_Name__c
,HPSW_Campaign__c
,HPSW_Campaign_City__c
,HPSW_Campaign_End_Date__c
,HPSW_Campaign_Fiscal_Quarter__c
,HPSW_Campaign_Quarter__c
,HPSW_Campaign_Start_Date__c
,HPSW_Existing_Opportunity__c
,HPSW_Existing_Opportunity_Value__c
,Fiscal_Quarter__c
,HPSW_Meeting_Date__c
,HPSW_Rep_Plan__c
,HPSW_Sales_Target_Pipeline_Build__c
,HPSW_Target_Account_Name__c
,Id
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWT_INS_DT
)
SELECT DISTINCT
CurrencyIsoCode
,Name
,HPSW_Activity_Description__c
,HPSW_Anticipated_Customers_Attending__c
,HPSW_Bridgeplan_Activity_Name__c
,HPSW_Campaign__c
,HPSW_Campaign_City__c
,HPSW_Campaign_End_Date__c
,HPSW_Campaign_Fiscal_Quarter__c
,HPSW_Campaign_Quarter__c
,HPSW_Campaign_Start_Date__c
,HPSW_Existing_Opportunity__c
,HPSW_Existing_Opportunity_Value__c
,Fiscal_Quarter__c
,HPSW_Meeting_Date__c
,HPSW_Rep_Plan__c
,HPSW_Sales_Target_Pipeline_Build__c
,HPSW_Target_Account_Name__c
,SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp.Id
,IsDeleted
,CreatedDate
,CreatedById
,SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp.LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SYSDATE AS SWT_INS_DT
FROM SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp JOIN SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key ON SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp.id= SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key.id AND SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp.LastModifiedDate=SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_HPSW_Add_Activities_to_Plan__c BASE
WHERE SF_HPSW_Add_Activities_to_Plan__c_stg_Tmp.id = BASE.id);


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
select 'SFDC','SF_HPSW_Add_Activities_to_Plan__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_HPSW_Add_Activities_to_Plan__c where SWT_INS_DT::date = sysdate::date),'Y';

commit;
/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_HPSW_Add_Activities_to_Plan__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_HPSW_Add_Activities_to_Plan__c' and  COMPLTN_STAT = 'N');
Commit;
*/

SELECT DROP_PARTITION('swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_HPSW_Add_Activities_to_Plan__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_HPSW_Add_Activities_to_Plan__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_HPSW_Add_Activities_to_Plan__c');


