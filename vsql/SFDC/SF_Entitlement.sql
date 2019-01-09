/****
****Script Name   : SF_Entitlement.sql
****Description   : Incremental data load for SF_Entitlement
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_Entitlement";

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
select 'SFDC','SF_Entitlement',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_Entitlement_Hist SELECT * from swt_rpt_stg.SF_Entitlement;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_Entitlement where id in (
select id from swt_rpt_stg.SF_Entitlement group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_Entitlement where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_Entitlement.id=t2.id and swt_rpt_stg.SF_Entitlement.auto_id<t2.auto_id);

Commit; 


CREATE LOCAL TEMP TABLE SF_Entitlement_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_Entitlement)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_Entitlement;

CREATE LOCAL TEMP TABLE SF_Entitlement_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_Entitlement)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_Entitlement_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_Entitlement_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_Entitlement_Hist
(
AccountId
,AssetId
,BusinessHoursId
,CasesPerEntitlement
,ContractLineItemId
,CreatedById
,CreatedDate
,EndDate
,Name
,SlaProcessId
,LastModifiedById
,LastModifiedDate
,IsPerIncident
,RemainingCases
,ServiceContractId
,StartDate
,Status
,Type
,Id
,SWT_Entitlement_ID__c
,SWT_Functional_Location__c
,SWT_Quantity__c
,SWT_Software_Service_Level_Category__c
,SWT_User_Email__c
,SWT_User_Type__c
,SWT_Account_Number__c
,SWT_Support_Level__c
,SWT_Site_Business_Name__c
,SWT_Resource_Type__c
,SWT_Resource_Name__c
,SWT_Multi_Region__c
,SWT_Local_Language__c
,SWT_Line_Item_End_Date__c
,SWT_Federal__c
,SWT_Complex_Environment__c
,SWT_30_Mins_SLO__c
,SWT_SiteStreet__c
,SWT_Product_Code__c
,SWT_AssetLineItem__c
,SWT_SiteCity__c
,SWT_SiteCountry__c
,SWT_SitePostalCode__c
,SWT_SiteState__c
,SWT_Subscription_Id__c
,SWT_BusinessArea__c
,SWT_Product_Description__c
,SWT_Product_Name__c
,SWT_Legacy_SAID__c
,SWT_Entitlement_Code__c
,SWT_Line_Item_Start_Date__c
,CurrencyIsoCode
,IsDeleted
,LastReferencedDate
,LastViewedDate
,SWT_Appliance_Serial_Number__c
,SWT_Preferred_Account_Name__c
,SystemModstamp
,SWT_Product_Type__c
,SWT_Line_Item_Status__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
AccountId
,AssetId
,BusinessHoursId
,CasesPerEntitlement
,ContractLineItemId
,CreatedById
,CreatedDate
,EndDate
,Name
,SlaProcessId
,LastModifiedById
,LastModifiedDate
,IsPerIncident
,RemainingCases
,ServiceContractId
,StartDate
,Status
,Type
,Id
,SWT_Entitlement_ID__c
,SWT_Functional_Location__c
,SWT_Quantity__c
,SWT_Software_Service_Level_Category__c
,SWT_User_Email__c
,SWT_User_Type__c
,SWT_Account_Number__c
,SWT_Support_Level__c
,SWT_Site_Business_Name__c
,SWT_Resource_Type__c
,SWT_Resource_Name__c
,SWT_Multi_Region__c
,SWT_Local_Language__c
,SWT_Line_Item_End_Date__c
,SWT_Federal__c
,SWT_Complex_Environment__c
,SWT_30_Mins_SLO__c
,SWT_SiteStreet__c
,SWT_Product_Code__c
,SWT_AssetLineItem__c
,SWT_SiteCity__c
,SWT_SiteCountry__c
,SWT_SitePostalCode__c
,SWT_SiteState__c
,SWT_Subscription_Id__c
,SWT_BusinessArea__c
,SWT_Product_Description__c
,SWT_Product_Name__c
,SWT_Legacy_SAID__c
,SWT_Entitlement_Code__c
,SWT_Line_Item_Start_Date__c
,CurrencyIsoCode
,IsDeleted
,LastReferencedDate
,LastViewedDate
,SWT_Appliance_Serial_Number__c
,SWT_Preferred_Account_Name__c
,SystemModstamp
,SWT_Product_Type__c
,SWT_Line_Item_Status__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_Entitlement WHERE id in 
(SELECT STG.id FROM SF_Entitlement_stg_Tmp_Key STG JOIN SF_Entitlement_base_Tmp
ON STG.id = SF_Entitlement_base_Tmp.id AND STG.LastModifiedDate >= SF_Entitlement_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_Entitlement_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_Entitlement WHERE id in
(SELECT STG.id FROM SF_Entitlement_stg_Tmp_Key STG JOIN SF_Entitlement_base_Tmp
ON STG.id = SF_Entitlement_base_Tmp.id AND STG.LastModifiedDate >= SF_Entitlement_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_Entitlement
(
AccountId
,AssetId
,BusinessHoursId
,CasesPerEntitlement
,ContractLineItemId
,CreatedById
,CreatedDate
,EndDate
,Name
,SlaProcessId
,LastModifiedById
,LastModifiedDate
,IsPerIncident
,RemainingCases
,ServiceContractId
,StartDate
,Status
,Type
,Id
,SWT_Entitlement_ID__c
,SWT_Functional_Location__c
,SWT_Quantity__c
,SWT_Software_Service_Level_Category__c
,SWT_User_Email__c
,SWT_User_Type__c
,SWT_Account_Number__c
,SWT_Support_Level__c
,SWT_Site_Business_Name__c
,SWT_Resource_Type__c
,SWT_Resource_Name__c
,SWT_Multi_Region__c
,SWT_Local_Language__c
,SWT_Line_Item_End_Date__c
,SWT_Federal__c
,SWT_Complex_Environment__c
,SWT_30_Mins_SLO__c
,SWT_SiteStreet__c
,SWT_Product_Code__c
,SWT_AssetLineItem__c
,SWT_SiteCity__c
,SWT_SiteCountry__c
,SWT_SitePostalCode__c
,SWT_SiteState__c
,SWT_Subscription_Id__c
,SWT_BusinessArea__c
,SWT_Product_Description__c
,SWT_Product_Name__c
,SWT_Legacy_SAID__c
,SWT_Entitlement_Code__c
,SWT_Line_Item_Start_Date__c
,CurrencyIsoCode
,IsDeleted
,LastReferencedDate
,LastViewedDate
,SWT_Appliance_Serial_Number__c
,SWT_Preferred_Account_Name__c
,SystemModstamp
,SWT_Product_Type__c
,SWT_Line_Item_Status__c
,SWT_INS_DT
)
SELECT DISTINCT 
AccountId
,AssetId
,BusinessHoursId
,CasesPerEntitlement
,ContractLineItemId
,CreatedById
,CreatedDate
,EndDate
,Name
,SlaProcessId
,LastModifiedById
,SF_Entitlement_stg_Tmp.LastModifiedDate
,IsPerIncident
,RemainingCases
,ServiceContractId
,StartDate
,Status
,Type
,SF_Entitlement_stg_Tmp.Id
,SWT_Entitlement_ID__c
,SWT_Functional_Location__c
,SWT_Quantity__c
,SWT_Software_Service_Level_Category__c
,SWT_User_Email__c
,SWT_User_Type__c
,SWT_Account_Number__c
,SWT_Support_Level__c
,SWT_Site_Business_Name__c
,SWT_Resource_Type__c
,SWT_Resource_Name__c
,SWT_Multi_Region__c
,SWT_Local_Language__c
,SWT_Line_Item_End_Date__c
,SWT_Federal__c
,SWT_Complex_Environment__c
,SWT_30_Mins_SLO__c
,SWT_SiteStreet__c
,SWT_Product_Code__c
,SWT_AssetLineItem__c
,SWT_SiteCity__c
,SWT_SiteCountry__c
,SWT_SitePostalCode__c
,SWT_SiteState__c
,SWT_Subscription_Id__c
,SWT_BusinessArea__c
,SWT_Product_Description__c
,SWT_Product_Name__c
,SWT_Legacy_SAID__c
,SWT_Entitlement_Code__c
,SWT_Line_Item_Start_Date__c
,CurrencyIsoCode
,IsDeleted
,LastReferencedDate
,LastViewedDate
,SWT_Appliance_Serial_Number__c
,SWT_Preferred_Account_Name__c
,SystemModstamp
,SWT_Product_Type__c
,SWT_Line_Item_Status__c
,SYSDATE AS SWT_INS_DT
FROM SF_Entitlement_stg_Tmp JOIN SF_Entitlement_stg_Tmp_Key ON SF_Entitlement_stg_Tmp.id= SF_Entitlement_stg_Tmp_Key.id AND SF_Entitlement_stg_Tmp.LastModifiedDate=SF_Entitlement_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_Entitlement BASE
WHERE SF_Entitlement_stg_Tmp.id = BASE.id);




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
select 'SFDC','SF_Entitlement',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_Entitlement where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_Entitlement' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_Entitlement' and  COMPLTN_STAT = 'N');

commit;
*/

SELECT DROP_PARTITION('swt_rpt_stg.SF_Entitlement_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_Entitlement_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_Entitlement');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_Entitlement');



