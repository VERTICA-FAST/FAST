/****
****Script Name	  : Incr_AT_Apttus_QPConfig__ProposalProductAttributeValue__c.sql
****Description   : Incremental data load for AT_Apttus_QPConfig__ProposalProductAttributeValue__c
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_Apttus_QPConfig__ProposalProductAttributeValue__c";
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
select 'APTTUS','AT_Apttus_QPConfig__ProposalProductAttributeValue__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;  

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_Apttus_QPConfig__ProposalProductAttributeValue__c_Hist select * from "swt_rpt_stg".AT_Apttus_QPConfig__ProposalProductAttributeValue__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c where id in (
select id from swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c.id=t2.id and swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c;

CREATE LOCAL TEMP TABLE AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.AT_Apttus_QPConfig__ProposalProductAttributeValue__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


/* Inserting deleted data into the Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c_Hist
(
Id
,Apttus_QPConfig__LineItemId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,SWT_Net_Record_Replay_Protocol__c
,SWT_BillingFrequency__c
,Apttus_QPConfig__BillingOffsetDays__c
,Apttus_QPConfig__Color__c
,SWT_Conf_Service_internal__c
,SWT_Conf_Service_external__c
,SWT_Database_Protocol__c
,SWT_DefaultQuantity__c
,SWT_Entitled_Quantity__c
,Ordered_Quantity__c
,HPE_ConnectedMX_GracePeriod__c
,HPE_ConnectedMX_RampUp__c
,Is_PS_Sales__c
,SWT_Load_Generators_net__c
,SWT_Load_Generators_database__c
,SWT_Load_Generators_web__c
,SWT_Load_Generators_mobile__c
,SWT_Location_internal__c
,SWT_Location_external__c
,Minimum_Commitment__c
,SWT_Mobile_Protocol__c
,Number_of_tests_DEV__c
,Number_of_tests_UI__c
,Number_of_tests_API__c
,SWT_Number_of_Unique_Transactions__c
,Parent_List_Price__c
,SWT_Protocol_User_Qty_net__c
,SWT_Protocol_User_Qty_Database__c
,SWT_Protocol_User_Qty_web__c
,SWT_Protocol_User_Qty_mobile__c
,SWT_Purchase_Type__c
,Test_Quantity_Update__c
,SWT_Quote_Purchase_Type__c
,Recommended_VUH_API__c
,Recommended_VUH_DEV__c
,Recommended_VUH_UI__c
,SWT_SupportBillingFrequency__c
,SWT_SupportLevel__c
,SWT_SupportTerm__c
,SWT_Term__c
,Test_duration_in_Hours_API__c
,Test_duration_in_Hours_DEV__c
,Test_duration_in_Hours_UI__c
,Test_LL__c
,SWT_Total_External_BPM__c
,SWT_Total_Internal_BPM__c
,SWT_Total_Premium_Vuser__c
,SWT_Total_Standard_Vuser__c
,SWT_Transactions_external__c
,SWT_Transactions_internal__c
,Variable_Commitment__c
,Apttus_QPConfig__Vendor__c
,Virtual_Users_per_Test_UI__c
,Virtual_Users_per_Test_DEV__c
,Virtual_Users_per_Test_API__c
,VUH_Override_UI__c
,VUH_Override_DEV__c
,VUH_Override_API__c
,SWT_Web_2_0_Protocol__c
,LastModifiedDate
,SWT_Content_Subscription__c
,SWT_List_Price__c
,SWT_Content_Subscription_PL__c
,SWT_Is_Partner__c
,SWT_RepSM__c
,SWT_Custom_Support_Attribute__c
,LD_DT
,SWT_INS_DT
,d_source
)
select 
Id
,Apttus_QPConfig__LineItemId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,SWT_Net_Record_Replay_Protocol__c
,SWT_BillingFrequency__c
,Apttus_QPConfig__BillingOffsetDays__c
,Apttus_QPConfig__Color__c
,SWT_Conf_Service_internal__c
,SWT_Conf_Service_external__c
,SWT_Database_Protocol__c
,SWT_DefaultQuantity__c
,SWT_Entitled_Quantity__c
,Ordered_Quantity__c
,HPE_ConnectedMX_GracePeriod__c
,HPE_ConnectedMX_RampUp__c
,Is_PS_Sales__c
,SWT_Load_Generators_net__c
,SWT_Load_Generators_database__c
,SWT_Load_Generators_web__c
,SWT_Load_Generators_mobile__c
,SWT_Location_internal__c
,SWT_Location_external__c
,Minimum_Commitment__c
,SWT_Mobile_Protocol__c
,Number_of_tests_DEV__c
,Number_of_tests_UI__c
,Number_of_tests_API__c
,SWT_Number_of_Unique_Transactions__c
,Parent_List_Price__c
,SWT_Protocol_User_Qty_net__c
,SWT_Protocol_User_Qty_Database__c
,SWT_Protocol_User_Qty_web__c
,SWT_Protocol_User_Qty_mobile__c
,SWT_Purchase_Type__c
,Test_Quantity_Update__c
,SWT_Quote_Purchase_Type__c
,Recommended_VUH_API__c
,Recommended_VUH_DEV__c
,Recommended_VUH_UI__c
,SWT_SupportBillingFrequency__c
,SWT_SupportLevel__c
,SWT_SupportTerm__c
,SWT_Term__c
,Test_duration_in_Hours_API__c
,Test_duration_in_Hours_DEV__c
,Test_duration_in_Hours_UI__c
,Test_LL__c
,SWT_Total_External_BPM__c
,SWT_Total_Internal_BPM__c
,SWT_Total_Premium_Vuser__c
,SWT_Total_Standard_Vuser__c
,SWT_Transactions_external__c
,SWT_Transactions_internal__c
,Variable_Commitment__c
,Apttus_QPConfig__Vendor__c
,Virtual_Users_per_Test_UI__c
,Virtual_Users_per_Test_DEV__c
,Virtual_Users_per_Test_API__c
,VUH_Override_UI__c
,VUH_Override_DEV__c
,VUH_Override_API__c
,SWT_Web_2_0_Protocol__c
,LastModifiedDate
,SWT_Content_Subscription__c
,SWT_List_Price__c
,SWT_Content_Subscription_PL__c
,SWT_Is_Partner__c
,SWT_RepSM__c
,SWT_Custom_Support_Attribute__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_Apttus_QPConfig__ProposalProductAttributeValue__c WHERE id in
(SELECT STG.id FROM AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key STG JOIN AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp
ON STG.id = AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_Apttus_QPConfig__ProposalProductAttributeValue__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_Apttus_QPConfig__ProposalProductAttributeValue__c WHERE id in
(SELECT STG.id FROM AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key STG JOIN AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp
ON STG.id = AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_QPConfig__ProposalProductAttributeValue__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_Apttus_QPConfig__ProposalProductAttributeValue__c
(
Id
,Apttus_QPConfig__LineItemId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,SWT_Net_Record_Replay_Protocol__c
,SWT_BillingFrequency__c
,Apttus_QPConfig__BillingOffsetDays__c
,Apttus_QPConfig__Color__c
,SWT_Conf_Service_internal__c
,SWT_Conf_Service_external__c
,SWT_Database_Protocol__c
,SWT_DefaultQuantity__c
,SWT_Entitled_Quantity__c
,Ordered_Quantity__c
,HPE_ConnectedMX_GracePeriod__c
,HPE_ConnectedMX_RampUp__c
,Is_PS_Sales__c
,SWT_Load_Generators_net__c
,SWT_Load_Generators_database__c
,SWT_Load_Generators_web__c
,SWT_Load_Generators_mobile__c
,SWT_Location_internal__c
,SWT_Location_external__c
,Minimum_Commitment__c
,SWT_Mobile_Protocol__c
,Number_of_tests_DEV__c
,Number_of_tests_UI__c
,Number_of_tests_API__c
,SWT_Number_of_Unique_Transactions__c
,Parent_List_Price__c
,SWT_Protocol_User_Qty_net__c
,SWT_Protocol_User_Qty_Database__c
,SWT_Protocol_User_Qty_web__c
,SWT_Protocol_User_Qty_mobile__c
,SWT_Purchase_Type__c
,Test_Quantity_Update__c
,SWT_Quote_Purchase_Type__c
,Recommended_VUH_API__c
,Recommended_VUH_DEV__c
,Recommended_VUH_UI__c
,SWT_SupportBillingFrequency__c
,SWT_SupportLevel__c
,SWT_SupportTerm__c
,SWT_Term__c
,Test_duration_in_Hours_API__c
,Test_duration_in_Hours_DEV__c
,Test_duration_in_Hours_UI__c
,Test_LL__c
,SWT_Total_External_BPM__c
,SWT_Total_Internal_BPM__c
,SWT_Total_Premium_Vuser__c
,SWT_Total_Standard_Vuser__c
,SWT_Transactions_external__c
,SWT_Transactions_internal__c
,Variable_Commitment__c
,Apttus_QPConfig__Vendor__c
,Virtual_Users_per_Test_UI__c
,Virtual_Users_per_Test_DEV__c
,Virtual_Users_per_Test_API__c
,VUH_Override_UI__c
,VUH_Override_DEV__c
,VUH_Override_API__c
,SWT_Web_2_0_Protocol__c
,LastModifiedDate
,SWT_Content_Subscription__c
,SWT_List_Price__c
,SWT_Content_Subscription_PL__c
,SWT_Is_Partner__c
,SWT_RepSM__c
,SWT_Custom_Support_Attribute__c
,SWT_INS_DT 
)
SELECT DISTINCT  
AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp.Id
,Apttus_QPConfig__LineItemId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,SWT_Net_Record_Replay_Protocol__c
,SWT_BillingFrequency__c
,Apttus_QPConfig__BillingOffsetDays__c
,Apttus_QPConfig__Color__c
,SWT_Conf_Service_internal__c
,SWT_Conf_Service_external__c
,SWT_Database_Protocol__c
,SWT_DefaultQuantity__c
,SWT_Entitled_Quantity__c
,Ordered_Quantity__c
,HPE_ConnectedMX_GracePeriod__c
,HPE_ConnectedMX_RampUp__c
,Is_PS_Sales__c
,SWT_Load_Generators_net__c
,SWT_Load_Generators_database__c
,SWT_Load_Generators_web__c
,SWT_Load_Generators_mobile__c
,SWT_Location_internal__c
,SWT_Location_external__c
,Minimum_Commitment__c
,SWT_Mobile_Protocol__c
,Number_of_tests_DEV__c
,Number_of_tests_UI__c
,Number_of_tests_API__c
,SWT_Number_of_Unique_Transactions__c
,Parent_List_Price__c
,SWT_Protocol_User_Qty_net__c
,SWT_Protocol_User_Qty_Database__c
,SWT_Protocol_User_Qty_web__c
,SWT_Protocol_User_Qty_mobile__c
,SWT_Purchase_Type__c
,Test_Quantity_Update__c
,SWT_Quote_Purchase_Type__c
,Recommended_VUH_API__c
,Recommended_VUH_DEV__c
,Recommended_VUH_UI__c
,SWT_SupportBillingFrequency__c
,SWT_SupportLevel__c
,SWT_SupportTerm__c
,SWT_Term__c
,Test_duration_in_Hours_API__c
,Test_duration_in_Hours_DEV__c
,Test_duration_in_Hours_UI__c
,Test_LL__c
,SWT_Total_External_BPM__c
,SWT_Total_Internal_BPM__c
,SWT_Total_Premium_Vuser__c
,SWT_Total_Standard_Vuser__c
,SWT_Transactions_external__c
,SWT_Transactions_internal__c
,Variable_Commitment__c
,Apttus_QPConfig__Vendor__c
,Virtual_Users_per_Test_UI__c
,Virtual_Users_per_Test_DEV__c
,Virtual_Users_per_Test_API__c
,VUH_Override_UI__c
,VUH_Override_DEV__c
,VUH_Override_API__c
,SWT_Web_2_0_Protocol__c
,AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp.LastModifiedDate
,SWT_Content_Subscription__c
,SWT_List_Price__c
,SWT_Content_Subscription_PL__c
,SWT_Is_Partner__c
,SWT_RepSM__c
,SWT_Custom_Support_Attribute__c
,sysdate as SWT_INS_DT 
FROM AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp JOIN AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key ON AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp.id= AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key.id AND AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp.LastModifiedDate=AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".AT_Apttus_QPConfig__ProposalProductAttributeValue__c BASE
WHERE AT_Apttus_QPConfig__ProposalProductAttributeValue__c_stg_Tmp.id = BASE.id);

commit;

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
select 'APTTUS','AT_Apttus_QPConfig__ProposalProductAttributeValue__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_Apttus_QPConfig__ProposalProductAttributeValue__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_Apttus_QPConfig__ProposalProductAttributeValue__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_Apttus_QPConfig__ProposalProductAttributeValue__c' and  COMPLTN_STAT = 'N');
Commit;*/

SELECT DROP_PARTITIONS('swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_Apttus_QPConfig__ProposalProductAttributeValue__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_Apttus_QPConfig__ProposalProductAttributeValue__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
SELECT ANALYZE_STATISTICS('swt_rpt_base.AT_Apttus_QPConfig__ProposalProductAttributeValue__c');






