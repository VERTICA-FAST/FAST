/****
****Script Name	  : SF_Product2.sql
****Description   : Incremental data load for SF_Product2
****/

/*Setting timing on */
\timing

/* SET SESSION AUTOCOMMIT TO OFF; */

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_Product2";

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
select 'SFDC','SF_Product2',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_Product2_Hist SELECT * from swt_rpt_stg.SF_Product2;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_Product2 where id in (
select id from swt_rpt_stg.SF_Product2 group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);


delete from swt_rpt_stg.SF_Product2 where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_Product2.id=t2.id and swt_rpt_stg.SF_Product2.auto_id<t2.auto_id);

commit;


CREATE LOCAL TEMP TABLE SF_Product2_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_Product2)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.SF_Product2;

CREATE LOCAL TEMP TABLE SF_Product2_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_Product2)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_Product2_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_Product2_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;



/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_Product2_Hist
(
IsActive
,CreatedById
,LastModifiedById
,ProductCode
,CurrencyIsoCode
,Description
,Family
,Name
,Apttus_Config2__IconSize__c
,APTS_Ext_ID__c
,SWT_BillingFrequency__c
,SWT_Billing_Plan__c
,Apttus_Config2__BundleInvoiceLevel__c
,SWT_BusinessArea__c
,SWT_BusinessAreaDescription__c
,SWT_BusinessUnit__c
,SWT_BusinessUnitDescription__c
,SWT_CategoryCode__c
,SWT_Center_Code__c
,SWT_Center__c
,Apttus_Config2__ConfigurationType__c
,Apttus_Config2__DiscontinuedDate__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EffectiveStartDate__c
,Apttus_Config2__ExcludeFromSitemap__c
,Apttus_Config2__ExpirationDate__c
,SWT_Family_Code__c
,SWT_Family__c
,SWT_FulfillmentProvider__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasDefaults__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HasSearchAttributes__c
,SWT_HasUsagePricing__c
,Apttus_Config2__Icon__c
,Apttus_Config2__IconId__c
,SWT_ItemID__c
,Apttus_Config2__LaunchDate__c
,Max_Contract_Term__c
,Min_Contract_Term__c
,Apttus_Config2__Customizable__c
,SWT_OrderingInstructions__c
,SWT_Pillar__c
,SWT_PL__c
,Product_Config_Type__c
,SWT_ProductDetail__c
,SWT_ProductRatePlanChargeID__c
,SWT_ProductSubType_UI__c
,SWT_ProductSubType__c
,SWT_ProductType_UI__c
,SWT_ProductType__c
,Search_Keyword__c
,SWT_SKUDisplayNote__c
,SWT_SoldByMaximumQty__c
,SWT_SoldByMinimumQty__c
,SWT_SoldByPack__c
,SWT_SourceSubsidiary__c
,SWT_Support_Group__c
,SWT_Taxable__c
,SWT_Term__c
,SWT_TierType__c
,SWT_TrackingID__c
,SWT_UNSPSC__c
,Apttus_Config2__Uom__c
,SWT_UOM__c
,SWT_UpfrontBilling__c
,Apttus_Config2__Version__c
,SWT_ZuoraProductRatePlanID__c
,Id
,LastModifiedDate
,SWT_Is_Base_Option__c
,SWT_Approval_Required__c
,SWT_Not_For_Resale__c
,SWT_CSR_Routed__c
,SWT_Manual_Config_Check__c
,SWT_Manual_Pricing__c
,SWT_Manual_VSOE__c
,SWT_Non_Discountable_SKU__c
,SWT_VSOE_Module__c
,SWT_Datasheet_Link__c
,SWT_Support_Business_Area__c
,SWT_SA_Date__c
,SWT_GA_Date__c
,SWT_ES_Date__c
,SWT_FlexCare_Resource_Type__c
,SWT_Purchase_Type__c
,SWT_Renewable__c
,SWT_Manual_Config_Check_Region__c
,SWT_Approval_Required_Region__c
,SWT_Approval_Required_Purchase_Type__c
,SWT_Manual_Config_Check_Purchase_Type__c
,SWT_Additional_Discount_Threshold__c
,SWT_Renewal_Duration_in_Months__c
,SWT_PLC_Status__c
,SWT_FundingType__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Is_New_Install__c
,SWT_Is_Upsell__c
,SWT_Product_Tax_Code__c
,SWT_Pillar_Code__c
,SWT_Support_Level__c
,SWT_On_Hold__c
,SWT_Term_Range_PL__c
,SWT_Royalty_Bearing__c
,SWT_ProductRoyaltyCode__c
,SWT_IsFlexCare__c
,SWT_Product_Class__c
,SWT_Is_Non_Production_License__c
,SWT_ProgramTypes__c
,CreatedDate
,SystemModstamp
,ExternalDataSourceId
,ExternalId
,DisplayUrl
,QuantityUnitOfMeasure
,IsDeleted
,LastViewedDate
,LastReferencedDate
,SWT_RevenueType__c
,SWT_EL_Date__c
,SWT_Financial_Hierarchy__c
,SWT_IntercompanyProduct__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
IsActive
,CreatedById
,LastModifiedById
,ProductCode
,CurrencyIsoCode
,Description
,Family
,Name
,Apttus_Config2__IconSize__c
,APTS_Ext_ID__c
,SWT_BillingFrequency__c
,SWT_Billing_Plan__c
,Apttus_Config2__BundleInvoiceLevel__c
,SWT_BusinessArea__c
,SWT_BusinessAreaDescription__c
,SWT_BusinessUnit__c
,SWT_BusinessUnitDescription__c
,SWT_CategoryCode__c
,SWT_Center_Code__c
,SWT_Center__c
,Apttus_Config2__ConfigurationType__c
,Apttus_Config2__DiscontinuedDate__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EffectiveStartDate__c
,Apttus_Config2__ExcludeFromSitemap__c
,Apttus_Config2__ExpirationDate__c
,SWT_Family_Code__c
,SWT_Family__c
,SWT_FulfillmentProvider__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasDefaults__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HasSearchAttributes__c
,SWT_HasUsagePricing__c
,Apttus_Config2__Icon__c
,Apttus_Config2__IconId__c
,SWT_ItemID__c
,Apttus_Config2__LaunchDate__c
,Max_Contract_Term__c
,Min_Contract_Term__c
,Apttus_Config2__Customizable__c
,SWT_OrderingInstructions__c
,SWT_Pillar__c
,SWT_PL__c
,Product_Config_Type__c
,SWT_ProductDetail__c
,SWT_ProductRatePlanChargeID__c
,SWT_ProductSubType_UI__c
,SWT_ProductSubType__c
,SWT_ProductType_UI__c
,SWT_ProductType__c
,Search_Keyword__c
,SWT_SKUDisplayNote__c
,SWT_SoldByMaximumQty__c
,SWT_SoldByMinimumQty__c
,SWT_SoldByPack__c
,SWT_SourceSubsidiary__c
,SWT_Support_Group__c
,SWT_Taxable__c
,SWT_Term__c
,SWT_TierType__c
,SWT_TrackingID__c
,SWT_UNSPSC__c
,Apttus_Config2__Uom__c
,SWT_UOM__c
,SWT_UpfrontBilling__c
,Apttus_Config2__Version__c
,SWT_ZuoraProductRatePlanID__c
,Id
,LastModifiedDate
,SWT_Is_Base_Option__c
,SWT_Approval_Required__c
,SWT_Not_For_Resale__c
,SWT_CSR_Routed__c
,SWT_Manual_Config_Check__c
,SWT_Manual_Pricing__c
,SWT_Manual_VSOE__c
,SWT_Non_Discountable_SKU__c
,SWT_VSOE_Module__c
,SWT_Datasheet_Link__c
,SWT_Support_Business_Area__c
,SWT_SA_Date__c
,SWT_GA_Date__c
,SWT_ES_Date__c
,SWT_FlexCare_Resource_Type__c
,SWT_Purchase_Type__c
,SWT_Renewable__c
,SWT_Manual_Config_Check_Region__c
,SWT_Approval_Required_Region__c
,SWT_Approval_Required_Purchase_Type__c
,SWT_Manual_Config_Check_Purchase_Type__c
,SWT_Additional_Discount_Threshold__c
,SWT_Renewal_Duration_in_Months__c
,SWT_PLC_Status__c
,SWT_FundingType__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Is_New_Install__c
,SWT_Is_Upsell__c
,SWT_Product_Tax_Code__c
,SWT_Pillar_Code__c
,SWT_Support_Level__c
,SWT_On_Hold__c
,SWT_Term_Range_PL__c
,SWT_Royalty_Bearing__c
,SWT_ProductRoyaltyCode__c
,SWT_IsFlexCare__c
,SWT_Product_Class__c
,SWT_Is_Non_Production_License__c
,SWT_ProgramTypes__c
,CreatedDate
,SystemModstamp
,ExternalDataSourceId
,ExternalId
,DisplayUrl
,QuantityUnitOfMeasure
,IsDeleted
,LastViewedDate
,LastReferencedDate
,SWT_RevenueType__c
,SWT_EL_Date__c
,SWT_Financial_Hierarchy__c
,SWT_IntercompanyProduct__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_Product2 WHERE id in
(SELECT STG.id FROM SF_Product2_stg_Tmp_Key STG JOIN SF_Product2_base_Tmp
ON STG.id = SF_Product2_base_Tmp.id AND STG.LastModifiedDate >= SF_Product2_base_Tmp.LastModifiedDate);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."SF_Product2_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_Product2 WHERE id in
(SELECT STG.id FROM SF_Product2_stg_Tmp_Key STG JOIN SF_Product2_base_Tmp
ON STG.id = SF_Product2_base_Tmp.id AND STG.LastModifiedDate >= SF_Product2_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_Product2
(
IsActive
,CreatedById
,LastModifiedById
,ProductCode
,CurrencyIsoCode
,Description
,Family
,Name
,Apttus_Config2__IconSize__c
,APTS_Ext_ID__c
,SWT_BillingFrequency__c
,SWT_Billing_Plan__c
,Apttus_Config2__BundleInvoiceLevel__c
,SWT_BusinessArea__c
,SWT_BusinessAreaDescription__c
,SWT_BusinessUnit__c
,SWT_BusinessUnitDescription__c
,SWT_CategoryCode__c
,SWT_Center_Code__c
,SWT_Center__c
,Apttus_Config2__ConfigurationType__c
,Apttus_Config2__DiscontinuedDate__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EffectiveStartDate__c
,Apttus_Config2__ExcludeFromSitemap__c
,Apttus_Config2__ExpirationDate__c
,SWT_Family_Code__c
,SWT_Family__c
,SWT_FulfillmentProvider__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasDefaults__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HasSearchAttributes__c
,SWT_HasUsagePricing__c
,Apttus_Config2__Icon__c
,Apttus_Config2__IconId__c
,SWT_ItemID__c
,Apttus_Config2__LaunchDate__c
,Max_Contract_Term__c
,Min_Contract_Term__c
,Apttus_Config2__Customizable__c
,SWT_OrderingInstructions__c
,SWT_Pillar__c
,SWT_PL__c
,Product_Config_Type__c
,SWT_ProductDetail__c
,SWT_ProductRatePlanChargeID__c
,SWT_ProductSubType_UI__c
,SWT_ProductSubType__c
,SWT_ProductType_UI__c
,SWT_ProductType__c
,Search_Keyword__c
,SWT_SKUDisplayNote__c
,SWT_SoldByMaximumQty__c
,SWT_SoldByMinimumQty__c
,SWT_SoldByPack__c
,SWT_SourceSubsidiary__c
,SWT_Support_Group__c
,SWT_Taxable__c
,SWT_Term__c
,SWT_TierType__c
,SWT_TrackingID__c
,SWT_UNSPSC__c
,Apttus_Config2__Uom__c
,SWT_UOM__c
,SWT_UpfrontBilling__c
,Apttus_Config2__Version__c
,SWT_ZuoraProductRatePlanID__c
,Id
,LastModifiedDate
,SWT_Is_Base_Option__c
,SWT_Approval_Required__c
,SWT_Not_For_Resale__c
,SWT_CSR_Routed__c
,SWT_Manual_Config_Check__c
,SWT_Manual_Pricing__c
,SWT_Manual_VSOE__c
,SWT_Non_Discountable_SKU__c
,SWT_VSOE_Module__c
,SWT_Datasheet_Link__c
,SWT_Support_Business_Area__c
,SWT_SA_Date__c
,SWT_GA_Date__c
,SWT_ES_Date__c
,SWT_FlexCare_Resource_Type__c
,SWT_Purchase_Type__c
,SWT_Renewable__c
,SWT_Manual_Config_Check_Region__c
,SWT_Approval_Required_Region__c
,SWT_Approval_Required_Purchase_Type__c
,SWT_Manual_Config_Check_Purchase_Type__c
,SWT_Additional_Discount_Threshold__c
,SWT_Renewal_Duration_in_Months__c
,SWT_PLC_Status__c
,SWT_FundingType__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Is_New_Install__c
,SWT_Is_Upsell__c
,SWT_Product_Tax_Code__c
,SWT_Pillar_Code__c
,SWT_Support_Level__c
,SWT_On_Hold__c
,SWT_Term_Range_PL__c
,SWT_Royalty_Bearing__c
,SWT_ProductRoyaltyCode__c
,SWT_IsFlexCare__c
,SWT_Product_Class__c
,SWT_Is_Non_Production_License__c
,SWT_ProgramTypes__c
,CreatedDate
,SystemModstamp
,ExternalDataSourceId
,ExternalId
,DisplayUrl
,QuantityUnitOfMeasure
,IsDeleted
,LastViewedDate
,LastReferencedDate
,SWT_RevenueType__c
,SWT_EL_Date__c
,SWT_Financial_Hierarchy__c
,SWT_IntercompanyProduct__c
,SWT_INS_DT
)
SELECT DISTINCT 
IsActive
,CreatedById
,LastModifiedById
,ProductCode
,CurrencyIsoCode
,Description
,Family
,Name
,Apttus_Config2__IconSize__c
,APTS_Ext_ID__c
,SWT_BillingFrequency__c
,SWT_Billing_Plan__c
,Apttus_Config2__BundleInvoiceLevel__c
,SWT_BusinessArea__c
,SWT_BusinessAreaDescription__c
,SWT_BusinessUnit__c
,SWT_BusinessUnitDescription__c
,SWT_CategoryCode__c
,SWT_Center_Code__c
,SWT_Center__c
,Apttus_Config2__ConfigurationType__c
,Apttus_Config2__DiscontinuedDate__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EffectiveStartDate__c
,Apttus_Config2__ExcludeFromSitemap__c
,Apttus_Config2__ExpirationDate__c
,SWT_Family_Code__c
,SWT_Family__c
,SWT_FulfillmentProvider__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasDefaults__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HasSearchAttributes__c
,SWT_HasUsagePricing__c
,Apttus_Config2__Icon__c
,Apttus_Config2__IconId__c
,SWT_ItemID__c
,Apttus_Config2__LaunchDate__c
,Max_Contract_Term__c
,Min_Contract_Term__c
,Apttus_Config2__Customizable__c
,SWT_OrderingInstructions__c
,SWT_Pillar__c
,SWT_PL__c
,Product_Config_Type__c
,SWT_ProductDetail__c
,SWT_ProductRatePlanChargeID__c
,SWT_ProductSubType_UI__c
,SWT_ProductSubType__c
,SWT_ProductType_UI__c
,SWT_ProductType__c
,Search_Keyword__c
,SWT_SKUDisplayNote__c
,SWT_SoldByMaximumQty__c
,SWT_SoldByMinimumQty__c
,SWT_SoldByPack__c
,SWT_SourceSubsidiary__c
,SWT_Support_Group__c
,SWT_Taxable__c
,SWT_Term__c
,SWT_TierType__c
,SWT_TrackingID__c
,SWT_UNSPSC__c
,Apttus_Config2__Uom__c
,SWT_UOM__c
,SWT_UpfrontBilling__c
,Apttus_Config2__Version__c
,SWT_ZuoraProductRatePlanID__c
,SF_Product2_stg_Tmp.Id
,SF_Product2_stg_Tmp.LastModifiedDate
,SWT_Is_Base_Option__c
,SWT_Approval_Required__c
,SWT_Not_For_Resale__c
,SWT_CSR_Routed__c
,SWT_Manual_Config_Check__c
,SWT_Manual_Pricing__c
,SWT_Manual_VSOE__c
,SWT_Non_Discountable_SKU__c
,SWT_VSOE_Module__c
,SWT_Datasheet_Link__c
,SWT_Support_Business_Area__c
,SWT_SA_Date__c
,SWT_GA_Date__c
,SWT_ES_Date__c
,SWT_FlexCare_Resource_Type__c
,SWT_Purchase_Type__c
,SWT_Renewable__c
,SWT_Manual_Config_Check_Region__c
,SWT_Approval_Required_Region__c
,SWT_Approval_Required_Purchase_Type__c
,SWT_Manual_Config_Check_Purchase_Type__c
,SWT_Additional_Discount_Threshold__c
,SWT_Renewal_Duration_in_Months__c
,SWT_PLC_Status__c
,SWT_FundingType__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Is_New_Install__c
,SWT_Is_Upsell__c
,SWT_Product_Tax_Code__c
,SWT_Pillar_Code__c
,SWT_Support_Level__c
,SWT_On_Hold__c
,SWT_Term_Range_PL__c
,SWT_Royalty_Bearing__c
,SWT_ProductRoyaltyCode__c
,SWT_IsFlexCare__c
,SWT_Product_Class__c
,SWT_Is_Non_Production_License__c
,SWT_ProgramTypes__c
,CreatedDate
,SystemModstamp
,ExternalDataSourceId
,ExternalId
,DisplayUrl
,QuantityUnitOfMeasure
,IsDeleted
,LastViewedDate
,LastReferencedDate
,SWT_RevenueType__c
,SWT_EL_Date__c
,SWT_Financial_Hierarchy__c
,SWT_IntercompanyProduct__c
,SYSDATE
FROM SF_Product2_stg_Tmp JOIN SF_Product2_stg_Tmp_Key ON SF_Product2_stg_Tmp.id= SF_Product2_stg_Tmp_Key.id AND SF_Product2_stg_Tmp.LastModifiedDate=SF_Product2_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_Product2 BASE
WHERE SF_Product2_stg_Tmp.id = BASE.id);

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
select 'SFDC','SF_Product2',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_Product2 where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_Product2' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_Product2' and  COMPLTN_STAT = 'N');
commit;
*/

select do_tm_task('mergeout','swt_rpt_stg.SF_Product2_Hist');
select do_tm_task('mergeout','swt_rpt_base.SF_Product2');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_Product2');



