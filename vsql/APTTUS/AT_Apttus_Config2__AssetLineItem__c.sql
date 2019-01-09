/****
****Script Name	  : AT_Apttus_Config2__AssetLineItem__c.sql
****Description   : Incremental data load for AT_Apttus_Config2__AssetLineItem__c
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_Apttus_Config2__AssetLineItem__c";

/* Inserting values into the Audit table  */

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
select 'APTTUS','AT_Apttus_Config2__AssetLineItem__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;  

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_Apttus_Config2__AssetLineItem__c_Hist select * from "swt_rpt_stg".AT_Apttus_Config2__AssetLineItem__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c where id in (
select id from swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c.id=t2.id and swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c.auto_id<t2.auto_id);

commit;


CREATE LOCAL TEMP TABLE AT_Apttus_Config2__AssetLineItem__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT * FROM swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c;
  
CREATE LOCAL TEMP TABLE AT_Apttus_Config2__AssetLineItem__c_base_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.AT_Apttus_Config2__AssetLineItem__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM AT_Apttus_Config2__AssetLineItem__c_stg_Tmp group by id) 
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

/* Inserting deleted data into the Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c_Hist
(
Id
,OwnerId
,Apttus_CMConfig__AgreementLineItemId__c
,Apttus_Config2__AllowedActions__c
,Apttus_Config2__AttributeValueId__c
,Apttus_Config2__BillingPlanId__c
,Apttus_Config2__BundleAssetId__c
,Apttus_Config2__ChargeGroupId__c
,Apttus_Config2__IsRenewed__c
,Apttus_Config2__OptionId__c
,Apttus_Config2__ParentAssetId__c
,Apttus_Config2__PriceListItemId__c
,Apttus_Config2__ProductId__c
,Apttus_QPConfig__ProposalLineItemId__c
,Apttus_Config2__TaxCodeId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Apttus_Config2__AdjustedPrice__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AssetARR__c
,Apttus_Config2__AssetCode__c
,Apttus_Config2__AssetMRR__c
,Apttus_Config2__AssetNumber__c
,Apttus_Config2__AssetStatus__c
,Apttus_Config2__AssetTCV__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__BaseCost__c
,Apttus_Config2__BaseExtendedCost__c
,Apttus_Config2__BaseExtendedPrice__c
,Apttus_Config2__BasePrice__c
,Apttus_Config2__BasePriceMethod__c
,Apttus_Config2__BillingEndDate__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingPreferenceId__c
,Apttus_Config2__BillingRule__c
,Apttus_Config2__BillingStartDate__c
,Apttus_Config2__BillThroughDate__c
,Apttus_Config2__BillToAccountId__c
,Apttus_Config2__BusinessLineItemId__c
,Apttus_Config2__BusinessObjectId__c
,Apttus_Config2__BusinessObjectType__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Comments__c
,SWT_Ship_To_Address__c
,Apttus_Config2__DeltaPrice__c
,Apttus_Config2__DeltaQuantity__c
,Apttus_Config2__Description__c
,SWT_EndCustomer__c
,Apttus_Config2__EndDate__c
,Apttus_Config2__ExtendedCost__c
,Apttus_Config2__ExtendedDescription__c
,Apttus_Config2__ExtendedPrice__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HideInvoiceDisplay__c
,Apttus_Config2__IsInactive__c
,Apttus_Config2__InitialActivationDate__c
,Apttus_Config2__IsOptionRollupLine__c
,Apttus_Config2__IsPrimaryLine__c
,Apttus_Config2__IsPrimaryRampLine__c
,Apttus_Config2__IsReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ItemSequence__c
,SWT_Jurisdiction_Text__c
,Apttus_Config2__LastRenewEndDate__c
,SWT_Legacy_Asset_Id__c
,Apttus_Config2__LineNumber__c
,Apttus_Config2__LineType__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__LocationId__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinUsageQuantity__c
,Apttus_Config2__MustUpgrade__c
,Apttus_Config2__NetPrice__c
,Apttus_Config2__NetUnitPrice__c
,Apttus_Config2__OptionCost__c
,Apttus_Config2__OptionPrice__c
,Apttus_Config2__OriginalStartDate__c
,Apttus_Config2__ParentBundleNumber__c
,Apttus_Config2__PaymentTermId__c
,Apttus_Config2__PriceGroup__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__Term__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__PrimaryLineNumber__c
,SWT_Product_Config_Type__c
,SWT_Product_Type__c
,Apttus_Config2__PurchaseDate__c
,Apttus_Config2__Quantity__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__RenewalAdjustmentAmount__c
,Apttus_Config2__RenewalAdjustmentType__c
,Apttus_Config2__RenewalDate__c
,Apttus_Config2__RenewalFrequency__c
,Apttus_Config2__RenewalTerm__c
,SWT_Subscription_Name__c
,Apttus_Config2__SellingFrequency__c
,Apttus_Config2__SellingTerm__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__AccountId__c
,Apttus_Config2__StartDate__c
,SWT_Subscription_End_Date__c
,SWT_Subscription_ID__c
,SWT_Subscription_Start_Date__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,SWT_Tier_Type_Formula__c
,SWT_Rate_Plan_Charge_ID__c
,SWT_Rate_Plan_ID__c
,LastModifiedDate
,SWT_Entitlement_Id__c
,SWT_Customer_Account_Reference__c
,SWT_Support_Business_Area__c
,SWT_Percentage_of_License_Net__c
,SWT_Cancellation_Comment__c
,SWT_Cancellation_Reason__c
,SWT_Cancellation_Type__c
,SWT_Product_Code__c
,SWT_Appliance_Serial_Number__c
,Internal_Id__c
,SWT_Initial_Quantity_New_Business__c
,SWT_Cancelled_Quantity__c
,SWT_Full_Cancelled_Asset__c
,SWT_Renewal_End_Date__c
,SWT_Bill_To_Address__c
,SWT_Sold_To_Address__c
,SWT_Quote_Sub_Type__c
,SWT_Quote_Type__c
,SWT_Market_Route__c
,SWT_Tier_2_Relationship__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_1_Partner__c
,SWT_Ship_To_Contact__c
,SWT_Sold_to_Contact__c
,SWT_Bill_To_Contact__c
,SWT_Cancellation_Note__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Product_Tax_Code__c
,SWT_Sold_By_Pack__c
,SWT_Business_Area__c
,SWT_Resource__c
,SWT_DropShip_PO__c
,SWT_ELA_Agreement_Number__c
,SWT_Previous_Year_Support_Net_Price__c
,SWT_Previous_Year_License_Net_Price__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWT_End_Customer__c
,SWT_Legacy_SAID__c
,Apttus_Config2__IsRenewalPending__c
,Apttus_Config2__NextRenewEndDate__c
,SWT_Asset_Number__c
,SWT_Legacy_Order_Id__c
,SWT_Control_Tower_ID__c
,SWT_Opportunity_Classification__c
,SWT_SVI__c
,SWT_Is_Product_Renewable__c
,SWT_Charge_Description__c
,LD_DT
,SWT_INS_DT
,d_source
)
select 
Id
,OwnerId
,Apttus_CMConfig__AgreementLineItemId__c
,Apttus_Config2__AllowedActions__c
,Apttus_Config2__AttributeValueId__c
,Apttus_Config2__BillingPlanId__c
,Apttus_Config2__BundleAssetId__c
,Apttus_Config2__ChargeGroupId__c
,Apttus_Config2__IsRenewed__c
,Apttus_Config2__OptionId__c
,Apttus_Config2__ParentAssetId__c
,Apttus_Config2__PriceListItemId__c
,Apttus_Config2__ProductId__c
,Apttus_QPConfig__ProposalLineItemId__c
,Apttus_Config2__TaxCodeId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Apttus_Config2__AdjustedPrice__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AssetARR__c
,Apttus_Config2__AssetCode__c
,Apttus_Config2__AssetMRR__c
,Apttus_Config2__AssetNumber__c
,Apttus_Config2__AssetStatus__c
,Apttus_Config2__AssetTCV__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__BaseCost__c
,Apttus_Config2__BaseExtendedCost__c
,Apttus_Config2__BaseExtendedPrice__c
,Apttus_Config2__BasePrice__c
,Apttus_Config2__BasePriceMethod__c
,Apttus_Config2__BillingEndDate__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingPreferenceId__c
,Apttus_Config2__BillingRule__c
,Apttus_Config2__BillingStartDate__c
,Apttus_Config2__BillThroughDate__c
,Apttus_Config2__BillToAccountId__c
,Apttus_Config2__BusinessLineItemId__c
,Apttus_Config2__BusinessObjectId__c
,Apttus_Config2__BusinessObjectType__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Comments__c
,SWT_Ship_To_Address__c
,Apttus_Config2__DeltaPrice__c
,Apttus_Config2__DeltaQuantity__c
,Apttus_Config2__Description__c
,SWT_EndCustomer__c
,Apttus_Config2__EndDate__c
,Apttus_Config2__ExtendedCost__c
,Apttus_Config2__ExtendedDescription__c
,Apttus_Config2__ExtendedPrice__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HideInvoiceDisplay__c
,Apttus_Config2__IsInactive__c
,Apttus_Config2__InitialActivationDate__c
,Apttus_Config2__IsOptionRollupLine__c
,Apttus_Config2__IsPrimaryLine__c
,Apttus_Config2__IsPrimaryRampLine__c
,Apttus_Config2__IsReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ItemSequence__c
,SWT_Jurisdiction_Text__c
,Apttus_Config2__LastRenewEndDate__c
,SWT_Legacy_Asset_Id__c
,Apttus_Config2__LineNumber__c
,Apttus_Config2__LineType__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__LocationId__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinUsageQuantity__c
,Apttus_Config2__MustUpgrade__c
,Apttus_Config2__NetPrice__c
,Apttus_Config2__NetUnitPrice__c
,Apttus_Config2__OptionCost__c
,Apttus_Config2__OptionPrice__c
,Apttus_Config2__OriginalStartDate__c
,Apttus_Config2__ParentBundleNumber__c
,Apttus_Config2__PaymentTermId__c
,Apttus_Config2__PriceGroup__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__Term__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__PrimaryLineNumber__c
,SWT_Product_Config_Type__c
,SWT_Product_Type__c
,Apttus_Config2__PurchaseDate__c
,Apttus_Config2__Quantity__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__RenewalAdjustmentAmount__c
,Apttus_Config2__RenewalAdjustmentType__c
,Apttus_Config2__RenewalDate__c
,Apttus_Config2__RenewalFrequency__c
,Apttus_Config2__RenewalTerm__c
,SWT_Subscription_Name__c
,Apttus_Config2__SellingFrequency__c
,Apttus_Config2__SellingTerm__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__AccountId__c
,Apttus_Config2__StartDate__c
,SWT_Subscription_End_Date__c
,SWT_Subscription_ID__c
,SWT_Subscription_Start_Date__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,SWT_Tier_Type_Formula__c
,SWT_Rate_Plan_Charge_ID__c
,SWT_Rate_Plan_ID__c
,LastModifiedDate
,SWT_Entitlement_Id__c
,SWT_Customer_Account_Reference__c
,SWT_Support_Business_Area__c
,SWT_Percentage_of_License_Net__c
,SWT_Cancellation_Comment__c
,SWT_Cancellation_Reason__c
,SWT_Cancellation_Type__c
,SWT_Product_Code__c
,SWT_Appliance_Serial_Number__c
,Internal_Id__c
,SWT_Initial_Quantity_New_Business__c
,SWT_Cancelled_Quantity__c
,SWT_Full_Cancelled_Asset__c
,SWT_Renewal_End_Date__c
,SWT_Bill_To_Address__c
,SWT_Sold_To_Address__c
,SWT_Quote_Sub_Type__c
,SWT_Quote_Type__c
,SWT_Market_Route__c
,SWT_Tier_2_Relationship__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_1_Partner__c
,SWT_Ship_To_Contact__c
,SWT_Sold_to_Contact__c
,SWT_Bill_To_Contact__c
,SWT_Cancellation_Note__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Product_Tax_Code__c
,SWT_Sold_By_Pack__c
,SWT_Business_Area__c
,SWT_Resource__c
,SWT_DropShip_PO__c
,SWT_ELA_Agreement_Number__c
,SWT_Previous_Year_Support_Net_Price__c
,SWT_Previous_Year_License_Net_Price__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWT_End_Customer__c
,SWT_Legacy_SAID__c
,Apttus_Config2__IsRenewalPending__c
,Apttus_Config2__NextRenewEndDate__c
,SWT_Asset_Number__c
,SWT_Legacy_Order_Id__c
,SWT_Control_Tower_ID__c
,SWT_Opportunity_Classification__c
,SWT_SVI__c
,SWT_Is_Product_Renewable__c
,SWT_Charge_Description__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_Apttus_Config2__AssetLineItem__c WHERE id in
(SELECT STG.id FROM AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key STG JOIN AT_Apttus_Config2__AssetLineItem__c_base_Tmp
ON STG.id = AT_Apttus_Config2__AssetLineItem__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_Config2__AssetLineItem__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_Apttus_Config2__AssetLineItem__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_Apttus_Config2__AssetLineItem__c WHERE id in
(SELECT STG.id FROM AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key STG JOIN AT_Apttus_Config2__AssetLineItem__c_base_Tmp
ON STG.id = AT_Apttus_Config2__AssetLineItem__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_Config2__AssetLineItem__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_Apttus_Config2__AssetLineItem__c
(
Id
,OwnerId
,Apttus_CMConfig__AgreementLineItemId__c
,Apttus_Config2__AllowedActions__c
,Apttus_Config2__AttributeValueId__c
,Apttus_Config2__BillingPlanId__c
,Apttus_Config2__BundleAssetId__c
,Apttus_Config2__ChargeGroupId__c
,Apttus_Config2__IsRenewed__c
,Apttus_Config2__OptionId__c
,Apttus_Config2__ParentAssetId__c
,Apttus_Config2__PriceListItemId__c
,Apttus_Config2__ProductId__c
,Apttus_QPConfig__ProposalLineItemId__c
,Apttus_Config2__TaxCodeId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Apttus_Config2__AdjustedPrice__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AssetARR__c
,Apttus_Config2__AssetCode__c
,Apttus_Config2__AssetMRR__c
,Apttus_Config2__AssetNumber__c
,Apttus_Config2__AssetStatus__c
,Apttus_Config2__AssetTCV__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__BaseCost__c
,Apttus_Config2__BaseExtendedCost__c
,Apttus_Config2__BaseExtendedPrice__c
,Apttus_Config2__BasePrice__c
,Apttus_Config2__BasePriceMethod__c
,Apttus_Config2__BillingEndDate__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingPreferenceId__c
,Apttus_Config2__BillingRule__c
,Apttus_Config2__BillingStartDate__c
,Apttus_Config2__BillThroughDate__c
,Apttus_Config2__BillToAccountId__c
,Apttus_Config2__BusinessLineItemId__c
,Apttus_Config2__BusinessObjectId__c
,Apttus_Config2__BusinessObjectType__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Comments__c
,SWT_Ship_To_Address__c
,Apttus_Config2__DeltaPrice__c
,Apttus_Config2__DeltaQuantity__c
,Apttus_Config2__Description__c
,SWT_EndCustomer__c
,Apttus_Config2__EndDate__c
,Apttus_Config2__ExtendedCost__c
,Apttus_Config2__ExtendedDescription__c
,Apttus_Config2__ExtendedPrice__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HideInvoiceDisplay__c
,Apttus_Config2__IsInactive__c
,Apttus_Config2__InitialActivationDate__c
,Apttus_Config2__IsOptionRollupLine__c
,Apttus_Config2__IsPrimaryLine__c
,Apttus_Config2__IsPrimaryRampLine__c
,Apttus_Config2__IsReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ItemSequence__c
,SWT_Jurisdiction_Text__c
,Apttus_Config2__LastRenewEndDate__c
,SWT_Legacy_Asset_Id__c
,Apttus_Config2__LineNumber__c
,Apttus_Config2__LineType__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__LocationId__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinUsageQuantity__c
,Apttus_Config2__MustUpgrade__c
,Apttus_Config2__NetPrice__c
,Apttus_Config2__NetUnitPrice__c
,Apttus_Config2__OptionCost__c
,Apttus_Config2__OptionPrice__c
,Apttus_Config2__OriginalStartDate__c
,Apttus_Config2__ParentBundleNumber__c
,Apttus_Config2__PaymentTermId__c
,Apttus_Config2__PriceGroup__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__Term__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__PrimaryLineNumber__c
,SWT_Product_Config_Type__c
,SWT_Product_Type__c
,Apttus_Config2__PurchaseDate__c
,Apttus_Config2__Quantity__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__RenewalAdjustmentAmount__c
,Apttus_Config2__RenewalAdjustmentType__c
,Apttus_Config2__RenewalDate__c
,Apttus_Config2__RenewalFrequency__c
,Apttus_Config2__RenewalTerm__c
,SWT_Subscription_Name__c
,Apttus_Config2__SellingFrequency__c
,Apttus_Config2__SellingTerm__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__AccountId__c
,Apttus_Config2__StartDate__c
,SWT_Subscription_End_Date__c
,SWT_Subscription_ID__c
,SWT_Subscription_Start_Date__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,SWT_Tier_Type_Formula__c
,SWT_Rate_Plan_Charge_ID__c
,SWT_Rate_Plan_ID__c
,LastModifiedDate
,SWT_Entitlement_Id__c
,SWT_Customer_Account_Reference__c
,SWT_Support_Business_Area__c
,SWT_Percentage_of_License_Net__c
,SWT_Cancellation_Comment__c
,SWT_Cancellation_Reason__c
,SWT_Cancellation_Type__c
,SWT_Product_Code__c
,SWT_Appliance_Serial_Number__c
,Internal_Id__c
,SWT_Initial_Quantity_New_Business__c
,SWT_Cancelled_Quantity__c
,SWT_Full_Cancelled_Asset__c
,SWT_Renewal_End_Date__c
,SWT_Bill_To_Address__c
,SWT_Sold_To_Address__c
,SWT_Quote_Sub_Type__c
,SWT_Quote_Type__c
,SWT_Market_Route__c
,SWT_Tier_2_Relationship__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_1_Partner__c
,SWT_Ship_To_Contact__c
,SWT_Sold_to_Contact__c
,SWT_Bill_To_Contact__c
,SWT_Cancellation_Note__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Product_Tax_Code__c
,SWT_Sold_By_Pack__c
,SWT_Business_Area__c
,SWT_Resource__c
,SWT_DropShip_PO__c
,SWT_ELA_Agreement_Number__c
,SWT_Previous_Year_Support_Net_Price__c
,SWT_Previous_Year_License_Net_Price__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWT_End_Customer__c
,SWT_Legacy_SAID__c
,Apttus_Config2__IsRenewalPending__c
,Apttus_Config2__NextRenewEndDate__c
,SWT_Asset_Number__c
,SWT_Legacy_Order_Id__c
,SWT_Control_Tower_ID__c
,SWT_Opportunity_Classification__c
,SWT_SVI__c
,SWT_Is_Product_Renewable__c
,SWT_Charge_Description__c
,SWT_INS_DT
)
SELECT DISTINCT 
AT_Apttus_Config2__AssetLineItem__c_stg_Tmp.Id
,OwnerId
,Apttus_CMConfig__AgreementLineItemId__c
,Apttus_Config2__AllowedActions__c
,Apttus_Config2__AttributeValueId__c
,Apttus_Config2__BillingPlanId__c
,Apttus_Config2__BundleAssetId__c
,Apttus_Config2__ChargeGroupId__c
,Apttus_Config2__IsRenewed__c
,Apttus_Config2__OptionId__c
,Apttus_Config2__ParentAssetId__c
,Apttus_Config2__PriceListItemId__c
,Apttus_Config2__ProductId__c
,Apttus_QPConfig__ProposalLineItemId__c
,Apttus_Config2__TaxCodeId__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Apttus_Config2__AdjustedPrice__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AssetARR__c
,Apttus_Config2__AssetCode__c
,Apttus_Config2__AssetMRR__c
,Apttus_Config2__AssetNumber__c
,Apttus_Config2__AssetStatus__c
,Apttus_Config2__AssetTCV__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__BaseCost__c
,Apttus_Config2__BaseExtendedCost__c
,Apttus_Config2__BaseExtendedPrice__c
,Apttus_Config2__BasePrice__c
,Apttus_Config2__BasePriceMethod__c
,Apttus_Config2__BillingEndDate__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingPreferenceId__c
,Apttus_Config2__BillingRule__c
,Apttus_Config2__BillingStartDate__c
,Apttus_Config2__BillThroughDate__c
,Apttus_Config2__BillToAccountId__c
,Apttus_Config2__BusinessLineItemId__c
,Apttus_Config2__BusinessObjectId__c
,Apttus_Config2__BusinessObjectType__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Comments__c
,SWT_Ship_To_Address__c
,Apttus_Config2__DeltaPrice__c
,Apttus_Config2__DeltaQuantity__c
,Apttus_Config2__Description__c
,SWT_EndCustomer__c
,Apttus_Config2__EndDate__c
,Apttus_Config2__ExtendedCost__c
,Apttus_Config2__ExtendedDescription__c
,Apttus_Config2__ExtendedPrice__c
,Apttus_Config2__HasAttributes__c
,Apttus_Config2__HasOptions__c
,Apttus_Config2__HideInvoiceDisplay__c
,Apttus_Config2__IsInactive__c
,Apttus_Config2__InitialActivationDate__c
,Apttus_Config2__IsOptionRollupLine__c
,Apttus_Config2__IsPrimaryLine__c
,Apttus_Config2__IsPrimaryRampLine__c
,Apttus_Config2__IsReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ItemSequence__c
,SWT_Jurisdiction_Text__c
,Apttus_Config2__LastRenewEndDate__c
,SWT_Legacy_Asset_Id__c
,Apttus_Config2__LineNumber__c
,Apttus_Config2__LineType__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__LocationId__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinUsageQuantity__c
,Apttus_Config2__MustUpgrade__c
,Apttus_Config2__NetPrice__c
,Apttus_Config2__NetUnitPrice__c
,Apttus_Config2__OptionCost__c
,Apttus_Config2__OptionPrice__c
,Apttus_Config2__OriginalStartDate__c
,Apttus_Config2__ParentBundleNumber__c
,Apttus_Config2__PaymentTermId__c
,Apttus_Config2__PriceGroup__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__Term__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__PrimaryLineNumber__c
,SWT_Product_Config_Type__c
,SWT_Product_Type__c
,Apttus_Config2__PurchaseDate__c
,Apttus_Config2__Quantity__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__RenewalAdjustmentAmount__c
,Apttus_Config2__RenewalAdjustmentType__c
,Apttus_Config2__RenewalDate__c
,Apttus_Config2__RenewalFrequency__c
,Apttus_Config2__RenewalTerm__c
,SWT_Subscription_Name__c
,Apttus_Config2__SellingFrequency__c
,Apttus_Config2__SellingTerm__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__AccountId__c
,Apttus_Config2__StartDate__c
,SWT_Subscription_End_Date__c
,SWT_Subscription_ID__c
,SWT_Subscription_Start_Date__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,SWT_Tier_Type_Formula__c
,SWT_Rate_Plan_Charge_ID__c
,SWT_Rate_Plan_ID__c
,AT_Apttus_Config2__AssetLineItem__c_stg_Tmp.LastModifiedDate
,SWT_Entitlement_Id__c
,SWT_Customer_Account_Reference__c
,SWT_Support_Business_Area__c
,SWT_Percentage_of_License_Net__c
,SWT_Cancellation_Comment__c
,SWT_Cancellation_Reason__c
,SWT_Cancellation_Type__c
,SWT_Product_Code__c
,SWT_Appliance_Serial_Number__c
,Internal_Id__c
,SWT_Initial_Quantity_New_Business__c
,SWT_Cancelled_Quantity__c
,SWT_Full_Cancelled_Asset__c
,SWT_Renewal_End_Date__c
,SWT_Bill_To_Address__c
,SWT_Sold_To_Address__c
,SWT_Quote_Sub_Type__c
,SWT_Quote_Type__c
,SWT_Market_Route__c
,SWT_Tier_2_Relationship__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_1_Partner__c
,SWT_Ship_To_Contact__c
,SWT_Sold_to_Contact__c
,SWT_Bill_To_Contact__c
,SWT_Cancellation_Note__c
,SWT_Product_Detail_Code__c
,SWT_UOM_code__c
,SWT_Product_Tax_Code__c
,SWT_Sold_By_Pack__c
,SWT_Business_Area__c
,SWT_Resource__c
,SWT_DropShip_PO__c
,SWT_ELA_Agreement_Number__c
,SWT_Previous_Year_Support_Net_Price__c
,SWT_Previous_Year_License_Net_Price__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWT_End_Customer__c
,SWT_Legacy_SAID__c
,Apttus_Config2__IsRenewalPending__c
,Apttus_Config2__NextRenewEndDate__c
,SWT_Asset_Number__c
,SWT_Legacy_Order_Id__c
,SWT_Control_Tower_ID__c
,SWT_Opportunity_Classification__c
,SWT_SVI__c
,SWT_Is_Product_Renewable__c
,SWT_Charge_Description__c
,SYSDATE
FROM AT_Apttus_Config2__AssetLineItem__c_stg_Tmp JOIN AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key ON AT_Apttus_Config2__AssetLineItem__c_stg_Tmp.id= AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key.id AND AT_Apttus_Config2__AssetLineItem__c_stg_Tmp.LastModifiedDate=AT_Apttus_Config2__AssetLineItem__c_stg_Tmp_Key.LastModifiedDate
	WHERE NOT EXISTS
	(SELECT 1 FROM "swt_rpt_base".AT_Apttus_Config2__AssetLineItem__c BASE
		WHERE AT_Apttus_Config2__AssetLineItem__c_stg_Tmp.id = BASE.id);

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
select 'APTTUS','AT_Apttus_Config2__AssetLineItem__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_Apttus_Config2__AssetLineItem__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
		
/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_Apttus_Config2__AssetLineItem__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_Apttus_Config2__AssetLineItem__c' and  COMPLTN_STAT = 'N');
Commit;*/


SELECT DROP_PARTITIONS('swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_Apttus_Config2__AssetLineItem__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_Apttus_Config2__AssetLineItem__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.AT_Apttus_Config2__AssetLineItem__c');





