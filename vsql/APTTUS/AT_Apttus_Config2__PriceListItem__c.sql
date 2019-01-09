
----Script Name	  : AT_Apttus_Config2__PriceListItem__c.sql
----Description   : Incremental data load for AT_Apttus_Config2__PriceListItem__c
----


/* Setting timing on**/
\timing
\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_Apttus_Config2__PriceListItem__c";

-- Inserting values into Audit table  --

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
select 'APTTUS','AT_Apttus_Config2__PriceListItem__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_Apttus_Config2__PriceListItem__c_Hist select * from "swt_rpt_stg".AT_Apttus_Config2__PriceListItem__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c where id in (
select id from swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c.id=t2.id and swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE AT_PriceList_stg_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT * FROM swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c)
SEGMENTED BY HASH(id,lastmodifieddate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c;

CREATE LOCAL TEMP TABLE AT_PriceList_base_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT ID,LASTMODIFIEDDATE FROM swt_rpt_base.AT_Apttus_Config2__PriceListItem__c)
SEGMENTED BY HASH(ID,LASTMODIFIEDDATE) ALL NODES;

CREATE LOCAL TEMP TABLE AT_PriceList_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(lastmodifieddate) as lastmodifieddate FROM AT_PriceList_stg_Tmp group by id) 
SEGMENTED BY HASH(ID,LASTMODIFIEDDATE) ALL NODES;




-- Inserting delete data into Historical Table --

insert /*+DIRECT*/ into swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c_Hist
(
Id
,Apttus_Config2__ARAccountRuleId__c
,Apttus_Config2__DRAccountRuleId__c
,Apttus_Config2__NumberOfMatrices__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__ProductId__c
,Apttus_Config2__ProductActive__c
,Apttus_Config2__RRAccountRuleId__c
,Apttus_Config2__RelatedItemId__c
,Apttus_Config2__RevRecPolicyId__c
,Apttus_Config2__RevenueSplitPolicyId__c
,SWT_ParentPriceListItem__c
,Apttus_Config2__TaxCodeId__c
,Apttus_Config2__UBARAccountRuleId__c
,Apttus_Config2__UBAccountRuleId__c
,CreatedById
,CurrencyIsoCode
,Name
,LastModifiedById
,Apttus_Config2__Active__c
,Apttus_Config2__AllocateGroupAdjustment__c
,Apttus_Config2__AllowManualAdjustment__c
,Apttus_Config2__AllowPriceRampOverlap__c
,Apttus_Config2__AllowProration__c
,APTS_Ext_ID__c
,Apttus_Config2__AutoCascadeQuantity__c
,Apttus_Config2__AutoCascadeSellingTerm__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalTerm__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__RollupPriceToBundle__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingRule__c
,SWT_BusinessMarkup__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Criteria__c
,Apttus_Config2__ContractPrice__c
,Apttus_Config2__Cost__c
,SWT_Currency__c
,Apttus_Config2__DefaultPriceDatasource__c
,Apttus_Config2__DefaultPriceFrom__c
,Apttus_Config2__DefaultQuantity__c
,Apttus_Config2__DefaultQuantityDatasource__c
,Apttus_Config2__DefaultQuantityFrom__c
,Apttus_Config2__DefaultSellingTerm__c
,Apttus_Config2__Description__c
,Apttus_Config2__DisableAssetIntegration__c
,Apttus_Config2__DisableSyncWithOpportunity__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EnableCommitment__c
,Apttus_Config2__EnablePriceRamp__c
,Apttus_Config2__ExpirationDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__HasCriteria__c
,SWT_HasMatrices__c
,SWT_Is_Cumulative_Range__c
,Apttus_Config2__IsQuantityReadOnly__c
,Apttus_Config2__IsSellingTermReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__MaxPrice__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinMaxPriceAppliesTo__c
,Apttus_Config2__MinPrice__c
,Apttus_Config2__MinUsageQuantity__c
,SWT_Overage_Type__c
,Apttus_Config2__RelatedPercent__c
,Apttus_Config2__RelatedPercentAppliesTo__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__ProductCode__c
,Apttus_Config2__ProductDescription__c
,Apttus_Config2__ProductFamily__c
,Apttus_Config2__ProductName__c
,SWT_ProductRatePlanChargeID__c
,SWT_ReferencePrice__c
,Apttus_Config2__RelatedAdjustmentAmount__c
,Apttus_Config2__RelatedAdjustmentAppliesTo__c
,Apttus_Config2__RelatedAdjustmentType__c
,Apttus_Config2__Sequence__c
,Apttus_Config2__SubType__c
,SWT_Market_Price__c
,SWT_PriceRate__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,Apttus_Config2__Type__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,LastModifiedDate
,SWT_GroupID__c
,LD_DT
,SWT_INS_DT
,d_source
)
select 
Id
,Apttus_Config2__ARAccountRuleId__c
,Apttus_Config2__DRAccountRuleId__c
,Apttus_Config2__NumberOfMatrices__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__ProductId__c
,Apttus_Config2__ProductActive__c
,Apttus_Config2__RRAccountRuleId__c
,Apttus_Config2__RelatedItemId__c
,Apttus_Config2__RevRecPolicyId__c
,Apttus_Config2__RevenueSplitPolicyId__c
,SWT_ParentPriceListItem__c
,Apttus_Config2__TaxCodeId__c
,Apttus_Config2__UBARAccountRuleId__c
,Apttus_Config2__UBAccountRuleId__c
,CreatedById
,CurrencyIsoCode
,Name
,LastModifiedById
,Apttus_Config2__Active__c
,Apttus_Config2__AllocateGroupAdjustment__c
,Apttus_Config2__AllowManualAdjustment__c
,Apttus_Config2__AllowPriceRampOverlap__c
,Apttus_Config2__AllowProration__c
,APTS_Ext_ID__c
,Apttus_Config2__AutoCascadeQuantity__c
,Apttus_Config2__AutoCascadeSellingTerm__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalTerm__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__RollupPriceToBundle__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingRule__c
,SWT_BusinessMarkup__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Criteria__c
,Apttus_Config2__ContractPrice__c
,Apttus_Config2__Cost__c
,SWT_Currency__c
,Apttus_Config2__DefaultPriceDatasource__c
,Apttus_Config2__DefaultPriceFrom__c
,Apttus_Config2__DefaultQuantity__c
,Apttus_Config2__DefaultQuantityDatasource__c
,Apttus_Config2__DefaultQuantityFrom__c
,Apttus_Config2__DefaultSellingTerm__c
,Apttus_Config2__Description__c
,Apttus_Config2__DisableAssetIntegration__c
,Apttus_Config2__DisableSyncWithOpportunity__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EnableCommitment__c
,Apttus_Config2__EnablePriceRamp__c
,Apttus_Config2__ExpirationDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__HasCriteria__c
,SWT_HasMatrices__c
,SWT_Is_Cumulative_Range__c
,Apttus_Config2__IsQuantityReadOnly__c
,Apttus_Config2__IsSellingTermReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__MaxPrice__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinMaxPriceAppliesTo__c
,Apttus_Config2__MinPrice__c
,Apttus_Config2__MinUsageQuantity__c
,SWT_Overage_Type__c
,Apttus_Config2__RelatedPercent__c
,Apttus_Config2__RelatedPercentAppliesTo__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__ProductCode__c
,Apttus_Config2__ProductDescription__c
,Apttus_Config2__ProductFamily__c
,Apttus_Config2__ProductName__c
,SWT_ProductRatePlanChargeID__c
,SWT_ReferencePrice__c
,Apttus_Config2__RelatedAdjustmentAmount__c
,Apttus_Config2__RelatedAdjustmentAppliesTo__c
,Apttus_Config2__RelatedAdjustmentType__c
,Apttus_Config2__Sequence__c
,Apttus_Config2__SubType__c
,SWT_Market_Price__c
,SWT_PriceRate__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,Apttus_Config2__Type__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,LastModifiedDate
,SWT_GroupID__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_Apttus_Config2__PriceListItem__c WHERE id in
(SELECT STG.id FROM AT_PriceList_stg_Tmp_Key STG join AT_PriceList_base_Tmp
ON STG.id = AT_PriceList_base_Tmp.id
AND STG.LastModifiedDate >= AT_PriceList_base_Tmp.LastModifiedDate);
-- Deleting before seven days data from current date in the Historical Table --  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_Apttus_Config2__PriceListItem__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */


-- Incremental VSQL script for loading data from Stage to Base --

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_Apttus_Config2__PriceListItem__c WHERE id in
(SELECT STG.id FROM AT_PriceList_stg_Tmp_Key STG join AT_PriceList_base_Tmp
ON STG.id = AT_PriceList_base_Tmp.id 
AND STG.LastModifiedDate >= AT_PriceList_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_Apttus_Config2__PriceListItem__c
(
Id
,Apttus_Config2__ARAccountRuleId__c
,Apttus_Config2__DRAccountRuleId__c
,Apttus_Config2__NumberOfMatrices__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__ProductId__c
,Apttus_Config2__ProductActive__c
,Apttus_Config2__RRAccountRuleId__c
,Apttus_Config2__RelatedItemId__c
,Apttus_Config2__RevRecPolicyId__c
,Apttus_Config2__RevenueSplitPolicyId__c
,SWT_ParentPriceListItem__c
,Apttus_Config2__TaxCodeId__c
,Apttus_Config2__UBARAccountRuleId__c
,Apttus_Config2__UBAccountRuleId__c
,CreatedById
,CurrencyIsoCode
,Name
,LastModifiedById
,Apttus_Config2__Active__c
,Apttus_Config2__AllocateGroupAdjustment__c
,Apttus_Config2__AllowManualAdjustment__c
,Apttus_Config2__AllowPriceRampOverlap__c
,Apttus_Config2__AllowProration__c
,APTS_Ext_ID__c
,Apttus_Config2__AutoCascadeQuantity__c
,Apttus_Config2__AutoCascadeSellingTerm__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalTerm__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__RollupPriceToBundle__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingRule__c
,SWT_BusinessMarkup__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Criteria__c
,Apttus_Config2__ContractPrice__c
,Apttus_Config2__Cost__c
,SWT_Currency__c
,Apttus_Config2__DefaultPriceDatasource__c
,Apttus_Config2__DefaultPriceFrom__c
,Apttus_Config2__DefaultQuantity__c
,Apttus_Config2__DefaultQuantityDatasource__c
,Apttus_Config2__DefaultQuantityFrom__c
,Apttus_Config2__DefaultSellingTerm__c
,Apttus_Config2__Description__c
,Apttus_Config2__DisableAssetIntegration__c
,Apttus_Config2__DisableSyncWithOpportunity__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EnableCommitment__c
,Apttus_Config2__EnablePriceRamp__c
,Apttus_Config2__ExpirationDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__HasCriteria__c
,SWT_HasMatrices__c
,SWT_Is_Cumulative_Range__c
,Apttus_Config2__IsQuantityReadOnly__c
,Apttus_Config2__IsSellingTermReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__MaxPrice__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinMaxPriceAppliesTo__c
,Apttus_Config2__MinPrice__c
,Apttus_Config2__MinUsageQuantity__c
,SWT_Overage_Type__c
,Apttus_Config2__RelatedPercent__c
,Apttus_Config2__RelatedPercentAppliesTo__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__ProductCode__c
,Apttus_Config2__ProductDescription__c
,Apttus_Config2__ProductFamily__c
,Apttus_Config2__ProductName__c
,SWT_ProductRatePlanChargeID__c
,SWT_ReferencePrice__c
,Apttus_Config2__RelatedAdjustmentAmount__c
,Apttus_Config2__RelatedAdjustmentAppliesTo__c
,Apttus_Config2__RelatedAdjustmentType__c
,Apttus_Config2__Sequence__c
,Apttus_Config2__SubType__c
,SWT_Market_Price__c
,SWT_PriceRate__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,Apttus_Config2__Type__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,LastModifiedDate
,SWT_GroupID__c
,SWT_INS_DT
)
SELECT DISTINCT 
AT_PriceList_stg_Tmp.Id
,Apttus_Config2__ARAccountRuleId__c
,Apttus_Config2__DRAccountRuleId__c
,Apttus_Config2__NumberOfMatrices__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__ProductId__c
,Apttus_Config2__ProductActive__c
,Apttus_Config2__RRAccountRuleId__c
,Apttus_Config2__RelatedItemId__c
,Apttus_Config2__RevRecPolicyId__c
,Apttus_Config2__RevenueSplitPolicyId__c
,SWT_ParentPriceListItem__c
,Apttus_Config2__TaxCodeId__c
,Apttus_Config2__UBARAccountRuleId__c
,Apttus_Config2__UBAccountRuleId__c
,CreatedById
,CurrencyIsoCode
,Name
,LastModifiedById
,Apttus_Config2__Active__c
,Apttus_Config2__AllocateGroupAdjustment__c
,Apttus_Config2__AllowManualAdjustment__c
,Apttus_Config2__AllowPriceRampOverlap__c
,Apttus_Config2__AllowProration__c
,APTS_Ext_ID__c
,Apttus_Config2__AutoCascadeQuantity__c
,Apttus_Config2__AutoCascadeSellingTerm__c
,Apttus_Config2__AutoRenew__c
,Apttus_Config2__AutoRenewalTerm__c
,Apttus_Config2__AutoRenewalType__c
,Apttus_Config2__RollupPriceToBundle__c
,Apttus_Config2__BillingFrequency__c
,Apttus_Config2__BillingRule__c
,SWT_BusinessMarkup__c
,Apttus_Config2__ChargeType__c
,Apttus_Config2__Criteria__c
,Apttus_Config2__ContractPrice__c
,Apttus_Config2__Cost__c
,SWT_Currency__c
,Apttus_Config2__DefaultPriceDatasource__c
,Apttus_Config2__DefaultPriceFrom__c
,Apttus_Config2__DefaultQuantity__c
,Apttus_Config2__DefaultQuantityDatasource__c
,Apttus_Config2__DefaultQuantityFrom__c
,Apttus_Config2__DefaultSellingTerm__c
,Apttus_Config2__Description__c
,Apttus_Config2__DisableAssetIntegration__c
,Apttus_Config2__DisableSyncWithOpportunity__c
,Apttus_Config2__EffectiveDate__c
,Apttus_Config2__EnableCommitment__c
,Apttus_Config2__EnablePriceRamp__c
,Apttus_Config2__ExpirationDate__c
,Apttus_Config2__Frequency__c
,Apttus_Config2__HasCriteria__c
,SWT_HasMatrices__c
,SWT_Is_Cumulative_Range__c
,Apttus_Config2__IsQuantityReadOnly__c
,Apttus_Config2__IsSellingTermReadOnly__c
,Apttus_Config2__IsUsageTierModifiable__c
,Apttus_Config2__ListPrice__c
,Apttus_Config2__MaxPrice__c
,Apttus_Config2__MaxUsageQuantity__c
,Apttus_Config2__MinMaxPriceAppliesTo__c
,Apttus_Config2__MinPrice__c
,Apttus_Config2__MinUsageQuantity__c
,SWT_Overage_Type__c
,Apttus_Config2__RelatedPercent__c
,Apttus_Config2__RelatedPercentAppliesTo__c
,Apttus_Config2__PriceIncludedInBundle__c
,Apttus_Config2__PriceMethod__c
,Apttus_Config2__PriceType__c
,Apttus_Config2__PriceUom__c
,Apttus_Config2__ProductCode__c
,Apttus_Config2__ProductDescription__c
,Apttus_Config2__ProductFamily__c
,Apttus_Config2__ProductName__c
,SWT_ProductRatePlanChargeID__c
,SWT_ReferencePrice__c
,Apttus_Config2__RelatedAdjustmentAmount__c
,Apttus_Config2__RelatedAdjustmentAppliesTo__c
,Apttus_Config2__RelatedAdjustmentType__c
,Apttus_Config2__Sequence__c
,Apttus_Config2__SubType__c
,SWT_Market_Price__c
,SWT_PriceRate__c
,Apttus_Config2__Taxable__c
,Apttus_Config2__TaxInclusive__c
,Apttus_Config2__Type__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,AT_PriceList_stg_Tmp.LastModifiedDate
,SWT_GroupID__c
,SYSDATE
FROM AT_PriceList_stg_Tmp JOIN AT_PriceList_stg_Tmp_Key 
ON AT_PriceList_stg_Tmp.id = AT_PriceList_stg_Tmp_Key.id 
AND AT_PriceList_stg_Tmp.LastModifiedDate=AT_PriceList_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM swt_rpt_base.AT_Apttus_Config2__PriceListItem__c BASE
WHERE AT_PriceList_stg_Tmp.id = BASE.id);

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
select 'APTTUS','AT_Apttus_Config2__PriceListItem__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_Apttus_Config2__PriceListItem__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
		
/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_Apttus_Config2__PriceListItem__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_Apttus_Config2__PriceListItem__c' and  COMPLTN_STAT = 'N');

Commit;
*/

SELECT DROP_PARTITIONS('swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_Apttus_Config2__PriceListItem__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_Apttus_Config2__PriceListItem__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
SELECT ANALYZE_STATISTICS('swt_rpt_base.AT_Apttus_Config2__PriceListItem__c');






