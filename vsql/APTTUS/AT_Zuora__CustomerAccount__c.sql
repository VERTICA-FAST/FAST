/****
****Script Name	  : AT_Zuora__CustomerAccount__c.sql
****Description   : Incremental data load for AT_Zuora__CustomerAccount__c
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_Zuora__CustomerAccount__c";

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
select 'APTTUS','AT_Zuora__CustomerAccount__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_Zuora__CustomerAccount__c_Hist select * from "swt_rpt_stg".AT_Zuora__CustomerAccount__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_Zuora__CustomerAccount__c where id in (
select id from swt_rpt_stg.AT_Zuora__CustomerAccount__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_Zuora__CustomerAccount__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_Zuora__CustomerAccount__c.id=t2.id and swt_rpt_stg.AT_Zuora__CustomerAccount__c.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE AT_Zuora__CustomerAccount__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.AT_Zuora__CustomerAccount__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_Zuora__CustomerAccount__c;

CREATE LOCAL TEMP TABLE AT_Zuora__CustomerAccount__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.AT_Zuora__CustomerAccount__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE AT_Zuora__CustomerAccount__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM AT_Zuora__CustomerAccount__c_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

/* Inserting deleted data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.AT_Zuora__CustomerAccount__c_Hist
(
Id
,Zuora__Account__c
,Zuora__Default_Payment_Method__c
,Zuora__External_Id__c
,Zuora__Parent__c
,Zuora__PaymentGateway__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Zuora__AccountNumber__c
,Zuora__AdditionalEmailAddresses__c
,Zuora__AllowInvoiceEdit__c
,Zuora__AutoPay__c
,Zuora__Batch__c
,Zuora__BcdSettingOption__c
,Zuora__BillCycleDay__c
,Zuora__Balance__c
,Zuora__BillToCity__c
,Zuora__BillToCountry__c
,Zuora__BillToAddress1__c
,Zuora__BillToAddress2__c
,Zuora__BillToState__c
,Zuora__BillToPostalCode__c
,Zuora__BillToId__c
,Zuora__BillToFax__c
,Zuora__BillToName__c
,Zuora__BillToWorkEmail__c
,Zuora__BillToWorkPhone__c
,Zuora__MRR__c
,Zuora__Communication_Profile_Id__c
,Zuora__CommunicationProfileId__c
,Zuora__Credit_Balance__c
,Zuora__CreditBalance__c
,Zuora__CreditCard_Expiration__c
,Zuora__CreditCardExpiration__c
,Zuora__CreditCard_Number__c
,Zuora__CreditCardNumber__c
,Zuora__CreditCardType__c
,Zuora__PaymentMethodType__c
,Zuora__CustomerServiceRepName__c
,Zuora__Currency__c
,Zuora__DefaultPaymentMethod__c
,Zuora__EntityID__c
,Zuora__InvoiceDeliveryPrefsEmail__c
,Zuora__InvoiceDeliveryPrefsPrint__c
,Zuora__InvoiceTemplateId__c
,Zuora__Is_Crm_Id_Change_Processed__c
,Zuora__LastInvoiceDate__c
,Zuora__Notes__c
,Zuora__PaymentMethod_Type__c
,Zuora__PaymentTerm__c
,Zuora__Payment_Term_Formula__c
,Zuora__Payment_Term__c
,Zuora__PurchaseOrderNumber__c
,Zuora__SalesRepName__c
,Zuora__SoldToAddress1__c
,Zuora__SoldToAddress2__c
,Zuora__SoldToCity__c
,Zuora__SoldToId__c
,Zuora__SoldToCountry__c
,Zuora__SoldToFax__c
,Zuora__SoldToName__c
,Zuora__SoldToPostalCode__c
,Zuora__SoldToState__c
,Zuora__SoldToWorkEmail__c
,Zuora__SoldToWorkPhone__c
,Zuora__Status__c
,Zuora__TaxExemptCertificateID__c
,Zuora__TaxExemptCertificateType__c
,Zuora__TaxExemptDescription__c
,Zuora__TaxExemptEffectiveDate__c
,Zuora__TaxExemptExpirationDate__c
,Zuora__TaxExemptIssuingJurisdiction__c
,Zuora__TaxExemptStatus__c
,Zuora__TotalInvoiceBalance__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,Zuora__Zuora_Id__c
,LastModifiedDate
,LD_DT
,SWT_INS_DT
,d_source
)
 select 
Id
,Zuora__Account__c
,Zuora__Default_Payment_Method__c
,Zuora__External_Id__c
,Zuora__Parent__c
,Zuora__PaymentGateway__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Zuora__AccountNumber__c
,Zuora__AdditionalEmailAddresses__c
,Zuora__AllowInvoiceEdit__c
,Zuora__AutoPay__c
,Zuora__Batch__c
,Zuora__BcdSettingOption__c
,Zuora__BillCycleDay__c
,Zuora__Balance__c
,Zuora__BillToCity__c
,Zuora__BillToCountry__c
,Zuora__BillToAddress1__c
,Zuora__BillToAddress2__c
,Zuora__BillToState__c
,Zuora__BillToPostalCode__c
,Zuora__BillToId__c
,Zuora__BillToFax__c
,Zuora__BillToName__c
,Zuora__BillToWorkEmail__c
,Zuora__BillToWorkPhone__c
,Zuora__MRR__c
,Zuora__Communication_Profile_Id__c
,Zuora__CommunicationProfileId__c
,Zuora__Credit_Balance__c
,Zuora__CreditBalance__c
,Zuora__CreditCard_Expiration__c
,Zuora__CreditCardExpiration__c
,Zuora__CreditCard_Number__c
,Zuora__CreditCardNumber__c
,Zuora__CreditCardType__c
,Zuora__PaymentMethodType__c
,Zuora__CustomerServiceRepName__c
,Zuora__Currency__c
,Zuora__DefaultPaymentMethod__c
,Zuora__EntityID__c
,Zuora__InvoiceDeliveryPrefsEmail__c
,Zuora__InvoiceDeliveryPrefsPrint__c
,Zuora__InvoiceTemplateId__c
,Zuora__Is_Crm_Id_Change_Processed__c
,Zuora__LastInvoiceDate__c
,Zuora__Notes__c
,Zuora__PaymentMethod_Type__c
,Zuora__PaymentTerm__c
,Zuora__Payment_Term_Formula__c
,Zuora__Payment_Term__c
,Zuora__PurchaseOrderNumber__c
,Zuora__SalesRepName__c
,Zuora__SoldToAddress1__c
,Zuora__SoldToAddress2__c
,Zuora__SoldToCity__c
,Zuora__SoldToId__c
,Zuora__SoldToCountry__c
,Zuora__SoldToFax__c
,Zuora__SoldToName__c
,Zuora__SoldToPostalCode__c
,Zuora__SoldToState__c
,Zuora__SoldToWorkEmail__c
,Zuora__SoldToWorkPhone__c
,Zuora__Status__c
,Zuora__TaxExemptCertificateID__c
,Zuora__TaxExemptCertificateType__c
,Zuora__TaxExemptDescription__c
,Zuora__TaxExemptEffectiveDate__c
,Zuora__TaxExemptExpirationDate__c
,Zuora__TaxExemptIssuingJurisdiction__c
,Zuora__TaxExemptStatus__c
,Zuora__TotalInvoiceBalance__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,Zuora__Zuora_Id__c
,LastModifiedDate
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_Zuora__CustomerAccount__c WHERE id in
(SELECT STG.id FROM AT_Zuora__CustomerAccount__c_stg_Tmp_Key STG JOIN AT_Zuora__CustomerAccount__c_base_Tmp
ON STG.id = AT_Zuora__CustomerAccount__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Zuora__CustomerAccount__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_Zuora__CustomerAccount__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_Zuora__CustomerAccount__c WHERE id in
(SELECT STG.id FROM AT_Zuora__CustomerAccount__c_stg_Tmp_Key STG JOIN AT_Zuora__CustomerAccount__c_base_Tmp
ON STG.id = AT_Zuora__CustomerAccount__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Zuora__CustomerAccount__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_Zuora__CustomerAccount__c
(
Id
,Zuora__Account__c
,Zuora__Default_Payment_Method__c
,Zuora__External_Id__c
,Zuora__Parent__c
,Zuora__PaymentGateway__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Zuora__AccountNumber__c
,Zuora__AdditionalEmailAddresses__c
,Zuora__AllowInvoiceEdit__c
,Zuora__AutoPay__c
,Zuora__Batch__c
,Zuora__BcdSettingOption__c
,Zuora__BillCycleDay__c
,Zuora__Balance__c
,Zuora__BillToCity__c
,Zuora__BillToCountry__c
,Zuora__BillToAddress1__c
,Zuora__BillToAddress2__c
,Zuora__BillToState__c
,Zuora__BillToPostalCode__c
,Zuora__BillToId__c
,Zuora__BillToFax__c
,Zuora__BillToName__c
,Zuora__BillToWorkEmail__c
,Zuora__BillToWorkPhone__c
,Zuora__MRR__c
,Zuora__Communication_Profile_Id__c
,Zuora__CommunicationProfileId__c
,Zuora__Credit_Balance__c
,Zuora__CreditBalance__c
,Zuora__CreditCard_Expiration__c
,Zuora__CreditCardExpiration__c
,Zuora__CreditCard_Number__c
,Zuora__CreditCardNumber__c
,Zuora__CreditCardType__c
,Zuora__PaymentMethodType__c
,Zuora__CustomerServiceRepName__c
,Zuora__Currency__c
,Zuora__DefaultPaymentMethod__c
,Zuora__EntityID__c
,Zuora__InvoiceDeliveryPrefsEmail__c
,Zuora__InvoiceDeliveryPrefsPrint__c
,Zuora__InvoiceTemplateId__c
,Zuora__Is_Crm_Id_Change_Processed__c
,Zuora__LastInvoiceDate__c
,Zuora__Notes__c
,Zuora__PaymentMethod_Type__c
,Zuora__PaymentTerm__c
,Zuora__Payment_Term_Formula__c
,Zuora__Payment_Term__c
,Zuora__PurchaseOrderNumber__c
,Zuora__SalesRepName__c
,Zuora__SoldToAddress1__c
,Zuora__SoldToAddress2__c
,Zuora__SoldToCity__c
,Zuora__SoldToId__c
,Zuora__SoldToCountry__c
,Zuora__SoldToFax__c
,Zuora__SoldToName__c
,Zuora__SoldToPostalCode__c
,Zuora__SoldToState__c
,Zuora__SoldToWorkEmail__c
,Zuora__SoldToWorkPhone__c
,Zuora__Status__c
,Zuora__TaxExemptCertificateID__c
,Zuora__TaxExemptCertificateType__c
,Zuora__TaxExemptDescription__c
,Zuora__TaxExemptEffectiveDate__c
,Zuora__TaxExemptExpirationDate__c
,Zuora__TaxExemptIssuingJurisdiction__c
,Zuora__TaxExemptStatus__c
,Zuora__TotalInvoiceBalance__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,Zuora__Zuora_Id__c
,LastModifiedDate
,SWT_INS_DT 
)
SELECT DISTINCT 
AT_Zuora__CustomerAccount__c_stg_Tmp.Id
,Zuora__Account__c
,Zuora__Default_Payment_Method__c
,Zuora__External_Id__c
,Zuora__Parent__c
,Zuora__PaymentGateway__c
,Name
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Zuora__AccountNumber__c
,Zuora__AdditionalEmailAddresses__c
,Zuora__AllowInvoiceEdit__c
,Zuora__AutoPay__c
,Zuora__Batch__c
,Zuora__BcdSettingOption__c
,Zuora__BillCycleDay__c
,Zuora__Balance__c
,Zuora__BillToCity__c
,Zuora__BillToCountry__c
,Zuora__BillToAddress1__c
,Zuora__BillToAddress2__c
,Zuora__BillToState__c
,Zuora__BillToPostalCode__c
,Zuora__BillToId__c
,Zuora__BillToFax__c
,Zuora__BillToName__c
,Zuora__BillToWorkEmail__c
,Zuora__BillToWorkPhone__c
,Zuora__MRR__c
,Zuora__Communication_Profile_Id__c
,Zuora__CommunicationProfileId__c
,Zuora__Credit_Balance__c
,Zuora__CreditBalance__c
,Zuora__CreditCard_Expiration__c
,Zuora__CreditCardExpiration__c
,Zuora__CreditCard_Number__c
,Zuora__CreditCardNumber__c
,Zuora__CreditCardType__c
,Zuora__PaymentMethodType__c
,Zuora__CustomerServiceRepName__c
,Zuora__Currency__c
,Zuora__DefaultPaymentMethod__c
,Zuora__EntityID__c
,Zuora__InvoiceDeliveryPrefsEmail__c
,Zuora__InvoiceDeliveryPrefsPrint__c
,Zuora__InvoiceTemplateId__c
,Zuora__Is_Crm_Id_Change_Processed__c
,Zuora__LastInvoiceDate__c
,Zuora__Notes__c
,Zuora__PaymentMethod_Type__c
,Zuora__PaymentTerm__c
,Zuora__Payment_Term_Formula__c
,Zuora__Payment_Term__c
,Zuora__PurchaseOrderNumber__c
,Zuora__SalesRepName__c
,Zuora__SoldToAddress1__c
,Zuora__SoldToAddress2__c
,Zuora__SoldToCity__c
,Zuora__SoldToId__c
,Zuora__SoldToCountry__c
,Zuora__SoldToFax__c
,Zuora__SoldToName__c
,Zuora__SoldToPostalCode__c
,Zuora__SoldToState__c
,Zuora__SoldToWorkEmail__c
,Zuora__SoldToWorkPhone__c
,Zuora__Status__c
,Zuora__TaxExemptCertificateID__c
,Zuora__TaxExemptCertificateType__c
,Zuora__TaxExemptDescription__c
,Zuora__TaxExemptEffectiveDate__c
,Zuora__TaxExemptExpirationDate__c
,Zuora__TaxExemptIssuingJurisdiction__c
,Zuora__TaxExemptStatus__c
,Zuora__TotalInvoiceBalance__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,Zuora__Zuora_Id__c
,AT_Zuora__CustomerAccount__c_stg_Tmp.LastModifiedDate
,sysdate as SWT_INS_DT 
FROM AT_Zuora__CustomerAccount__c_stg_Tmp JOIN AT_Zuora__CustomerAccount__c_stg_Tmp_Key ON AT_Zuora__CustomerAccount__c_stg_Tmp.id= AT_Zuora__CustomerAccount__c_stg_Tmp_Key.id AND AT_Zuora__CustomerAccount__c_stg_Tmp.LastModifiedDate=AT_Zuora__CustomerAccount__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".AT_Zuora__CustomerAccount__c BASE
WHERE AT_Zuora__CustomerAccount__c_stg_Tmp.id = BASE.id);

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
select 'APTTUS','AT_Zuora__CustomerAccount__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_Zuora__CustomerAccount__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_Zuora__CustomerAccount__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_Zuora__CustomerAccount__c' and  COMPLTN_STAT = 'N');
Commit;*/

SELECT DROP_PARTITIONS('swt_rpt_stg.AT_Zuora__CustomerAccount__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_Zuora__CustomerAccount__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_Zuora__CustomerAccount__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
SELECT ANALYZE_STATISTICS('swt_rpt_base.AT_Zuora__CustomerAccount__c');







