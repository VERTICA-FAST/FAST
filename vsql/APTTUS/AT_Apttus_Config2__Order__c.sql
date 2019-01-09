/****
****Script Name	  : AT_Apttus_Config2__Order__c.sql
****Description   : Incremental data load for AT_Apttus_Config2__Order__c
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_Apttus_Config2__Order__c";

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
select 'APTTUS','AT_Apttus_Config2__Order__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit; 

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_Apttus_Config2__Order__c_Hist select * from "swt_rpt_stg".AT_Apttus_Config2__Order__c;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_Apttus_Config2__Order__c where id in (
select id from swt_rpt_stg.AT_Apttus_Config2__Order__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_Apttus_Config2__Order__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_Apttus_Config2__Order__c.id=t2.id and swt_rpt_stg.AT_Apttus_Config2__Order__c.auto_id<t2.auto_id);

commit;


CREATE LOCAL TEMP TABLE AT_Apttus_Config2__Order__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.AT_Apttus_Config2__Order__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_Apttus_Config2__Order__c;

CREATE LOCAL TEMP TABLE AT_Apttus_Config2__Order__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.AT_Apttus_Config2__Order__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE AT_Apttus_Config2__Order__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM AT_Apttus_Config2__Order__c_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


/* Inserting deleted data into the Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.AT_Apttus_Config2__Order__c_Hist
(
Id
,OwnerId
,SWT_Generated_from_Primary_Quote__c
,Apttus_Config2__ParentOrderId__c
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Name
,Apttus_Config2__Accept__c
,SWT_Additional_Email_Five__c
,SWT_Additional_Email_Four__c
,SWT_Additional_Email_One__c
,SWT_Additional_Email_Three__c
,SWT_Additional_Email_Two__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AutoActivateOrder__c
,Apttus_Config2__BillingPreferenceId__c
,SWT_Billing_Timing__c
,Apttus_Config2__BillToAccountId__c
,SWT_Bill_To_Address__c
,SWT_Bill_To_Contact__c
,SWT_BillToContactEmail__c
,SWT_Booked_Engagement_Margin__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__CompletedDate__c
,Apttus_Config2__ConfigurationSyncDate__c
,Apttus_Config2__ConfigureNG__c
,SWT_Currency__c
,SWT_Delivery_To_Address__c
,SWT_Ship_To_Contact__c
,SWT_DeliveryToContactEmail_c__c
,Apttus_Config2__Description__c
,SWT_Enable_Line_Item_Shipping__c
,SWT_EndCustomer__c
,SWT_End_Customer_Address__c
,SWT_End_Customer_Contact__c
,SWT_End_Customer_Contact_Email__c
,SWT_External_Invoice_Memo__c
,SWT_Market_Route__c
,SWT_Has_Support_Option__c
,SWT_Influencer__c
,SWT_InternalId__c
,SWT_Invoice_Delivery_Preferance__c
,SWT_Invoice_ID__c
,Apttus_Config2__IsTaskPending__c
,SWT_Letter_of_Commitment__c
,Apttus_Config2__LocationId__c
,SWT_Order_Booking_Owner__c
,Apttus_Config2__OrderDate__c
,Apttus_Config2__OrderEndDate__c
,SWT_Order_Fulfillment_Date__c
,Apttus_Config2__OrderReferenceNumber__c
,Apttus_Config2__OrderStartDate__c
,SWT_OrderStatusBackUp__c
,SWT_OrderTCV__c
,SWT_Order_Type__c
,SWT_Payment_Methods__c
,Apttus_Config2__PaymentTermId__c
,SWT_PaymentTerms__c
,SWT_PO_Amount__c
,Apttus_Config2__PODate__c
,Apttus_Config2__PONumber__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__PrimaryContactId__c
,SWT_Profit_Center__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__ActivatedDate__c
,Apttus_Config2__ReadyForBillingDate__c
,Apttus_Config2__FulfilledDate__c
,Apttus_Config2__ReadyForRevRecDate__c
,Apttus_Config2__RelatedOpportunityId__c
,SWT_Zuora_Subscription_Name__c
,SWT_Sales_Rep__c
,SWT_Sales_Rep_Email__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__SoldToAccountId__c
,SWT_Sold_to_Contact__c
,SWT_Sold_To_Contact_Email__c
,Apttus_Config2__Source__c
,SWT_Source__c
,Apttus_Config2__Status__c
,SWT_Subscription_Activated__c
,SWT_Subsidiary__c
,SWT_Subsidiary_Code__c
,SWT_Add_on_Sub_Type__c
,Sync_with_Mulesoft__c
,SWT_Initial_Terms__c
,SWT_Tier_1_Partner__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_2_Relationship__c
,Apttus_Config2__Type__c
,SWT_Zuora_Subscription_ID__c
,SWT_Agreement_Order__c
,SWT_Open_Air_Project_ID__c
,SWT_OrderPickedByBatch__c
,SWT_Zuora_Request_Sent__c
,SWT_NetSuite_Project_ID__c
,SWT_Bid_Manager__c
,SWT_Risk_Rating__c
,SWT_License_Sale_Involved__c
,SWT_PO_Exempt__c
,SWT_Scope_of_Services__c
,SWT_Opportunity_ID__c
,SWT_License_Representative__c
,SWT_Revenue_Recognition_Method__c
,SWT_Federal__c
,SWT_Blended_Rates_Approval_Required__c
,SWT_Termination_Reason__c
,Apttus_FF_Agreement_Number__c
,SWT_Delivery_Without_Approval__c
,SWT_DWO_Project_ID__c
,SWT_Contract_Terms__c
,LastModifiedDate
,SWT_Term_Changed__c
,SWT_Return_to_Support__c
,SWT_Old_Term__c
,SWT_Merged_Subscription__c
,SWT_Billing_Plan__c
,SWT_SaaS_Contact__c
,SWT_Expected_Start_Date__c
,SWT_Line_of_Business__c
,SWT_Geography__c
,SWT_Old_Internal_Id__c
,SWT_Renewal_Opportunity_ID__c
,SWT_Order_Provisioning_Date__c
,SWT_Counter__c
,SWT_Fulfilled_Line_Items__c
,SWT_Sold_To_Address__c
,SWT_Upgrade_Related_Termination__c
,SWT_Quote_In_Validation_Date__c
,SWT_Order_Booking_Queue__c
,SWT_Proposal_Account__c
,SWT_Show_Original_Balance_to_Customer__c
,SWT_ELA_Agreement_Name__c
,SWT_ELA_Agreement_Number__c
,SWT_ELA_Fence_Type__c
,SWT_ELA_Original_Balance__c
,SWT_ELA_Program_Type__c
,SWT_Show_Available_Balance_to_Customer__c
,SWT_Show_Pricing_Info_to_Customer__c
,SWT_Show_Prod_List_Unit_Priceto_Customer__c
,SWT_Show_Prodct_Net_UnitPricetoCustomer__c
,SWT_Opportunity_Classification__c
,SWT_ULK_Approval_Status__c
,SWT_Entitled_to_Reallocation__c
,SWT_Transfer_Entitlement_to_ELA__c
,SWT_Reallocation_Frequency_Restriction__c
,SWT_Last_Reallocation_Prior_to_EndofTerm__c
,SWT_ELA_Percent_Balance_Threshold__c
,SWT_Shipment_Notice_Received__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWTOrderTCVUSD__c
,SWT_Old_Subscription_ID__c
,SWT_PO_Amount_USD__c
,SWT_Order_Total_Net_Price__c
,SWT_Order_Total_Unit_Price__c
,SWT_Integration_Error_Message__c
,SWT_Booked_Engagement_Margin_USD__c
,SWT_Old_Parent_Order_Id__c
,SWT_Zuora_Payment_Id__c
,SWT_Total_of_Line_items__c
,LD_DT
,SWT_INS_DT
,d_source
)
select 
Id
,OwnerId
,SWT_Generated_from_Primary_Quote__c
,Apttus_Config2__ParentOrderId__c
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Name
,Apttus_Config2__Accept__c
,SWT_Additional_Email_Five__c
,SWT_Additional_Email_Four__c
,SWT_Additional_Email_One__c
,SWT_Additional_Email_Three__c
,SWT_Additional_Email_Two__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AutoActivateOrder__c
,Apttus_Config2__BillingPreferenceId__c
,SWT_Billing_Timing__c
,Apttus_Config2__BillToAccountId__c
,SWT_Bill_To_Address__c
,SWT_Bill_To_Contact__c
,SWT_BillToContactEmail__c
,SWT_Booked_Engagement_Margin__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__CompletedDate__c
,Apttus_Config2__ConfigurationSyncDate__c
,Apttus_Config2__ConfigureNG__c
,SWT_Currency__c
,SWT_Delivery_To_Address__c
,SWT_Ship_To_Contact__c
,SWT_DeliveryToContactEmail_c__c
,Apttus_Config2__Description__c
,SWT_Enable_Line_Item_Shipping__c
,SWT_EndCustomer__c
,SWT_End_Customer_Address__c
,SWT_End_Customer_Contact__c
,SWT_End_Customer_Contact_Email__c
,SWT_External_Invoice_Memo__c
,SWT_Market_Route__c
,SWT_Has_Support_Option__c
,SWT_Influencer__c
,SWT_InternalId__c
,SWT_Invoice_Delivery_Preferance__c
,SWT_Invoice_ID__c
,Apttus_Config2__IsTaskPending__c
,SWT_Letter_of_Commitment__c
,Apttus_Config2__LocationId__c
,SWT_Order_Booking_Owner__c
,Apttus_Config2__OrderDate__c
,Apttus_Config2__OrderEndDate__c
,SWT_Order_Fulfillment_Date__c
,Apttus_Config2__OrderReferenceNumber__c
,Apttus_Config2__OrderStartDate__c
,SWT_OrderStatusBackUp__c
,SWT_OrderTCV__c
,SWT_Order_Type__c
,SWT_Payment_Methods__c
,Apttus_Config2__PaymentTermId__c
,SWT_PaymentTerms__c
,SWT_PO_Amount__c
,Apttus_Config2__PODate__c
,Apttus_Config2__PONumber__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__PrimaryContactId__c
,SWT_Profit_Center__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__ActivatedDate__c
,Apttus_Config2__ReadyForBillingDate__c
,Apttus_Config2__FulfilledDate__c
,Apttus_Config2__ReadyForRevRecDate__c
,Apttus_Config2__RelatedOpportunityId__c
,SWT_Zuora_Subscription_Name__c
,SWT_Sales_Rep__c
,SWT_Sales_Rep_Email__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__SoldToAccountId__c
,SWT_Sold_to_Contact__c
,SWT_Sold_To_Contact_Email__c
,Apttus_Config2__Source__c
,SWT_Source__c
,Apttus_Config2__Status__c
,SWT_Subscription_Activated__c
,SWT_Subsidiary__c
,SWT_Subsidiary_Code__c
,SWT_Add_on_Sub_Type__c
,Sync_with_Mulesoft__c
,SWT_Initial_Terms__c
,SWT_Tier_1_Partner__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_2_Relationship__c
,Apttus_Config2__Type__c
,SWT_Zuora_Subscription_ID__c
,SWT_Agreement_Order__c
,SWT_Open_Air_Project_ID__c
,SWT_OrderPickedByBatch__c
,SWT_Zuora_Request_Sent__c
,SWT_NetSuite_Project_ID__c
,SWT_Bid_Manager__c
,SWT_Risk_Rating__c
,SWT_License_Sale_Involved__c
,SWT_PO_Exempt__c
,SWT_Scope_of_Services__c
,SWT_Opportunity_ID__c
,SWT_License_Representative__c
,SWT_Revenue_Recognition_Method__c
,SWT_Federal__c
,SWT_Blended_Rates_Approval_Required__c
,SWT_Termination_Reason__c
,Apttus_FF_Agreement_Number__c
,SWT_Delivery_Without_Approval__c
,SWT_DWO_Project_ID__c
,SWT_Contract_Terms__c
,LastModifiedDate
,SWT_Term_Changed__c
,SWT_Return_to_Support__c
,SWT_Old_Term__c
,SWT_Merged_Subscription__c
,SWT_Billing_Plan__c
,SWT_SaaS_Contact__c
,SWT_Expected_Start_Date__c
,SWT_Line_of_Business__c
,SWT_Geography__c
,SWT_Old_Internal_Id__c
,SWT_Renewal_Opportunity_ID__c
,SWT_Order_Provisioning_Date__c
,SWT_Counter__c
,SWT_Fulfilled_Line_Items__c
,SWT_Sold_To_Address__c
,SWT_Upgrade_Related_Termination__c
,SWT_Quote_In_Validation_Date__c
,SWT_Order_Booking_Queue__c
,SWT_Proposal_Account__c
,SWT_Show_Original_Balance_to_Customer__c
,SWT_ELA_Agreement_Name__c
,SWT_ELA_Agreement_Number__c
,SWT_ELA_Fence_Type__c
,SWT_ELA_Original_Balance__c
,SWT_ELA_Program_Type__c
,SWT_Show_Available_Balance_to_Customer__c
,SWT_Show_Pricing_Info_to_Customer__c
,SWT_Show_Prod_List_Unit_Priceto_Customer__c
,SWT_Show_Prodct_Net_UnitPricetoCustomer__c
,SWT_Opportunity_Classification__c
,SWT_ULK_Approval_Status__c
,SWT_Entitled_to_Reallocation__c
,SWT_Transfer_Entitlement_to_ELA__c
,SWT_Reallocation_Frequency_Restriction__c
,SWT_Last_Reallocation_Prior_to_EndofTerm__c
,SWT_ELA_Percent_Balance_Threshold__c
,SWT_Shipment_Notice_Received__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWTOrderTCVUSD__c
,SWT_Old_Subscription_ID__c
,SWT_PO_Amount_USD__c
,SWT_Order_Total_Net_Price__c
,SWT_Order_Total_Unit_Price__c
,SWT_Integration_Error_Message__c
,SWT_Booked_Engagement_Margin_USD__c
,SWT_Old_Parent_Order_Id__c
,SWT_Zuora_Payment_Id__c
,SWT_Total_of_Line_items__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_Apttus_Config2__Order__c WHERE id in
(SELECT STG.id FROM AT_Apttus_Config2__Order__c_stg_Tmp_Key STG JOIN AT_Apttus_Config2__Order__c_base_Tmp
ON STG.id = AT_Apttus_Config2__Order__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_Config2__Order__c_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_Apttus_Config2__Order__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_Apttus_Config2__Order__c WHERE id in
(SELECT STG.id FROM AT_Apttus_Config2__Order__c_stg_Tmp_Key STG JOIN AT_Apttus_Config2__Order__c_base_Tmp
ON STG.id = AT_Apttus_Config2__Order__c_base_Tmp.id AND STG.LastModifiedDate >= AT_Apttus_Config2__Order__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_Apttus_Config2__Order__c
(
Id
,OwnerId
,SWT_Generated_from_Primary_Quote__c
,Apttus_Config2__ParentOrderId__c
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Name
,Apttus_Config2__Accept__c
,SWT_Additional_Email_Five__c
,SWT_Additional_Email_Four__c
,SWT_Additional_Email_One__c
,SWT_Additional_Email_Three__c
,SWT_Additional_Email_Two__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AutoActivateOrder__c
,Apttus_Config2__BillingPreferenceId__c
,SWT_Billing_Timing__c
,Apttus_Config2__BillToAccountId__c
,SWT_Bill_To_Address__c
,SWT_Bill_To_Contact__c
,SWT_BillToContactEmail__c
,SWT_Booked_Engagement_Margin__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__CompletedDate__c
,Apttus_Config2__ConfigurationSyncDate__c
,Apttus_Config2__ConfigureNG__c
,SWT_Currency__c
,SWT_Delivery_To_Address__c
,SWT_Ship_To_Contact__c
,SWT_DeliveryToContactEmail_c__c
,Apttus_Config2__Description__c
,SWT_Enable_Line_Item_Shipping__c
,SWT_EndCustomer__c
,SWT_End_Customer_Address__c
,SWT_End_Customer_Contact__c
,SWT_End_Customer_Contact_Email__c
,SWT_External_Invoice_Memo__c
,SWT_Market_Route__c
,SWT_Has_Support_Option__c
,SWT_Influencer__c
,SWT_InternalId__c
,SWT_Invoice_Delivery_Preferance__c
,SWT_Invoice_ID__c
,Apttus_Config2__IsTaskPending__c
,SWT_Letter_of_Commitment__c
,Apttus_Config2__LocationId__c
,SWT_Order_Booking_Owner__c
,Apttus_Config2__OrderDate__c
,Apttus_Config2__OrderEndDate__c
,SWT_Order_Fulfillment_Date__c
,Apttus_Config2__OrderReferenceNumber__c
,Apttus_Config2__OrderStartDate__c
,SWT_OrderStatusBackUp__c
,SWT_OrderTCV__c
,SWT_Order_Type__c
,SWT_Payment_Methods__c
,Apttus_Config2__PaymentTermId__c
,SWT_PaymentTerms__c
,SWT_PO_Amount__c
,Apttus_Config2__PODate__c
,Apttus_Config2__PONumber__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__PrimaryContactId__c
,SWT_Profit_Center__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__ActivatedDate__c
,Apttus_Config2__ReadyForBillingDate__c
,Apttus_Config2__FulfilledDate__c
,Apttus_Config2__ReadyForRevRecDate__c
,Apttus_Config2__RelatedOpportunityId__c
,SWT_Zuora_Subscription_Name__c
,SWT_Sales_Rep__c
,SWT_Sales_Rep_Email__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__SoldToAccountId__c
,SWT_Sold_to_Contact__c
,SWT_Sold_To_Contact_Email__c
,Apttus_Config2__Source__c
,SWT_Source__c
,Apttus_Config2__Status__c
,SWT_Subscription_Activated__c
,SWT_Subsidiary__c
,SWT_Subsidiary_Code__c
,SWT_Add_on_Sub_Type__c
,Sync_with_Mulesoft__c
,SWT_Initial_Terms__c
,SWT_Tier_1_Partner__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_2_Relationship__c
,Apttus_Config2__Type__c
,SWT_Zuora_Subscription_ID__c
,SWT_Agreement_Order__c
,SWT_Open_Air_Project_ID__c
,SWT_OrderPickedByBatch__c
,SWT_Zuora_Request_Sent__c
,SWT_NetSuite_Project_ID__c
,SWT_Bid_Manager__c
,SWT_Risk_Rating__c
,SWT_License_Sale_Involved__c
,SWT_PO_Exempt__c
,SWT_Scope_of_Services__c
,SWT_Opportunity_ID__c
,SWT_License_Representative__c
,SWT_Revenue_Recognition_Method__c
,SWT_Federal__c
,SWT_Blended_Rates_Approval_Required__c
,SWT_Termination_Reason__c
,Apttus_FF_Agreement_Number__c
,SWT_Delivery_Without_Approval__c
,SWT_DWO_Project_ID__c
,SWT_Contract_Terms__c
,LastModifiedDate
,SWT_Term_Changed__c
,SWT_Return_to_Support__c
,SWT_Old_Term__c
,SWT_Merged_Subscription__c
,SWT_Billing_Plan__c
,SWT_SaaS_Contact__c
,SWT_Expected_Start_Date__c
,SWT_Line_of_Business__c
,SWT_Geography__c
,SWT_Old_Internal_Id__c
,SWT_Renewal_Opportunity_ID__c
,SWT_Order_Provisioning_Date__c
,SWT_Counter__c
,SWT_Fulfilled_Line_Items__c
,SWT_Sold_To_Address__c
,SWT_Upgrade_Related_Termination__c
,SWT_Quote_In_Validation_Date__c
,SWT_Order_Booking_Queue__c
,SWT_Proposal_Account__c
,SWT_Show_Original_Balance_to_Customer__c
,SWT_ELA_Agreement_Name__c
,SWT_ELA_Agreement_Number__c
,SWT_ELA_Fence_Type__c
,SWT_ELA_Original_Balance__c
,SWT_ELA_Program_Type__c
,SWT_Show_Available_Balance_to_Customer__c
,SWT_Show_Pricing_Info_to_Customer__c
,SWT_Show_Prod_List_Unit_Priceto_Customer__c
,SWT_Show_Prodct_Net_UnitPricetoCustomer__c
,SWT_Opportunity_Classification__c
,SWT_ULK_Approval_Status__c
,SWT_Entitled_to_Reallocation__c
,SWT_Transfer_Entitlement_to_ELA__c
,SWT_Reallocation_Frequency_Restriction__c
,SWT_Last_Reallocation_Prior_to_EndofTerm__c
,SWT_ELA_Percent_Balance_Threshold__c
,SWT_Shipment_Notice_Received__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWTOrderTCVUSD__c
,SWT_Old_Subscription_ID__c
,SWT_PO_Amount_USD__c
,SWT_Order_Total_Net_Price__c
,SWT_Order_Total_Unit_Price__c
,SWT_Integration_Error_Message__c
,SWT_Booked_Engagement_Margin_USD__c
,SWT_Old_Parent_Order_Id__c
,SWT_Zuora_Payment_Id__c
,SWT_Total_of_Line_items__c
,SWT_INS_DT
)
SELECT DISTINCT 
AT_Apttus_Config2__Order__c_stg_Tmp.Id
,OwnerId
,SWT_Generated_from_Primary_Quote__c
,Apttus_Config2__ParentOrderId__c
,CreatedById
,CurrencyIsoCode
,LastModifiedById
,Name
,Apttus_Config2__Accept__c
,SWT_Additional_Email_Five__c
,SWT_Additional_Email_Four__c
,SWT_Additional_Email_One__c
,SWT_Additional_Email_Three__c
,SWT_Additional_Email_Two__c
,Apttus_CMConfig__AgreementId__c
,Apttus_Config2__AutoActivateOrder__c
,Apttus_Config2__BillingPreferenceId__c
,SWT_Billing_Timing__c
,Apttus_Config2__BillToAccountId__c
,SWT_Bill_To_Address__c
,SWT_Bill_To_Contact__c
,SWT_BillToContactEmail__c
,SWT_Booked_Engagement_Margin__c
,Apttus_Config2__CancelledDate__c
,Apttus_Config2__CompletedDate__c
,Apttus_Config2__ConfigurationSyncDate__c
,Apttus_Config2__ConfigureNG__c
,SWT_Currency__c
,SWT_Delivery_To_Address__c
,SWT_Ship_To_Contact__c
,SWT_DeliveryToContactEmail_c__c
,Apttus_Config2__Description__c
,SWT_Enable_Line_Item_Shipping__c
,SWT_EndCustomer__c
,SWT_End_Customer_Address__c
,SWT_End_Customer_Contact__c
,SWT_End_Customer_Contact_Email__c
,SWT_External_Invoice_Memo__c
,SWT_Market_Route__c
,SWT_Has_Support_Option__c
,SWT_Influencer__c
,SWT_InternalId__c
,SWT_Invoice_Delivery_Preferance__c
,SWT_Invoice_ID__c
,Apttus_Config2__IsTaskPending__c
,SWT_Letter_of_Commitment__c
,Apttus_Config2__LocationId__c
,SWT_Order_Booking_Owner__c
,Apttus_Config2__OrderDate__c
,Apttus_Config2__OrderEndDate__c
,SWT_Order_Fulfillment_Date__c
,Apttus_Config2__OrderReferenceNumber__c
,Apttus_Config2__OrderStartDate__c
,SWT_OrderStatusBackUp__c
,SWT_OrderTCV__c
,SWT_Order_Type__c
,SWT_Payment_Methods__c
,Apttus_Config2__PaymentTermId__c
,SWT_PaymentTerms__c
,SWT_PO_Amount__c
,Apttus_Config2__PODate__c
,Apttus_Config2__PONumber__c
,Apttus_Config2__PriceListId__c
,Apttus_Config2__PricingDate__c
,Apttus_Config2__PrimaryContactId__c
,SWT_Profit_Center__c
,Apttus_QPConfig__ProposalId__c
,Apttus_Config2__ActivatedDate__c
,Apttus_Config2__ReadyForBillingDate__c
,Apttus_Config2__FulfilledDate__c
,Apttus_Config2__ReadyForRevRecDate__c
,Apttus_Config2__RelatedOpportunityId__c
,SWT_Zuora_Subscription_Name__c
,SWT_Sales_Rep__c
,SWT_Sales_Rep_Email__c
,Apttus_Config2__ShipToAccountId__c
,Apttus_Config2__SoldToAccountId__c
,SWT_Sold_to_Contact__c
,SWT_Sold_To_Contact_Email__c
,Apttus_Config2__Source__c
,SWT_Source__c
,Apttus_Config2__Status__c
,SWT_Subscription_Activated__c
,SWT_Subsidiary__c
,SWT_Subsidiary_Code__c
,SWT_Add_on_Sub_Type__c
,Sync_with_Mulesoft__c
,SWT_Initial_Terms__c
,SWT_Tier_1_Partner__c
,SWT_Tier_1_Relationship__c
,SWT_Tier_2_Partner__c
,SWT_Tier_2_Relationship__c
,Apttus_Config2__Type__c
,SWT_Zuora_Subscription_ID__c
,SWT_Agreement_Order__c
,SWT_Open_Air_Project_ID__c
,SWT_OrderPickedByBatch__c
,SWT_Zuora_Request_Sent__c
,SWT_NetSuite_Project_ID__c
,SWT_Bid_Manager__c
,SWT_Risk_Rating__c
,SWT_License_Sale_Involved__c
,SWT_PO_Exempt__c
,SWT_Scope_of_Services__c
,SWT_Opportunity_ID__c
,SWT_License_Representative__c
,SWT_Revenue_Recognition_Method__c
,SWT_Federal__c
,SWT_Blended_Rates_Approval_Required__c
,SWT_Termination_Reason__c
,Apttus_FF_Agreement_Number__c
,SWT_Delivery_Without_Approval__c
,SWT_DWO_Project_ID__c
,SWT_Contract_Terms__c
,AT_Apttus_Config2__Order__c_stg_Tmp.LastModifiedDate
,SWT_Term_Changed__c
,SWT_Return_to_Support__c
,SWT_Old_Term__c
,SWT_Merged_Subscription__c
,SWT_Billing_Plan__c
,SWT_SaaS_Contact__c
,SWT_Expected_Start_Date__c
,SWT_Line_of_Business__c
,SWT_Geography__c
,SWT_Old_Internal_Id__c
,SWT_Renewal_Opportunity_ID__c
,SWT_Order_Provisioning_Date__c
,SWT_Counter__c
,SWT_Fulfilled_Line_Items__c
,SWT_Sold_To_Address__c
,SWT_Upgrade_Related_Termination__c
,SWT_Quote_In_Validation_Date__c
,SWT_Order_Booking_Queue__c
,SWT_Proposal_Account__c
,SWT_Show_Original_Balance_to_Customer__c
,SWT_ELA_Agreement_Name__c
,SWT_ELA_Agreement_Number__c
,SWT_ELA_Fence_Type__c
,SWT_ELA_Original_Balance__c
,SWT_ELA_Program_Type__c
,SWT_Show_Available_Balance_to_Customer__c
,SWT_Show_Pricing_Info_to_Customer__c
,SWT_Show_Prod_List_Unit_Priceto_Customer__c
,SWT_Show_Prodct_Net_UnitPricetoCustomer__c
,SWT_Opportunity_Classification__c
,SWT_ULK_Approval_Status__c
,SWT_Entitled_to_Reallocation__c
,SWT_Transfer_Entitlement_to_ELA__c
,SWT_Reallocation_Frequency_Restriction__c
,SWT_Last_Reallocation_Prior_to_EndofTerm__c
,SWT_ELA_Percent_Balance_Threshold__c
,SWT_Shipment_Notice_Received__c
,IsDeleted
,CreatedDate
,SystemModstamp
,LastActivityDate
,LastViewedDate
,LastReferencedDate
,SWTOrderTCVUSD__c
,SWT_Old_Subscription_ID__c
,SWT_PO_Amount_USD__c
,SWT_Order_Total_Net_Price__c
,SWT_Order_Total_Unit_Price__c
,SWT_Integration_Error_Message__c
,SWT_Booked_Engagement_Margin_USD__c
,SWT_Old_Parent_Order_Id__c
,SWT_Zuora_Payment_Id__c
,SWT_Total_of_Line_items__c
,SYSDATE
FROM AT_Apttus_Config2__Order__c_stg_Tmp JOIN AT_Apttus_Config2__Order__c_stg_Tmp_Key ON AT_Apttus_Config2__Order__c_stg_Tmp.id= AT_Apttus_Config2__Order__c_stg_Tmp_Key.id AND AT_Apttus_Config2__Order__c_stg_Tmp.LastModifiedDate=AT_Apttus_Config2__Order__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".AT_Apttus_Config2__Order__c BASE
WHERE AT_Apttus_Config2__Order__c_stg_Tmp.id = BASE.id);

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
select 'APTTUS','AT_Apttus_Config2__Order__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_Apttus_Config2__Order__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_Apttus_Config2__Order__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_Apttus_Config2__Order__c' and  COMPLTN_STAT = 'N');
Commit;*/
    
SELECT DROP_PARTITIONS('swt_rpt_stg.AT_Apttus_Config2__Order__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_Apttus_Config2__Order__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_Apttus_Config2__Order__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.AT_Apttus_Config2__Order__c');



