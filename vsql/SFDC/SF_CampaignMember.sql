
----Script Name   : SF_CampaignMember.sql
----Description   : Incremental data load for SF_CampaignMember


/*Setting timing on */
\timing

--SET SESSION AUTOCOMMIT TO OFF;

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_CampaignMember";

--- Inserting values into Audit table

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
  select 'SFDC','SF_CampaignMember',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

  Commit;  

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_CampaignMember_Hist SELECT * from swt_rpt_stg.SF_CampaignMember;
COMMIT;  

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_CampaignMember where id in (
select id from swt_rpt_stg.SF_CampaignMember group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_CampaignMember where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_CampaignMember.id=t2.id and swt_rpt_stg.SF_CampaignMember.auto_id<t2.auto_id);

Commit; 
  
  

CREATE LOCAL TEMP TABLE SF_CampaignMember_stg_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT * FROM swt_rpt_stg.SF_CampaignMember)
SEGMENTED BY HASH(ID,LASTMODIFIEDDATE) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_CampaignMember;

CREATE LOCAL TEMP TABLE SF_CampaignMember_base_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT ID,LASTMODIFIEDDATE FROM swt_rpt_base.SF_CampaignMember)
SEGMENTED BY HASH(ID,LASTMODIFIEDDATE) ALL NODES;


CREATE LOCAL TEMP TABLE SF_CampaignMember_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(lastmodifieddate) as lastmodifieddate FROM SF_CampaignMember_stg_Tmp group by id) 
SEGMENTED BY HASH(ID,LASTMODIFIEDDATE) ALL NODES;
  
  
  
-- Inserting Stage table data into Historical Table

insert /*+DIRECT*/ into swt_rpt_stg.SF_CampaignMember_Hist
(
Id
,IsDeleted
,CampaignId
,LeadId
,ContactId
,Status
,HasResponded
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,FirstRespondedDate
,CurrencyIsoCode
,Salutation
,Name
,FirstName
,LastName
,Title
,Street
,City
,State
,PostalCode
,Country
,Email
,Phone
,Fax
,MobilePhone
,Description
,DoNotCall
,HasOptedOutOfEmail
,HasOptedOutOfFax
,LeadSource
,CompanyOrAccount
,Type
,LeadOrContactId
,LeadOrContactOwnerId
,SWT_Primary__c
,SWT_Activity__c
,SWT_Source_Business_Unit__c
,SWT_Source_Country__c
,SWT_Source_Email__c
,SWT_Source_First_Name__c
,SWT_Source_Last_Name__c
,SWT_CampaignMemberExternal_Id__c
,SWT_Is_Primary__c
,SWT_Campaign_Member_External_ID__c
,et4ae5__Activity__c
,CFCR_Account__c
,CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_AQL_Owner__c
,CFCR_AQL_Owner_Role__c
,CFCR_AQL_to_MQL__c
,CFCR_AQL_to_SAL__c
,CFCR_Business_Unit__c
,CFCR_Current_Stage_in_Funnel__c
,CFCR_Lost_Reason__c
,CFCR_MQL_Owner__c
,CFCR_MQL_Owner_Role__c
,CFCR_MQL_to_Won__c
,CFCR_Opportunity_18_Digit_ID__c
,CFCR_Opportunity_Stage__c
,CFCR_Opportunity_Type__c
,CFCR_pi_utm_campaign__c
,CFCR_pi_utm_content__c
,CFCR_pi_utm_medium__c
,CFCR_pi_utm_source__c
,CFCR_pi_utm_term__c
,CFCR_Response_to_AQL__c
,CFCR_SAL_Owner_Role__c
,CFCR_SQL_Owner__c
,CFCR_SQL_Owner_Role__c
,CFCR_SQL_to_SQO__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CFCR_SQO_Owner__c
,CFCR_SQO_Owner_Role__c
,CFCR_SQO_to_Won__c
,CFCR_Status_Reason__c
,CFCR_Still_in_AQL__c
,CFCR_Still_in_Inquiry__c
,CFCR_Still_in_MQL__c
,CFCR_Still_in_SAL__c
,CFCR_Still_in_SQL__c
,CFCR_Still_in_SQO__c
,CFCR_Won_Owner__c
,CFCR_Won_Owner_Role__c
,CFCR_SRL__c
,CFCR_SRL_Date__c
,FCRM__FCR_18_Character_ID__c
,FCRM__FCR_Campaign_Repeat_Parent__c
,FCRM__FCR_Current_Lead_Contact_Status__c
,FCRM__FCR_First_Owner_Assigned__c
,FCRM__FCR_First_Owner_Type__c
,FCRM__FCR_First_Owner_Worked__c
,FCRM__FCR_Inquiry_Target__c
,FCRM__FCR_Inquiry_Target_Date__c
,FCRM__FCR_Inquiry_to_MQR__c
,FCRM__FCR_Lead_Contact_18_Digit_ID__c
,FCRM__FCR_Member_Type_On_Create__c
,FCRM__FCR_MQR_to_SAR__c
,FCRM__FCR_Net_New_Lead__c
,FCRM__FCR_Opportunity__c
,FCRM__FCR_Opportunity_Amount__c
,FCRM__FCR_Opportunity_Closed_Date__c
,FCRM__FCR_QR__c
,FCRM__FCR_QR_Date__c
,FCRM__FCR_Response_Date__c
,FCRM__FCR_Response_Score__c
,FCRM__FCR_Response_Status__c
,FCRM__FCR_SAR__c
,FCRM__FCR_SAR_Date__c
,FCRM__FCR_SAR_Owner__c
,FCRM__FCR_SAR_to_SQR__c
,FCRM__FCR_SQR__c
,FCRM__FCR_SQR_Date__c
,FCRM__FCR_SQR_to_Closed_Won__c
,FCRM__FCR_SQR_Won__c
,FCRM__FCR_Status_Age__c
,FCRM__FCR_View_Response__c
,CFCR_Actionable__c
,CFCR_Admin_Defective_Member__c
,CFCR_Annual_Revenue__c
,CFCR_BANT_Criteria_Completed__c
,CFCR_Count__c
,CFCR_Created_By_Formula__c
,CFCR_Current_Stage_In_Funnel_Date__c
,CFCR_FCCRM_Threshold_Current__c
,CFCR_FCCRM_Threshold_On_Create__c
,CFCR_Industry_Formula__c
,CFCR_Lead_Contact_Create_Date_Time__c
,CFCR_Lead_Source_Formula__c
,CFCR_Manual_Adjustment__c
,CFCR_Manual_Adjustment_Notes__c
,CFCR_No_of_Employees__c
,CFCR_pi_campaign__c
,CFCR_pi_grade__c
,CFCR_pi_grade_On_Update__c
,CFCR_pi_last_activity__c
,CFCR_pi_score__c
,CFCR_pi_score_On_Update__c
,CFCR_pi_url__c
,CFCR_Region__c
,CFCR_TEM_Trial__c
,FCRM__FCR_Admin_Opportunity_Status__c
,FCRM__FCR_Admin_Response_Control__c
,FCRM__FCR_Admin_Response_Day__c
,FCRM__FCR_Admin_SyncPending__c
,FCRM__FCR_Admin_SyncTest__c
,FCRM__FCR_Admin_Update_Counter__c
,FCRM__FCR_CascadeID__c
,FCRM__FCR_ClosedOpRevenueModel1__c
,FCRM__FCR_ClosedOpRevenueModel2__c
,FCRM__FCR_ClosedOpRevenueModel3__c
,FCRM__FCR_Converted_Lead__c
,FCRM__FCR_Dated_Opportunity_Amount__c
,FCRM__FCR_FCCRM_Logo__c
,FCRM__FCR_First_Queue_Assigned__c
,FCRM__FCR_Last_Modified_By_Date_Formula__c
,FCRM__FCR_Last_Modified_By_Formula__c
,FCRM__FCR_LostOpRevenueModel1__c
,FCRM__FCR_LostOpRevenueModel2__c
,FCRM__FCR_LostOpRevenueModel3__c
,FCRM__FCR_Name_Created_Date__c
,FCRM__FCR_Non_Response_Audit__c
,FCRM__FCR_Nurture_Timeout__c
,FCRM__FCR_OpenOpRevenueModel1__c
,FCRM__FCR_OpenOpRevenueModel2__c
,FCRM__FCR_OpenOpRevenueModel3__c
,FCRM__FCR_Opportunity_Cleared__c
,FCRM__FCR_Opportunity_Closed__c
,FCRM__FCR_Opportunity_Closed_Won__c
,FCRM__FCR_Opportunity_Count__c
,FCRM__FCR_Opportunity_Create_Date__c
,FCRM__FCR_Opportunity_Created_by__c
,FCRM__FCR_Opportunity_Response_Error__c
,FCRM__FCR_Opportunity_Value_Lost__c
,FCRM__FCR_Opportunity_Value_Won__c
,FCRM__FCR_Original_Campaign__c
,FCRM__FCR_Precedence_Campaign__c
,FCRM__FCR_Precedence_Replaced_Date__c
,FCRM__FCR_Precedence_Response__c
,FCRM__FCR_Precedence_Response_Link__c
,FCRM__FCR_Reactivation_Date__c
,FCRM__FCR_Repeat_Count__c
,FCRM__FCR_Replaced_Campaign__c
,FCRM__FCR_Replaced_Response__c
,FCRM__FCR_Replaced_Response_Link__c
,FCRM__FCR_Response_Engagement_Level__c
,FCRM__FCR_Revenue_Timestamp__c
,FCRM__FCR_Status_Last_Set__c
,FCRM__FCR_Superpower_Field__c
,FCRM__FCR_TotalOpRevenueModel1__c
,FCRM__FCR_TotalOpRevenueModel2__c
,FCRM__FCR_TotalOpRevenueModel3__c
,FCRM__FCR_TQR__c
,FCRM__Opportunity_Value_Open__c
,Source_Campaign__c
,CFCR_Authorized_Buyer__c
,CFCR_Business_Area__c
,CFCR_Lead_Qualified_Date__c
,CFCR_Lead_Qualifier__c
,CFCR_Lead_Qualifier_Role_Type__c
,CFCR_Projected_Budget_Amount__c
,CFCR_Projected_Budget__c
,CFCR_Purchaser_Role__c
,CFCR_Timeframe_to_Buy__c
,CFCR_Original_Lead_ID__c
,SWT_Partner_Lead__c
,LD_DT
,SWT_INS_DT
,d_source
)
 select 
 Id
,IsDeleted
,CampaignId
,LeadId
,ContactId
,Status
,HasResponded
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,FirstRespondedDate
,CurrencyIsoCode
,Salutation
,Name
,FirstName
,LastName
,Title
,Street
,City
,State
,PostalCode
,Country
,Email
,Phone
,Fax
,MobilePhone
,Description
,DoNotCall
,HasOptedOutOfEmail
,HasOptedOutOfFax
,LeadSource
,CompanyOrAccount
,Type
,LeadOrContactId
,LeadOrContactOwnerId
,SWT_Primary__c
,SWT_Activity__c
,SWT_Source_Business_Unit__c
,SWT_Source_Country__c
,SWT_Source_Email__c
,SWT_Source_First_Name__c
,SWT_Source_Last_Name__c
,SWT_CampaignMemberExternal_Id__c
,SWT_Is_Primary__c
,SWT_Campaign_Member_External_ID__c
,et4ae5__Activity__c
,CFCR_Account__c
,CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_AQL_Owner__c
,CFCR_AQL_Owner_Role__c
,CFCR_AQL_to_MQL__c
,CFCR_AQL_to_SAL__c
,CFCR_Business_Unit__c
,CFCR_Current_Stage_in_Funnel__c
,CFCR_Lost_Reason__c
,CFCR_MQL_Owner__c
,CFCR_MQL_Owner_Role__c
,CFCR_MQL_to_Won__c
,CFCR_Opportunity_18_Digit_ID__c
,CFCR_Opportunity_Stage__c
,CFCR_Opportunity_Type__c
,CFCR_pi_utm_campaign__c
,CFCR_pi_utm_content__c
,CFCR_pi_utm_medium__c
,CFCR_pi_utm_source__c
,CFCR_pi_utm_term__c
,CFCR_Response_to_AQL__c
,CFCR_SAL_Owner_Role__c
,CFCR_SQL_Owner__c
,CFCR_SQL_Owner_Role__c
,CFCR_SQL_to_SQO__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CFCR_SQO_Owner__c
,CFCR_SQO_Owner_Role__c
,CFCR_SQO_to_Won__c
,CFCR_Status_Reason__c
,CFCR_Still_in_AQL__c
,CFCR_Still_in_Inquiry__c
,CFCR_Still_in_MQL__c
,CFCR_Still_in_SAL__c
,CFCR_Still_in_SQL__c
,CFCR_Still_in_SQO__c
,CFCR_Won_Owner__c
,CFCR_Won_Owner_Role__c
,CFCR_SRL__c
,CFCR_SRL_Date__c
,FCRM__FCR_18_Character_ID__c
,FCRM__FCR_Campaign_Repeat_Parent__c
,FCRM__FCR_Current_Lead_Contact_Status__c
,FCRM__FCR_First_Owner_Assigned__c
,FCRM__FCR_First_Owner_Type__c
,FCRM__FCR_First_Owner_Worked__c
,FCRM__FCR_Inquiry_Target__c
,FCRM__FCR_Inquiry_Target_Date__c
,FCRM__FCR_Inquiry_to_MQR__c
,FCRM__FCR_Lead_Contact_18_Digit_ID__c
,FCRM__FCR_Member_Type_On_Create__c
,FCRM__FCR_MQR_to_SAR__c
,FCRM__FCR_Net_New_Lead__c
,FCRM__FCR_Opportunity__c
,FCRM__FCR_Opportunity_Amount__c
,FCRM__FCR_Opportunity_Closed_Date__c
,FCRM__FCR_QR__c
,FCRM__FCR_QR_Date__c
,FCRM__FCR_Response_Date__c
,FCRM__FCR_Response_Score__c
,FCRM__FCR_Response_Status__c
,FCRM__FCR_SAR__c
,FCRM__FCR_SAR_Date__c
,FCRM__FCR_SAR_Owner__c
,FCRM__FCR_SAR_to_SQR__c
,FCRM__FCR_SQR__c
,FCRM__FCR_SQR_Date__c
,FCRM__FCR_SQR_to_Closed_Won__c
,FCRM__FCR_SQR_Won__c
,FCRM__FCR_Status_Age__c
,FCRM__FCR_View_Response__c
,CFCR_Actionable__c
,CFCR_Admin_Defective_Member__c
,CFCR_Annual_Revenue__c
,CFCR_BANT_Criteria_Completed__c
,CFCR_Count__c
,CFCR_Created_By_Formula__c
,CFCR_Current_Stage_In_Funnel_Date__c
,CFCR_FCCRM_Threshold_Current__c
,CFCR_FCCRM_Threshold_On_Create__c
,CFCR_Industry_Formula__c
,CFCR_Lead_Contact_Create_Date_Time__c
,CFCR_Lead_Source_Formula__c
,CFCR_Manual_Adjustment__c
,CFCR_Manual_Adjustment_Notes__c
,CFCR_No_of_Employees__c
,CFCR_pi_campaign__c
,CFCR_pi_grade__c
,CFCR_pi_grade_On_Update__c
,CFCR_pi_last_activity__c
,CFCR_pi_score__c
,CFCR_pi_score_On_Update__c
,CFCR_pi_url__c
,CFCR_Region__c
,CFCR_TEM_Trial__c
,FCRM__FCR_Admin_Opportunity_Status__c
,FCRM__FCR_Admin_Response_Control__c
,FCRM__FCR_Admin_Response_Day__c
,FCRM__FCR_Admin_SyncPending__c
,FCRM__FCR_Admin_SyncTest__c
,FCRM__FCR_Admin_Update_Counter__c
,FCRM__FCR_CascadeID__c
,FCRM__FCR_ClosedOpRevenueModel1__c
,FCRM__FCR_ClosedOpRevenueModel2__c
,FCRM__FCR_ClosedOpRevenueModel3__c
,FCRM__FCR_Converted_Lead__c
,FCRM__FCR_Dated_Opportunity_Amount__c
,FCRM__FCR_FCCRM_Logo__c
,FCRM__FCR_First_Queue_Assigned__c
,FCRM__FCR_Last_Modified_By_Date_Formula__c
,FCRM__FCR_Last_Modified_By_Formula__c
,FCRM__FCR_LostOpRevenueModel1__c
,FCRM__FCR_LostOpRevenueModel2__c
,FCRM__FCR_LostOpRevenueModel3__c
,FCRM__FCR_Name_Created_Date__c
,FCRM__FCR_Non_Response_Audit__c
,FCRM__FCR_Nurture_Timeout__c
,FCRM__FCR_OpenOpRevenueModel1__c
,FCRM__FCR_OpenOpRevenueModel2__c
,FCRM__FCR_OpenOpRevenueModel3__c
,FCRM__FCR_Opportunity_Cleared__c
,FCRM__FCR_Opportunity_Closed__c
,FCRM__FCR_Opportunity_Closed_Won__c
,FCRM__FCR_Opportunity_Count__c
,FCRM__FCR_Opportunity_Create_Date__c
,FCRM__FCR_Opportunity_Created_by__c
,FCRM__FCR_Opportunity_Response_Error__c
,FCRM__FCR_Opportunity_Value_Lost__c
,FCRM__FCR_Opportunity_Value_Won__c
,FCRM__FCR_Original_Campaign__c
,FCRM__FCR_Precedence_Campaign__c
,FCRM__FCR_Precedence_Replaced_Date__c
,FCRM__FCR_Precedence_Response__c
,FCRM__FCR_Precedence_Response_Link__c
,FCRM__FCR_Reactivation_Date__c
,FCRM__FCR_Repeat_Count__c
,FCRM__FCR_Replaced_Campaign__c
,FCRM__FCR_Replaced_Response__c
,FCRM__FCR_Replaced_Response_Link__c
,FCRM__FCR_Response_Engagement_Level__c
,FCRM__FCR_Revenue_Timestamp__c
,FCRM__FCR_Status_Last_Set__c
,FCRM__FCR_Superpower_Field__c
,FCRM__FCR_TotalOpRevenueModel1__c
,FCRM__FCR_TotalOpRevenueModel2__c
,FCRM__FCR_TotalOpRevenueModel3__c
,FCRM__FCR_TQR__c
,FCRM__Opportunity_Value_Open__c
,Source_Campaign__c
,CFCR_Authorized_Buyer__c
,CFCR_Business_Area__c
,CFCR_Lead_Qualified_Date__c
,CFCR_Lead_Qualifier__c
,CFCR_Lead_Qualifier_Role_Type__c
,CFCR_Projected_Budget_Amount__c
,CFCR_Projected_Budget__c
,CFCR_Purchaser_Role__c
,CFCR_Timeframe_to_Buy__c
,CFCR_Original_Lead_ID__c
,SWT_Partner_Lead__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_CampaignMember WHERE id in
(SELECT STG.id FROM SF_CampaignMember_stg_Tmp_Key STG JOIN SF_CampaignMember_base_Tmp
ON STG.id = SF_CampaignMember_base_Tmp.id AND STG.LastModifiedDate >= SF_CampaignMember_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_CampaignMember_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/


/* Incremental VSQL script for loading data from Stage to Base */  

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_CampaignMember WHERE id in
(SELECT STG.id FROM SF_CampaignMember_stg_Tmp_Key STG JOIN SF_CampaignMember_base_Tmp
ON STG.id = SF_CampaignMember_base_Tmp.id AND STG.LastModifiedDate >= SF_CampaignMember_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_CampaignMember
(
 Id
,IsDeleted
,CampaignId
,LeadId
,ContactId
,Status
,HasResponded
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,FirstRespondedDate
,CurrencyIsoCode
,Salutation
,Name
,FirstName
,LastName
,Title
,Street
,City
,State
,PostalCode
,Country
,Email
,Phone
,Fax
,MobilePhone
,Description
,DoNotCall
,HasOptedOutOfEmail
,HasOptedOutOfFax
,LeadSource
,CompanyOrAccount
,Type
,LeadOrContactId
,LeadOrContactOwnerId
,SWT_Primary__c
,SWT_Activity__c
,SWT_Source_Business_Unit__c
,SWT_Source_Country__c
,SWT_Source_Email__c
,SWT_Source_First_Name__c
,SWT_Source_Last_Name__c
,SWT_CampaignMemberExternal_Id__c
,SWT_Is_Primary__c
,SWT_Campaign_Member_External_ID__c
,et4ae5__Activity__c
,CFCR_Account__c
,CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_AQL_Owner__c
,CFCR_AQL_Owner_Role__c
,CFCR_AQL_to_MQL__c
,CFCR_AQL_to_SAL__c
,CFCR_Business_Unit__c
,CFCR_Current_Stage_in_Funnel__c
,CFCR_Lost_Reason__c
,CFCR_MQL_Owner__c
,CFCR_MQL_Owner_Role__c
,CFCR_MQL_to_Won__c
,CFCR_Opportunity_18_Digit_ID__c
,CFCR_Opportunity_Stage__c
,CFCR_Opportunity_Type__c
,CFCR_pi_utm_campaign__c
,CFCR_pi_utm_content__c
,CFCR_pi_utm_medium__c
,CFCR_pi_utm_source__c
,CFCR_pi_utm_term__c
,CFCR_Response_to_AQL__c
,CFCR_SAL_Owner_Role__c
,CFCR_SQL_Owner__c
,CFCR_SQL_Owner_Role__c
,CFCR_SQL_to_SQO__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CFCR_SQO_Owner__c
,CFCR_SQO_Owner_Role__c
,CFCR_SQO_to_Won__c
,CFCR_Status_Reason__c
,CFCR_Still_in_AQL__c
,CFCR_Still_in_Inquiry__c
,CFCR_Still_in_MQL__c
,CFCR_Still_in_SAL__c
,CFCR_Still_in_SQL__c
,CFCR_Still_in_SQO__c
,CFCR_Won_Owner__c
,CFCR_Won_Owner_Role__c
,CFCR_SRL__c
,CFCR_SRL_Date__c
,FCRM__FCR_18_Character_ID__c
,FCRM__FCR_Campaign_Repeat_Parent__c
,FCRM__FCR_Current_Lead_Contact_Status__c
,FCRM__FCR_First_Owner_Assigned__c
,FCRM__FCR_First_Owner_Type__c
,FCRM__FCR_First_Owner_Worked__c
,FCRM__FCR_Inquiry_Target__c
,FCRM__FCR_Inquiry_Target_Date__c
,FCRM__FCR_Inquiry_to_MQR__c
,FCRM__FCR_Lead_Contact_18_Digit_ID__c
,FCRM__FCR_Member_Type_On_Create__c
,FCRM__FCR_MQR_to_SAR__c
,FCRM__FCR_Net_New_Lead__c
,FCRM__FCR_Opportunity__c
,FCRM__FCR_Opportunity_Amount__c
,FCRM__FCR_Opportunity_Closed_Date__c
,FCRM__FCR_QR__c
,FCRM__FCR_QR_Date__c
,FCRM__FCR_Response_Date__c
,FCRM__FCR_Response_Score__c
,FCRM__FCR_Response_Status__c
,FCRM__FCR_SAR__c
,FCRM__FCR_SAR_Date__c
,FCRM__FCR_SAR_Owner__c
,FCRM__FCR_SAR_to_SQR__c
,FCRM__FCR_SQR__c
,FCRM__FCR_SQR_Date__c
,FCRM__FCR_SQR_to_Closed_Won__c
,FCRM__FCR_SQR_Won__c
,FCRM__FCR_Status_Age__c
,FCRM__FCR_View_Response__c
,CFCR_Actionable__c
,CFCR_Admin_Defective_Member__c
,CFCR_Annual_Revenue__c
,CFCR_BANT_Criteria_Completed__c
,CFCR_Count__c
,CFCR_Created_By_Formula__c
,CFCR_Current_Stage_In_Funnel_Date__c
,CFCR_FCCRM_Threshold_Current__c
,CFCR_FCCRM_Threshold_On_Create__c
,CFCR_Industry_Formula__c
,CFCR_Lead_Contact_Create_Date_Time__c
,CFCR_Lead_Source_Formula__c
,CFCR_Manual_Adjustment__c
,CFCR_Manual_Adjustment_Notes__c
,CFCR_No_of_Employees__c
,CFCR_pi_campaign__c
,CFCR_pi_grade__c
,CFCR_pi_grade_On_Update__c
,CFCR_pi_last_activity__c
,CFCR_pi_score__c
,CFCR_pi_score_On_Update__c
,CFCR_pi_url__c
,CFCR_Region__c
,CFCR_TEM_Trial__c
,FCRM__FCR_Admin_Opportunity_Status__c
,FCRM__FCR_Admin_Response_Control__c
,FCRM__FCR_Admin_Response_Day__c
,FCRM__FCR_Admin_SyncPending__c
,FCRM__FCR_Admin_SyncTest__c
,FCRM__FCR_Admin_Update_Counter__c
,FCRM__FCR_CascadeID__c
,FCRM__FCR_ClosedOpRevenueModel1__c
,FCRM__FCR_ClosedOpRevenueModel2__c
,FCRM__FCR_ClosedOpRevenueModel3__c
,FCRM__FCR_Converted_Lead__c
,FCRM__FCR_Dated_Opportunity_Amount__c
,FCRM__FCR_FCCRM_Logo__c
,FCRM__FCR_First_Queue_Assigned__c
,FCRM__FCR_Last_Modified_By_Date_Formula__c
,FCRM__FCR_Last_Modified_By_Formula__c
,FCRM__FCR_LostOpRevenueModel1__c
,FCRM__FCR_LostOpRevenueModel2__c
,FCRM__FCR_LostOpRevenueModel3__c
,FCRM__FCR_Name_Created_Date__c
,FCRM__FCR_Non_Response_Audit__c
,FCRM__FCR_Nurture_Timeout__c
,FCRM__FCR_OpenOpRevenueModel1__c
,FCRM__FCR_OpenOpRevenueModel2__c
,FCRM__FCR_OpenOpRevenueModel3__c
,FCRM__FCR_Opportunity_Cleared__c
,FCRM__FCR_Opportunity_Closed__c
,FCRM__FCR_Opportunity_Closed_Won__c
,FCRM__FCR_Opportunity_Count__c
,FCRM__FCR_Opportunity_Create_Date__c
,FCRM__FCR_Opportunity_Created_by__c
,FCRM__FCR_Opportunity_Response_Error__c
,FCRM__FCR_Opportunity_Value_Lost__c
,FCRM__FCR_Opportunity_Value_Won__c
,FCRM__FCR_Original_Campaign__c
,FCRM__FCR_Precedence_Campaign__c
,FCRM__FCR_Precedence_Replaced_Date__c
,FCRM__FCR_Precedence_Response__c
,FCRM__FCR_Precedence_Response_Link__c
,FCRM__FCR_Reactivation_Date__c
,FCRM__FCR_Repeat_Count__c
,FCRM__FCR_Replaced_Campaign__c
,FCRM__FCR_Replaced_Response__c
,FCRM__FCR_Replaced_Response_Link__c
,FCRM__FCR_Response_Engagement_Level__c
,FCRM__FCR_Revenue_Timestamp__c
,FCRM__FCR_Status_Last_Set__c
,FCRM__FCR_Superpower_Field__c
,FCRM__FCR_TotalOpRevenueModel1__c
,FCRM__FCR_TotalOpRevenueModel2__c
,FCRM__FCR_TotalOpRevenueModel3__c
,FCRM__FCR_TQR__c
,FCRM__Opportunity_Value_Open__c
,Source_Campaign__c
,CFCR_Authorized_Buyer__c
,CFCR_Business_Area__c
,CFCR_Lead_Qualified_Date__c
,CFCR_Lead_Qualifier__c
,CFCR_Lead_Qualifier_Role_Type__c
,CFCR_Projected_Budget_Amount__c
,CFCR_Projected_Budget__c
,CFCR_Purchaser_Role__c
,CFCR_Timeframe_to_Buy__c
,CFCR_Original_Lead_ID__c
,SWT_Partner_Lead__c
,SWT_INS_DT
)
SELECT DISTINCT 
SF_CampaignMember_stg_Tmp.Id
,IsDeleted
,CampaignId
,LeadId
,ContactId
,Status
,HasResponded
,CreatedDate
,CreatedById
,SF_CampaignMember_stg_Tmp.LastModifiedDate
,LastModifiedById
,SystemModstamp
,FirstRespondedDate
,CurrencyIsoCode
,Salutation
,Name
,FirstName
,LastName
,Title
,Street
,City
,State
,PostalCode
,Country
,Email
,Phone
,Fax
,MobilePhone
,Description
,DoNotCall
,HasOptedOutOfEmail
,HasOptedOutOfFax
,LeadSource
,CompanyOrAccount
,Type
,LeadOrContactId
,LeadOrContactOwnerId
,SWT_Primary__c
,SWT_Activity__c
,SWT_Source_Business_Unit__c
,SWT_Source_Country__c
,SWT_Source_Email__c
,SWT_Source_First_Name__c
,SWT_Source_Last_Name__c
,SWT_CampaignMemberExternal_Id__c
,SWT_Is_Primary__c
,SWT_Campaign_Member_External_ID__c
,et4ae5__Activity__c
,CFCR_Account__c
,CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_AQL_Owner__c
,CFCR_AQL_Owner_Role__c
,CFCR_AQL_to_MQL__c
,CFCR_AQL_to_SAL__c
,CFCR_Business_Unit__c
,CFCR_Current_Stage_in_Funnel__c
,CFCR_Lost_Reason__c
,CFCR_MQL_Owner__c
,CFCR_MQL_Owner_Role__c
,CFCR_MQL_to_Won__c
,CFCR_Opportunity_18_Digit_ID__c
,CFCR_Opportunity_Stage__c
,CFCR_Opportunity_Type__c
,CFCR_pi_utm_campaign__c
,CFCR_pi_utm_content__c
,CFCR_pi_utm_medium__c
,CFCR_pi_utm_source__c
,CFCR_pi_utm_term__c
,CFCR_Response_to_AQL__c
,CFCR_SAL_Owner_Role__c
,CFCR_SQL_Owner__c
,CFCR_SQL_Owner_Role__c
,CFCR_SQL_to_SQO__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CFCR_SQO_Owner__c
,CFCR_SQO_Owner_Role__c
,CFCR_SQO_to_Won__c
,CFCR_Status_Reason__c
,CFCR_Still_in_AQL__c
,CFCR_Still_in_Inquiry__c
,CFCR_Still_in_MQL__c
,CFCR_Still_in_SAL__c
,CFCR_Still_in_SQL__c
,CFCR_Still_in_SQO__c
,CFCR_Won_Owner__c
,CFCR_Won_Owner_Role__c
,CFCR_SRL__c
,CFCR_SRL_Date__c
,FCRM__FCR_18_Character_ID__c
,FCRM__FCR_Campaign_Repeat_Parent__c
,FCRM__FCR_Current_Lead_Contact_Status__c
,FCRM__FCR_First_Owner_Assigned__c
,FCRM__FCR_First_Owner_Type__c
,FCRM__FCR_First_Owner_Worked__c
,FCRM__FCR_Inquiry_Target__c
,FCRM__FCR_Inquiry_Target_Date__c
,FCRM__FCR_Inquiry_to_MQR__c
,FCRM__FCR_Lead_Contact_18_Digit_ID__c
,FCRM__FCR_Member_Type_On_Create__c
,FCRM__FCR_MQR_to_SAR__c
,FCRM__FCR_Net_New_Lead__c
,FCRM__FCR_Opportunity__c
,FCRM__FCR_Opportunity_Amount__c
,FCRM__FCR_Opportunity_Closed_Date__c
,FCRM__FCR_QR__c
,FCRM__FCR_QR_Date__c
,FCRM__FCR_Response_Date__c
,FCRM__FCR_Response_Score__c
,FCRM__FCR_Response_Status__c
,FCRM__FCR_SAR__c
,FCRM__FCR_SAR_Date__c
,FCRM__FCR_SAR_Owner__c
,FCRM__FCR_SAR_to_SQR__c
,FCRM__FCR_SQR__c
,FCRM__FCR_SQR_Date__c
,FCRM__FCR_SQR_to_Closed_Won__c
,FCRM__FCR_SQR_Won__c
,FCRM__FCR_Status_Age__c
,FCRM__FCR_View_Response__c
,CFCR_Actionable__c
,CFCR_Admin_Defective_Member__c
,CFCR_Annual_Revenue__c
,CFCR_BANT_Criteria_Completed__c
,CFCR_Count__c
,CFCR_Created_By_Formula__c
,CFCR_Current_Stage_In_Funnel_Date__c
,CFCR_FCCRM_Threshold_Current__c
,CFCR_FCCRM_Threshold_On_Create__c
,CFCR_Industry_Formula__c
,CFCR_Lead_Contact_Create_Date_Time__c
,CFCR_Lead_Source_Formula__c
,CFCR_Manual_Adjustment__c
,CFCR_Manual_Adjustment_Notes__c
,CFCR_No_of_Employees__c
,CFCR_pi_campaign__c
,CFCR_pi_grade__c
,CFCR_pi_grade_On_Update__c
,CFCR_pi_last_activity__c
,CFCR_pi_score__c
,CFCR_pi_score_On_Update__c
,CFCR_pi_url__c
,CFCR_Region__c
,CFCR_TEM_Trial__c
,FCRM__FCR_Admin_Opportunity_Status__c
,FCRM__FCR_Admin_Response_Control__c
,FCRM__FCR_Admin_Response_Day__c
,FCRM__FCR_Admin_SyncPending__c
,FCRM__FCR_Admin_SyncTest__c
,FCRM__FCR_Admin_Update_Counter__c
,FCRM__FCR_CascadeID__c
,FCRM__FCR_ClosedOpRevenueModel1__c
,FCRM__FCR_ClosedOpRevenueModel2__c
,FCRM__FCR_ClosedOpRevenueModel3__c
,FCRM__FCR_Converted_Lead__c
,FCRM__FCR_Dated_Opportunity_Amount__c
,FCRM__FCR_FCCRM_Logo__c
,FCRM__FCR_First_Queue_Assigned__c
,FCRM__FCR_Last_Modified_By_Date_Formula__c
,FCRM__FCR_Last_Modified_By_Formula__c
,FCRM__FCR_LostOpRevenueModel1__c
,FCRM__FCR_LostOpRevenueModel2__c
,FCRM__FCR_LostOpRevenueModel3__c
,FCRM__FCR_Name_Created_Date__c
,FCRM__FCR_Non_Response_Audit__c
,FCRM__FCR_Nurture_Timeout__c
,FCRM__FCR_OpenOpRevenueModel1__c
,FCRM__FCR_OpenOpRevenueModel2__c
,FCRM__FCR_OpenOpRevenueModel3__c
,FCRM__FCR_Opportunity_Cleared__c
,FCRM__FCR_Opportunity_Closed__c
,FCRM__FCR_Opportunity_Closed_Won__c
,FCRM__FCR_Opportunity_Count__c
,FCRM__FCR_Opportunity_Create_Date__c
,FCRM__FCR_Opportunity_Created_by__c
,FCRM__FCR_Opportunity_Response_Error__c
,FCRM__FCR_Opportunity_Value_Lost__c
,FCRM__FCR_Opportunity_Value_Won__c
,FCRM__FCR_Original_Campaign__c
,FCRM__FCR_Precedence_Campaign__c
,FCRM__FCR_Precedence_Replaced_Date__c
,FCRM__FCR_Precedence_Response__c
,FCRM__FCR_Precedence_Response_Link__c
,FCRM__FCR_Reactivation_Date__c
,FCRM__FCR_Repeat_Count__c
,FCRM__FCR_Replaced_Campaign__c
,FCRM__FCR_Replaced_Response__c
,FCRM__FCR_Replaced_Response_Link__c
,FCRM__FCR_Response_Engagement_Level__c
,FCRM__FCR_Revenue_Timestamp__c
,FCRM__FCR_Status_Last_Set__c
,FCRM__FCR_Superpower_Field__c
,FCRM__FCR_TotalOpRevenueModel1__c
,FCRM__FCR_TotalOpRevenueModel2__c
,FCRM__FCR_TotalOpRevenueModel3__c
,FCRM__FCR_TQR__c
,FCRM__Opportunity_Value_Open__c
,Source_Campaign__c
,CFCR_Authorized_Buyer__c
,CFCR_Business_Area__c
,CFCR_Lead_Qualified_Date__c
,CFCR_Lead_Qualifier__c
,CFCR_Lead_Qualifier_Role_Type__c
,CFCR_Projected_Budget_Amount__c
,CFCR_Projected_Budget__c
,CFCR_Purchaser_Role__c
,CFCR_Timeframe_to_Buy__c
,CFCR_Original_Lead_ID__c
,SWT_Partner_Lead__c
,SYSDATE AS SWT_INS_DT
FROM SF_CampaignMember_stg_Tmp JOIN SF_CampaignMember_stg_Tmp_Key ON SF_CampaignMember_stg_Tmp.id=SF_CampaignMember_stg_Tmp_Key.id AND SF_CampaignMember_stg_Tmp.LastModifiedDate=SF_CampaignMember_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_CampaignMember BASE
WHERE SF_CampaignMember_stg_Tmp.id = BASE.id);
		
		
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
select 'SFDC','SF_CampaignMember',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_CampaignMember where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_CampaignMember' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_CampaignMember' and  COMPLTN_STAT = 'N');

commit;

SELECT DROP_PARTITION('swt_rpt_stg.SF_CampaignMember_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_CampaignMember_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_CampaignMember');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_CampaignMember');


