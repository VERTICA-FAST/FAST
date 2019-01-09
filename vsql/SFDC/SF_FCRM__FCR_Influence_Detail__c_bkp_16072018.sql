
/****
****Script Name   : SF_FCRM__FCR_Influence_Detail__c.sql
****Description   : Incremental data load for SF_FCRM__FCR_Influence_Detail__c
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_FCRM__FCR_Influence_Detail__c";

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
select 'SFDC','SF_FCRM__FCR_Influence_Detail__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c_Hist SELECT * from swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c where id in (
select id from swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);


delete from swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c.id=t2.id and swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE SF_FCRM__FCR_Influence_Detail__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c;

CREATE LOCAL TEMP TABLE SF_FCRM__FCR_Influence_Detail__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT Id,LastModifiedDate FROM swt_rpt_base.SF_FCRM__FCR_Influence_Detail__c)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT Id, max(LastModifiedDate) as LastModifiedDate FROM SF_FCRM__FCR_Influence_Detail__c_stg_Tmp group by Id)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c_Hist
(
CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_Days_Before_Opportunity_Close__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CreatedById
,CreatedDate
,CurrencyIsoCode
,FCRM__Account__c
,FCRM__Admin_Update_Counter__c
,FCRM__Campaign__c
,FCRM__Cascade_Parent_Campaign__c
,FCRM__ClosedOpRevenueModel1__c
,FCRM__ClosedOpRevenueModel2__c
,FCRM__ClosedOpRevenueModel3__c
,FCRM__Contact__c
,FCRM__Count__c
,FCRM__Days_Before_Opportunity_Create__c
,FCRM__HasInfluenceModel1__c
,FCRM__HasInfluenceModel2__c
,FCRM__HasInfluenceModel3__c
,FCRM__Inquiry_Target__c
,FCRM__Inquiry_Target_Date__c
,FCRM__Lead__c
,FCRM__LostOpRevenueModel1__c
,FCRM__LostOpRevenueModel2__c
,FCRM__LostOpRevenueModel3__c
,FCRM__Member_Status__c
,FCRM__Member_Type_On_Create__c
,FCRM__Name_Created_Date__c
,FCRM__Net_New_Name__c
,FCRM__Next_Opportunity_Stage__c
,FCRM__OpenOpRevenueModel1__c
,FCRM__OpenOpRevenueModel2__c
,FCRM__OpenOpRevenueModel3__c
,FCRM__Opportunity__c
,FCRM__Opportunity_Stage__c
,FCRM__Opportunity_Stage_Date__c
,FCRM__Opportunity_Stage_Progression_Date__c
,FCRM__Opportunity_Timeframe__c
,FCRM__QR__c
,FCRM__QR_Date__c
,FCRM__Response_Date__c
,FCRM__Response_ID__c
,FCRM__Response_Status__c
,FCRM__SAR__c
,FCRM__SAR_Date__c
,FCRM__SQR__c
,FCRM__SQR_Date__c
,FCRM__SQR_Won__c
,FCRM__Test_Record__c
,FCRM__Tipping_Point_Response__c
,FCRM__TotalOpRevenueModel1__c
,FCRM__TotalOpRevenueModel2__c
,FCRM__TotalOpRevenueModel3__c
,FCRM__UniqueID__c
,FCRM__Unix_Op_Close_Time__c
,FCRM__Unix_Op_Create_Time__c
,Id
,IsDeleted
,LastModifiedById
,LastModifiedDate
,Name
,OwnerId
,SystemModstamp
,CFCR_Days_After_Opportunity_Create__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_Days_Before_Opportunity_Close__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CreatedById
,CreatedDate
,CurrencyIsoCode
,FCRM__Account__c
,FCRM__Admin_Update_Counter__c
,FCRM__Campaign__c
,FCRM__Cascade_Parent_Campaign__c
,FCRM__ClosedOpRevenueModel1__c
,FCRM__ClosedOpRevenueModel2__c
,FCRM__ClosedOpRevenueModel3__c
,FCRM__Contact__c
,FCRM__Count__c
,FCRM__Days_Before_Opportunity_Create__c
,FCRM__HasInfluenceModel1__c
,FCRM__HasInfluenceModel2__c
,FCRM__HasInfluenceModel3__c
,FCRM__Inquiry_Target__c
,FCRM__Inquiry_Target_Date__c
,FCRM__Lead__c
,FCRM__LostOpRevenueModel1__c
,FCRM__LostOpRevenueModel2__c
,FCRM__LostOpRevenueModel3__c
,FCRM__Member_Status__c
,FCRM__Member_Type_On_Create__c
,FCRM__Name_Created_Date__c
,FCRM__Net_New_Name__c
,FCRM__Next_Opportunity_Stage__c
,FCRM__OpenOpRevenueModel1__c
,FCRM__OpenOpRevenueModel2__c
,FCRM__OpenOpRevenueModel3__c
,FCRM__Opportunity__c
,FCRM__Opportunity_Stage__c
,FCRM__Opportunity_Stage_Date__c
,FCRM__Opportunity_Stage_Progression_Date__c
,FCRM__Opportunity_Timeframe__c
,FCRM__QR__c
,FCRM__QR_Date__c
,FCRM__Response_Date__c
,FCRM__Response_ID__c
,FCRM__Response_Status__c
,FCRM__SAR__c
,FCRM__SAR_Date__c
,FCRM__SQR__c
,FCRM__SQR_Date__c
,FCRM__SQR_Won__c
,FCRM__Test_Record__c
,FCRM__Tipping_Point_Response__c
,FCRM__TotalOpRevenueModel1__c
,FCRM__TotalOpRevenueModel2__c
,FCRM__TotalOpRevenueModel3__c
,FCRM__UniqueID__c
,FCRM__Unix_Op_Close_Time__c
,FCRM__Unix_Op_Create_Time__c
,Id
,IsDeleted
,LastModifiedById
,LastModifiedDate
,Name
,OwnerId
,SystemModstamp
,CFCR_Days_After_Opportunity_Create__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_FCRM__FCR_Influence_Detail__c WHERE id in
(SELECT STG.id FROM SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key STG JOIN SF_FCRM__FCR_Influence_Detail__c_base_Tmp
ON STG.id = SF_FCRM__FCR_Influence_Detail__c_base_Tmp.id AND STG.LastModifiedDate >= SF_FCRM__FCR_Influence_Detail__c_base_Tmp.LastModifiedDate);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."SF_FCRM__FCR_Influence_Detail__c_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;





/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_FCRM__FCR_Influence_Detail__c WHERE id in
(SELECT STG.id FROM SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key STG JOIN SF_FCRM__FCR_Influence_Detail__c_base_Tmp
ON STG.id = SF_FCRM__FCR_Influence_Detail__c_base_Tmp.id AND STG.LastModifiedDate >= SF_FCRM__FCR_Influence_Detail__c_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_FCRM__FCR_Influence_Detail__c
(
CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_Days_Before_Opportunity_Close__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CreatedById
,CreatedDate
,CurrencyIsoCode
,FCRM__Account__c
,FCRM__Admin_Update_Counter__c
,FCRM__Campaign__c
,FCRM__Cascade_Parent_Campaign__c
,FCRM__ClosedOpRevenueModel1__c
,FCRM__ClosedOpRevenueModel2__c
,FCRM__ClosedOpRevenueModel3__c
,FCRM__Contact__c
,FCRM__Count__c
,FCRM__Days_Before_Opportunity_Create__c
,FCRM__HasInfluenceModel1__c
,FCRM__HasInfluenceModel2__c
,FCRM__HasInfluenceModel3__c
,FCRM__Inquiry_Target__c
,FCRM__Inquiry_Target_Date__c
,FCRM__Lead__c
,FCRM__LostOpRevenueModel1__c
,FCRM__LostOpRevenueModel2__c
,FCRM__LostOpRevenueModel3__c
,FCRM__Member_Status__c
,FCRM__Member_Type_On_Create__c
,FCRM__Name_Created_Date__c
,FCRM__Net_New_Name__c
,FCRM__Next_Opportunity_Stage__c
,FCRM__OpenOpRevenueModel1__c
,FCRM__OpenOpRevenueModel2__c
,FCRM__OpenOpRevenueModel3__c
,FCRM__Opportunity__c
,FCRM__Opportunity_Stage__c
,FCRM__Opportunity_Stage_Date__c
,FCRM__Opportunity_Stage_Progression_Date__c
,FCRM__Opportunity_Timeframe__c
,FCRM__QR__c
,FCRM__QR_Date__c
,FCRM__Response_Date__c
,FCRM__Response_ID__c
,FCRM__Response_Status__c
,FCRM__SAR__c
,FCRM__SAR_Date__c
,FCRM__SQR__c
,FCRM__SQR_Date__c
,FCRM__SQR_Won__c
,FCRM__Test_Record__c
,FCRM__Tipping_Point_Response__c
,FCRM__TotalOpRevenueModel1__c
,FCRM__TotalOpRevenueModel2__c
,FCRM__TotalOpRevenueModel3__c
,FCRM__UniqueID__c
,FCRM__Unix_Op_Close_Time__c
,FCRM__Unix_Op_Create_Time__c
,Id
,IsDeleted
,LastModifiedById
,LastModifiedDate
,Name
,OwnerId
,SystemModstamp
,CFCR_Days_After_Opportunity_Create__c
,SWT_INS_DT
)
SELECT DISTINCT 
CFCR_AQL__c
,CFCR_AQL_Date__c
,CFCR_Days_Before_Opportunity_Close__c
,CFCR_SQO__c
,CFCR_SQO_Date__c
,CreatedById
,CreatedDate
,CurrencyIsoCode
,FCRM__Account__c
,FCRM__Admin_Update_Counter__c
,FCRM__Campaign__c
,FCRM__Cascade_Parent_Campaign__c
,FCRM__ClosedOpRevenueModel1__c
,FCRM__ClosedOpRevenueModel2__c
,FCRM__ClosedOpRevenueModel3__c
,FCRM__Contact__c
,FCRM__Count__c
,FCRM__Days_Before_Opportunity_Create__c
,FCRM__HasInfluenceModel1__c
,FCRM__HasInfluenceModel2__c
,FCRM__HasInfluenceModel3__c
,FCRM__Inquiry_Target__c
,FCRM__Inquiry_Target_Date__c
,FCRM__Lead__c
,FCRM__LostOpRevenueModel1__c
,FCRM__LostOpRevenueModel2__c
,FCRM__LostOpRevenueModel3__c
,FCRM__Member_Status__c
,FCRM__Member_Type_On_Create__c
,FCRM__Name_Created_Date__c
,FCRM__Net_New_Name__c
,FCRM__Next_Opportunity_Stage__c
,FCRM__OpenOpRevenueModel1__c
,FCRM__OpenOpRevenueModel2__c
,FCRM__OpenOpRevenueModel3__c
,FCRM__Opportunity__c
,FCRM__Opportunity_Stage__c
,FCRM__Opportunity_Stage_Date__c
,FCRM__Opportunity_Stage_Progression_Date__c
,FCRM__Opportunity_Timeframe__c
,FCRM__QR__c
,FCRM__QR_Date__c
,FCRM__Response_Date__c
,FCRM__Response_ID__c
,FCRM__Response_Status__c
,FCRM__SAR__c
,FCRM__SAR_Date__c
,FCRM__SQR__c
,FCRM__SQR_Date__c
,FCRM__SQR_Won__c
,FCRM__Test_Record__c
,FCRM__Tipping_Point_Response__c
,FCRM__TotalOpRevenueModel1__c
,FCRM__TotalOpRevenueModel2__c
,FCRM__TotalOpRevenueModel3__c
,FCRM__UniqueID__c
,FCRM__Unix_Op_Close_Time__c
,FCRM__Unix_Op_Create_Time__c
,SF_FCRM__FCR_Influence_Detail__c_stg_Tmp.Id
,IsDeleted
,LastModifiedById
,SF_FCRM__FCR_Influence_Detail__c_stg_Tmp.LastModifiedDate
,Name
,OwnerId
,SystemModstamp
,CFCR_Days_After_Opportunity_Create__c
,SYSDATE AS SWT_INS_DT
FROM SF_FCRM__FCR_Influence_Detail__c_stg_Tmp JOIN SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key ON SF_FCRM__FCR_Influence_Detail__c_stg_Tmp.Id= SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key.Id AND SF_FCRM__FCR_Influence_Detail__c_stg_Tmp.LastModifiedDate=SF_FCRM__FCR_Influence_Detail__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".SF_FCRM__FCR_Influence_Detail__c BASE
WHERE SF_FCRM__FCR_Influence_Detail__c_stg_Tmp.Id = BASE.Id);


/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_FCRM__FCR_Influence_Detail__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_FCRM__FCR_Influence_Detail__c' and  COMPLTN_STAT = 'N');


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
select 'SFDC','SF_FCRM__FCR_Influence_Detail__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_FCRM__FCR_Influence_Detail__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

select do_tm_task('mergeout','swt_rpt_stg.SF_FCRM__FCR_Influence_Detail__c_Hist');
select do_tm_task('mergeout','swt_rpt_base.SF_FCRM__FCR_Influence_Detail__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_FCRM__FCR_Influence_Detail__c');


