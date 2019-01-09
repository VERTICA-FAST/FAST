/****
****Script Name   : SF_OpportunityTeamMember.sql
****Description   : Incremental data load for SF_OpportunityTeamMember
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_OpportunityTeamMember";

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
select 'SFDC','SF_OpportunityTeamMember',SYSDATE::date,SYSDATE,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_OpportunityTeamMember_Hist SELECT * from swt_rpt_stg.SF_OpportunityTeamMember;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_OpportunityTeamMember where id in (
select id from swt_rpt_stg.SF_OpportunityTeamMember group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);


delete from swt_rpt_stg.SF_OpportunityTeamMember where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_OpportunityTeamMember.id=t2.id and swt_rpt_stg.SF_OpportunityTeamMember.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE SF_OpportunityTeamMember_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_OpportunityTeamMember)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.SF_OpportunityTeamMember;

CREATE LOCAL TEMP TABLE SF_OpportunityTeamMember_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT Id,LastModifiedDate FROM swt_rpt_base.SF_OpportunityTeamMember)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_OpportunityTeamMember_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_OpportunityTeamMember_stg_Tmp group by id)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_OpportunityTeamMember_Hist
(
Id
,OpportunityId
,UserId
,Name
,PhotoUrl
,Title
,TeamMemberRole
,OpportunityAccessLevel
,CurrencyIsoCode
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,IsDeleted
,SWT_Speciality__c
,SWT_Account_Id__c
,SWT_Account_Link__c
,SWT_Account_Name__c
,SWT_Opportunity_Id__c
,SWT_Opportunity_Link__c
,SWT_Opportunity_Name__c
,SWT_User_Name__c
,SWT_Role__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
Id
,OpportunityId
,UserId
,Name
,PhotoUrl
,Title
,TeamMemberRole
,OpportunityAccessLevel
,CurrencyIsoCode
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,IsDeleted
,SWT_Speciality__c
,SWT_Account_Id__c
,SWT_Account_Link__c
,SWT_Account_Name__c
,SWT_Opportunity_Id__c
,SWT_Opportunity_Link__c
,SWT_Opportunity_Name__c
,SWT_User_Name__c
,SWT_Role__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_OpportunityTeamMember WHERE id in
(SELECT STG.id FROM SF_OpportunityTeamMember_stg_Tmp_Key STG JOIN SF_OpportunityTeamMember_base_Tmp
ON STG.id = SF_OpportunityTeamMember_base_Tmp.id AND STG.LastModifiedDate >= SF_OpportunityTeamMember_base_Tmp.LastModifiedDate);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."SF_OpportunityTeamMember_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,SYSDATE)::date;





/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_OpportunityTeamMember WHERE id in
(SELECT STG.id FROM SF_OpportunityTeamMember_stg_Tmp_Key STG JOIN SF_OpportunityTeamMember_base_Tmp
ON STG.id = SF_OpportunityTeamMember_base_Tmp.id AND STG.LastModifiedDate >= SF_OpportunityTeamMember_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_OpportunityTeamMember
(
Id
,OpportunityId
,UserId
,Name
,PhotoUrl
,Title
,TeamMemberRole
,OpportunityAccessLevel
,CurrencyIsoCode
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,IsDeleted
,SWT_Speciality__c
,SWT_Account_Id__c
,SWT_Account_Link__c
,SWT_Account_Name__c
,SWT_Opportunity_Id__c
,SWT_Opportunity_Link__c
,SWT_Opportunity_Name__c
,SWT_User_Name__c
,SWT_Role__c
,SWT_INS_DT
)

SELECT DISTINCT 
SF_OpportunityTeamMember_stg_Tmp.Id
,OpportunityId
,UserId
,Name
,PhotoUrl
,Title
,TeamMemberRole
,OpportunityAccessLevel
,CurrencyIsoCode
,CreatedDate
,CreatedById
,SF_OpportunityTeamMember_stg_Tmp.LastModifiedDate
,LastModifiedById
,SystemModstamp
,IsDeleted
,SWT_Speciality__c
,SWT_Account_Id__c
,SWT_Account_Link__c
,SWT_Account_Name__c
,SWT_Opportunity_Id__c
,SWT_Opportunity_Link__c
,SWT_Opportunity_Name__c
,SWT_User_Name__c
,SWT_Role__c
,SYSDATE AS SWT_INS_DT
FROM SF_OpportunityTeamMember_stg_Tmp JOIN SF_OpportunityTeamMember_stg_Tmp_Key ON SF_OpportunityTeamMember_stg_Tmp.Id=SF_OpportunityTeamMember_stg_Tmp_Key.Id and SF_OpportunityTeamMember_stg_Tmp.LastModifiedDate=SF_OpportunityTeamMember_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_OpportunityTeamMember BASE
WHERE SF_OpportunityTeamMember_stg_Tmp.id = BASE.id);

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
select 'SFDC','SF_OpportunityTeamMember',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_OpportunityTeamMember where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_OpportunityTeamMember' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_OpportunityTeamMember' and  COMPLTN_STAT = 'N');
commit;
*/

select do_tm_task('mergeout','swt_rpt_stg.SF_OpportunityTeamMember_Hist');
select do_tm_task('mergeout','swt_rpt_base.SF_OpportunityTeamMember');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_OpportunityTeamMember');


