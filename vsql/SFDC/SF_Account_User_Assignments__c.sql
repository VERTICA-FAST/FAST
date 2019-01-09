/****
****Script Name   : SF_Account_User_Assignments__c.sql
****Description   : Incremental data load for SF_Account_User_Assignments__c
****/

/**SET SESSION AUTOCOMMIT TO OFF **/

--SET SESSION AUTOCOMMIT TO OFF;

/*Setting timing on */
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_Account_User_Assignments__c";

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
select 'SFDC','SF_Account_User_Assignments__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_Account_User_Assignments__c_Hist SELECT * from swt_rpt_stg.SF_Account_User_Assignments__c;
Commit;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_Account_User_Assignments__c where id in (
select id from swt_rpt_stg.SF_Account_User_Assignments__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_Account_User_Assignments__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_Account_User_Assignments__c.id=t2.id and swt_rpt_stg.SF_Account_User_Assignments__c.auto_id<t2.auto_id);

Commit; 

CREATE LOCAL TEMP TABLE SF_Account_User_Assignments__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM "swt_rpt_stg".SF_Account_User_Assignments__c
) SEGMENTED BY HASH(id,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_Account_User_Assignments__c;

CREATE LOCAL TEMP TABLE SF_Account_User_Assignments__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT id,LastModifiedDate FROM "swt_rpt_base".SF_Account_User_Assignments__c
) SEGMENTED BY HASH(id,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_Account_User_Assignments__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_Account_User_Assignments__c_stg_Tmp group by id
) SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;



/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_Account_User_Assignments__c_Hist
(
Name
,CurrencyIsoCode
,SWT_Account_ID__c
,SWT_Account_Number__c
,SWT_Business_Area_Group__c
,SWT_New_sales_representative__c
,SWT_Old_sales_representative__c
,Id
,OwnerId
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastViewedDate
,LastReferencedDate
,SWT_Active__c
,SWT_External_ID__c
,SWT_Business_Unit__c
,SWT_Delete_Flag__c
,SWT_User_s_Role_Type__c
,Start_Date__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
Name
,CurrencyIsoCode
,SWT_Account_ID__c
,SWT_Account_Number__c
,SWT_Business_Area_Group__c
,SWT_New_sales_representative__c
,SWT_Old_sales_representative__c
,Id
,OwnerId
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastViewedDate
,LastReferencedDate
,SWT_Active__c
,SWT_External_ID__c
,SWT_Business_Unit__c
,SWT_Delete_Flag__c
,SWT_User_s_Role_Type__c
,Start_Date__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_Account_User_Assignments__c WHERE id in 
(SELECT STG.id FROM SF_Account_User_Assignments__c_stg_Tmp_Key STG JOIN  SF_Account_User_Assignments__c_base_Tmp
ON STG.id = SF_Account_User_Assignments__c_base_Tmp.id AND STG.LastModifiedDate >= SF_Account_User_Assignments__c_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_Account_User_Assignments__c_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_Account_User_Assignments__c WHERE id in
(SELECT STG.id FROM SF_Account_User_Assignments__c_stg_Tmp_Key STG JOIN  SF_Account_User_Assignments__c_base_Tmp
ON STG.id = SF_Account_User_Assignments__c_base_Tmp.id AND STG.LastModifiedDate >= SF_Account_User_Assignments__c_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_Account_User_Assignments__c
(
Name
,CurrencyIsoCode
,SWT_Account_ID__c
,SWT_Account_Number__c
,SWT_Business_Area_Group__c
,SWT_New_sales_representative__c
,SWT_Old_sales_representative__c
,Id
,OwnerId
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastViewedDate
,LastReferencedDate
,SWT_Active__c
,SWT_External_ID__c
,SWT_Business_Unit__c
,SWT_Delete_Flag__c
,SWT_User_s_Role_Type__c
,Start_Date__c
,SWT_INS_DT
)
SELECT
DISTINCT  Name
,CurrencyIsoCode
,SWT_Account_ID__c
,SWT_Account_Number__c
,SWT_Business_Area_Group__c
,SWT_New_sales_representative__c
,SWT_Old_sales_representative__c
,stg_Tmp.Id
,OwnerId
,IsDeleted
,CreatedDate
,CreatedById
,stg_Tmp.LastModifiedDate
,LastModifiedById
,SystemModstamp
,LastViewedDate
,LastReferencedDate
,SWT_Active__c
,SWT_External_ID__c
,SWT_Business_Unit__c
,SWT_Delete_Flag__c
,SWT_User_s_Role_Type__c
,Start_Date__c
,SYSDATE AS SWT_INS_DT
FROM SF_Account_User_Assignments__c_stg_Tmp stg_Tmp JOIN SF_Account_User_Assignments__c_stg_Tmp_Key stg_Tmp_Key ON stg_Tmp.id = stg_Tmp_Key.id AND stg_Tmp.LastModifiedDate = stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".SF_Account_User_Assignments__c BASE
WHERE stg_Tmp.id = BASE.id);


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
select 'SFDC','SF_Account_User_Assignments__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_Account_User_Assignments__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
/* Deleting partial audit entry */

/*DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_Account_User_Assignments__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_Account_User_Assignments__c' and  COMPLTN_STAT = 'N');

commit;
*/

SELECT DROP_PARTITION('swt_rpt_stg.SF_Account_User_Assignments__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_Account_User_Assignments__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_Account_User_Assignments__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_Account_User_Assignments__c');





