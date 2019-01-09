
/****
****Script Name   : SF_GroupMember.sql
****Description   : Incremental data load for SF_GroupMember
****/


/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

--SET SESSION AUTOCOMMIT TO OFF;

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_GroupMember";

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
select 'SFDC','SF_GroupMember',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_GroupMember_Hist SELECT * from swt_rpt_stg.SF_GroupMember;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_GroupMember where id in (
select id from swt_rpt_stg.SF_GroupMember group by id,SystemModstamp having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_GroupMember where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_GroupMember.id=t2.id and swt_rpt_stg.SF_GroupMember.auto_id<t2.auto_id);

Commit; 



CREATE LOCAL TEMP TABLE SF_GroupMember_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_GroupMember)
SEGMENTED BY HASH(Id,SystemModstamp) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_GroupMember;

CREATE LOCAL TEMP TABLE SF_GroupMember_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT Id,SystemModstamp FROM swt_rpt_base.SF_GroupMember)
SEGMENTED BY HASH(Id,SystemModstamp) ALL NODES;


CREATE LOCAL TEMP TABLE SF_GroupMember_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT Id, max(SystemModstamp) as SystemModstamp FROM SF_GroupMember_stg_Tmp group by Id)
SEGMENTED BY HASH(Id,SystemModstamp) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_GroupMember_Hist
(
GroupId
,Id
,SystemModstamp
,UserOrGroupId
,LD_DT
,SWT_INS_DT
,d_source
)
select
GroupId
,Id
,SystemModstamp
,UserOrGroupId
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_GroupMember WHERE id in
(SELECT STG.id FROM SF_GroupMember_stg_Tmp_Key STG JOIN SF_GroupMember_base_Tmp
ON STG.id = SF_GroupMember_base_Tmp.id AND STG.SystemModstamp >= SF_GroupMember_base_Tmp.SystemModstamp);

/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_GroupMember_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_GroupMember WHERE id in
(SELECT STG.id FROM SF_GroupMember_stg_Tmp_Key STG JOIN SF_GroupMember_base_Tmp
ON STG.id = SF_GroupMember_base_Tmp.id AND STG.SystemModstamp >= SF_GroupMember_base_Tmp.SystemModstamp);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_GroupMember
(
GroupId
,Id
,SystemModstamp
,UserOrGroupId
,SWT_INS_DT
)
SELECT DISTINCT 
GroupId
,SF_GroupMember_stg_Tmp.Id
,SF_GroupMember_stg_Tmp.SystemModstamp
,UserOrGroupId
,SYSDATE AS SWT_INS_DT
FROM SF_GroupMember_stg_Tmp JOIN SF_GroupMember_stg_Tmp_Key ON SF_GroupMember_stg_Tmp.Id= SF_GroupMember_stg_Tmp_Key.Id AND SF_GroupMember_stg_Tmp.SystemModstamp=SF_GroupMember_stg_Tmp_Key.SystemModstamp
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".SF_GroupMember BASE
WHERE SF_GroupMember_stg_Tmp.Id = BASE.Id);


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
select 'SFDC','SF_GroupMember',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_GroupMember where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_GroupMember' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_GroupMember' and  COMPLTN_STAT = 'N');

Commit;
*/

SELECT DROP_PARTITION('swt_rpt_stg.SF_GroupMember_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_GroupMember_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_GroupMember');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_GroupMember');


