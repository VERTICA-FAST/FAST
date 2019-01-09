/****
****Script Name	  : NS_Classes.sql
****Description   : Incremental data load for NS_Classes
****/

/* Setting timing on**/
\timing
/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Classes";

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
select 'NETSUITE','NS_Classes',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Classes_Hist SELECT * from "swt_rpt_stg".NS_Classes;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select class_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Classes where class_id in (
select class_id from swt_rpt_stg.NS_Classes group by class_id,date_last_modified having count(1)>1)
group by class_id);


delete from swt_rpt_stg.NS_Classes where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Classes.class_id=t2.class_id and swt_rpt_stg.NS_Classes.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE NS_Classes_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Classes)
SEGMENTED BY HASH(class_id,date_last_modified) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.NS_Classes;

CREATE LOCAL TEMP TABLE NS_Classes_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT class_id,date_last_modified FROM swt_rpt_base.NS_Classes)
SEGMENTED BY HASH(class_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Classes_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT class_id, max(date_last_modified) as date_last_modified FROM NS_Classes_stg_Tmp group by class_id)
SEGMENTED BY HASH(class_id,date_last_modified) ALL NODES;

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Classes_Hist
(
class_extid
,class_id
,date_last_modified
,full_name
,isinactive
,name
,parent_id
,ba_hierarchy_description
,ba_hierarchy_level_id
,description
,LD_DT
,SWT_INS_DT
,d_source
)
select
class_extid
,class_id
,date_last_modified
,full_name
,isinactive
,name
,parent_id
,ba_hierarchy_description
,ba_hierarchy_level_id
,description
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Classes WHERE class_id in
(SELECT STG.class_id FROM NS_Classes_stg_Tmp_Key STG JOIN NS_Classes_base_Tmp
ON STG.class_id = NS_Classes_base_Tmp.class_id AND STG.date_last_modified >= NS_Classes_base_Tmp.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Classes_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Classes WHERE class_id in
(SELECT STG.class_id FROM NS_Classes_stg_Tmp_Key STG JOIN NS_Classes_base_Tmp
ON STG.class_id = NS_Classes_base_Tmp.class_id AND STG.date_last_modified >= NS_Classes_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Classes
(
class_extid
,class_id
,date_last_modified
,full_name
,isinactive
,name
,parent_id
,ba_hierarchy_description
,ba_hierarchy_level_id
,description
,SWT_INS_DT
)
SELECT DISTINCT 
class_extid
,NS_Classes_stg_Tmp.class_id
,NS_Classes_stg_Tmp.date_last_modified
,full_name
,isinactive
,name
,parent_id
,ba_hierarchy_description
,ba_hierarchy_level_id
,description
,sysdate as SWT_INS_DT
FROM NS_Classes_stg_Tmp JOIN NS_Classes_stg_Tmp_Key ON NS_Classes_stg_Tmp.class_id= NS_Classes_stg_Tmp_Key.class_id AND NS_Classes_stg_Tmp.date_last_modified=NS_Classes_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Classes BASE
WHERE NS_Classes_stg_Tmp.class_id = BASE.class_id);



/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Classes' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Classes' and  COMPLTN_STAT = 'N');
*/

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
select 'NETSUITE','NS_Classes',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Classes where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT DO_TM_TASK('mergeout',  'swt_rpt_base.NS_Classes');
SELECT DO_TM_TASK('mergeout',  'swt_rpt_stg.NS_Classes_Hist');

select ANALYZE_STATISTICS('swt_rpt_base.NS_Classes');


