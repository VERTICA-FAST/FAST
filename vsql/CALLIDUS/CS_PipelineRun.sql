

/****
****Script Name   : CS_PipelineRun.sql
****Description   : Incremental  data load for CS_PipelineRun
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select sysdate st from dual;

INSERT /*+DIRECT*/ INTO swt_rpt_stg.FAST_LD_AUDT
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
select 'CALLIDUS','CS_PipelineRun',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."CS_PipelineRun") ,null,'N';

Commit;

CREATE LOCAL TEMP TABLE CS_PipelineRun_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.CS_PipelineRun)
SEGMENTED BY HASH(PIPELINERUNSEQ) ALL NODES;


INSERT /*+DIRECT*/ INTO "swt_rpt_stg"."CS_PipelineRun_Hist"
(
PIPELINERUNSEQ
,STARTTIME
,STOPTIME
,PERIODSEQ
,USERID
,COMMAND
,STAGETYPESEQ
,RUNMODE
,TRACELEVEL
,STATUS
,TARGETDATABASE
,PRODUCTVERSION
,SCHEMAVERSION
,STOREDPROCVERSION
,DESCRIPTION
,BATCHNAME
,PROCESSINGUNITSEQ
,REMOVEDATE
,MODELRUNSEQ
,RUNPROGRESS
,LD_DT
,SWT_INS_DT
,d_source
)
SELECT DISTINCT
PIPELINERUNSEQ
,STARTTIME
,STOPTIME
,PERIODSEQ
,USERID
,COMMAND
,STAGETYPESEQ
,RUNMODE
,TRACELEVEL
,STATUS
,TARGETDATABASE
,PRODUCTVERSION
,SCHEMAVERSION
,STOREDPROCVERSION
,DESCRIPTION
,BATCHNAME
,PROCESSINGUNITSEQ
,REMOVEDATE
,MODELRUNSEQ
,RUNPROGRESS
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base"."CS_PipelineRun" WHERE PIPELINERUNSEQ IN (SELECT PIPELINERUNSEQ FROM CS_PipelineRun_stg_Tmp);

DELETE /*+DIRECT*/ from "swt_rpt_stg"."CS_PipelineRun_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;

DELETE /*+DIRECT*/ FROM "swt_rpt_base"."CS_PipelineRun" WHERE PIPELINERUNSEQ IN (SELECT PIPELINERUNSEQ FROM CS_PipelineRun_stg_Tmp);

INSERT /*+DIRECT*/ INTO "swt_rpt_base"."CS_PipelineRun"
(
PIPELINERUNSEQ
,STARTTIME
,STOPTIME
,PERIODSEQ
,USERID
,COMMAND
,STAGETYPESEQ
,RUNMODE
,TRACELEVEL
,STATUS
,TARGETDATABASE
,PRODUCTVERSION
,SCHEMAVERSION
,STOREDPROCVERSION
,DESCRIPTION
,BATCHNAME
,PROCESSINGUNITSEQ
,REMOVEDATE
,MODELRUNSEQ
,RUNPROGRESS
,SWT_INS_DT
)
SELECT DISTINCT
PIPELINERUNSEQ
,STARTTIME
,STOPTIME
,PERIODSEQ
,USERID
,COMMAND
,STAGETYPESEQ
,RUNMODE
,TRACELEVEL
,STATUS
,TARGETDATABASE
,PRODUCTVERSION
,SCHEMAVERSION
,STOREDPROCVERSION
,DESCRIPTION
,BATCHNAME
,PROCESSINGUNITSEQ
,REMOVEDATE
,MODELRUNSEQ
,RUNPROGRESS
,SYSDATE AS SWT_INS_DT
FROM CS_PipelineRun_stg_Tmp;

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'CALLIDUS' and
TBL_NM = 'CS_PipelineRun' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'CALLIDUS' and  TBL_NM = 'CS_PipelineRun' and  COMPLTN_STAT = 'N');


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
select 'CALLIDUS','CS_PipelineRun',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."CS_PipelineRun") ,(select count(*) from swt_rpt_base.CS_PipelineRun where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.CS_PipelineRun');
SELECT PURGE_TABLE('swt_rpt_stg.CS_PipelineRun_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.CS_PipelineRun');
INSERT /*+Direct*/ INTO swt_rpt_stg.CS_PipelineRun_Hist SELECT * from swt_rpt_stg.CS_PipelineRun;
COMMIT;
TRUNCATE TABLE swt_rpt_stg.CS_PipelineRun;
