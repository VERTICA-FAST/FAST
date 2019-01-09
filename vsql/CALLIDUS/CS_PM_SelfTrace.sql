/****
****Script Name   : CS_PM_SelfTrace.sql
****Description   : Full load for CS_PM_SelfTrace 
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
  select 'CALLIDUS','CS_PM_SelfTrace',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."CS_PM_SelfTrace") ,null,'N';

  Commit;  

TRUNCATE TABLE "swt_rpt_base"."CS_PM_SelfTrace";

/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from swt_rpt_stg.CS_PM_SelfTrace_Hist  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;

INSERT /*+DIRECT*/ INTO "swt_rpt_base"."CS_PM_SelfTrace"
(SOURCEMEASUREMENTSEQ
,TARGETMEASUREMENTSEQ
,PIPELINERUNSEQ
,SOURCEPERIODSEQ
,TARGETPERIODSEQ
,CONTRIBUTIONVALUE
,UNITTYPEFORCONTRIBUTIONVALUE
,BUSINESSUNITMAP
,PROCESSINGUNITSEQ
,SWT_INS_DT
) 
SELECT DISTINCT
SOURCEMEASUREMENTSEQ
,TARGETMEASUREMENTSEQ
,PIPELINERUNSEQ
,SOURCEPERIODSEQ
,TARGETPERIODSEQ
,CONTRIBUTIONVALUE
,UNITTYPEFORCONTRIBUTIONVALUE
,BUSINESSUNITMAP
,PROCESSINGUNITSEQ
,SYSDATE AS SWT_INS_DT 
FROM "swt_rpt_stg"."CS_PM_SelfTrace";



/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'CALLIDUS' and
TBL_NM = 'CS_PM_SelfTrace' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'CALLIDUS' and  TBL_NM = 'CS_PM_SelfTrace' and  COMPLTN_STAT = 'N');


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
select 'CALLIDUS','CS_PM_SelfTrace',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."CS_PM_SelfTrace") ,(select count(*) from swt_rpt_base.CS_PM_SelfTrace where SWT_INS_DT::date = sysdate::date),'Y';

COMMIT;	

INSERT /*+DIRECT*/ INTO swt_rpt_stg.CS_PM_SelfTrace_Hist SELECT * FROM swt_rpt_stg.CS_PM_SelfTrace;

COMMIT;

TRUNCATE TABLE swt_rpt_stg.CS_PM_SelfTrace;
SELECT PURGE_TABLE('swt_rpt_stg.CS_PM_SelfTrace_Hist');
select ANALYZE_STATISTICS('swt_rpt_base.CS_PM_SelfTrace');

