
/****
****Script Name   : CS_Commission.sql
****Description   : Incremental  data load for CS_Commission
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
select 'CALLIDUS','CS_Commission',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."CS_Commission") ,null,'N';

Commit;

CREATE LOCAL TEMP TABLE CS_Commission_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.CS_Commission)
SEGMENTED BY HASH(PERIODSEQ) ALL NODES;

CREATE LOCAL TEMP TABLE CS_Balance_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT PERIODSEQ FROM swt_rpt_base.CS_Balance)
SEGMENTED BY HASH(PERIODSEQ) ALL NODES;


INSERT /*+DIRECT*/ INTO "swt_rpt_stg"."CS_Commission_Hist"
(
COMMISSIONSEQ
,CREDITSEQ
,INCENTIVESEQ
,ENTRYNUMBER
,PIPELINERUNSEQ
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,VALUE
,UNITTYPEFORVALUE
,RATEVALUE
,UNITTYPEFORRATEVALUE
,BUSINESSUNITMAP
,PROCESSINGUNITSEQ
,UNITTYPEFORENTRYNUMBER
,LD_DT
,SWT_INS_DT
,d_source
)
SELECT DISTINCT
COMMISSIONSEQ
,CREDITSEQ
,INCENTIVESEQ
,ENTRYNUMBER
,PIPELINERUNSEQ
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,VALUE
,UNITTYPEFORVALUE
,RATEVALUE
,UNITTYPEFORRATEVALUE
,BUSINESSUNITMAP
,PROCESSINGUNITSEQ
,UNITTYPEFORENTRYNUMBER
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base"."CS_Commission" WHERE PERIODSEQ IN
(SELECT STG.PERIODSEQ FROM CS_Commission_stg_Tmp STG JOIN CS_Balance_base_Tmp BASE ON STG.PERIODSEQ=BASE.PERIODSEQ);

DELETE /*+DIRECT*/ from "swt_rpt_stg"."CS_Commission_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;

DELETE /*+DIRECT*/ FROM "swt_rpt_base"."CS_Commission" WHERE PERIODSEQ IN
(SELECT STG.PERIODSEQ FROM CS_Commission_stg_Tmp STG JOIN CS_Balance_base_Tmp BASE ON STG.PERIODSEQ=BASE.PERIODSEQ);

INSERT /*+DIRECT*/ INTO "swt_rpt_base"."CS_Commission"
(
COMMISSIONSEQ
,CREDITSEQ
,INCENTIVESEQ
,ENTRYNUMBER
,PIPELINERUNSEQ
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,VALUE
,UNITTYPEFORVALUE
,RATEVALUE
,UNITTYPEFORRATEVALUE
,BUSINESSUNITMAP
,PROCESSINGUNITSEQ
,UNITTYPEFORENTRYNUMBER
,SWT_INS_DT
)
SELECT DISTINCT
STG.COMMISSIONSEQ
,STG.CREDITSEQ
,STG.INCENTIVESEQ
,STG.ENTRYNUMBER
,STG.PIPELINERUNSEQ
,STG.PAYEESEQ
,STG.POSITIONSEQ
,STG.PERIODSEQ
,STG.ORIGINTYPEID
,STG.PIPELINERUNDATE
,STG.VALUE
,STG.UNITTYPEFORVALUE
,STG.RATEVALUE
,STG.UNITTYPEFORRATEVALUE
,STG.BUSINESSUNITMAP
,STG.PROCESSINGUNITSEQ
,STG.UNITTYPEFORENTRYNUMBER
,SYSDATE AS SWT_INS_DT
FROM CS_Commission_stg_Tmp STG JOIN CS_Balance_base_Tmp BASE ON STG.PERIODSEQ=BASE.PERIODSEQ;

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'CALLIDUS' and
TBL_NM = 'CS_Commission' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'CALLIDUS' and  TBL_NM = 'CS_Commission' and  COMPLTN_STAT = 'N');


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
select 'CALLIDUS','CS_Commission',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."CS_Commission") ,(select count(*) from swt_rpt_base.CS_Commission where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


INSERT /*+Direct*/ INTO swt_rpt_stg.CS_Commission_Hist SELECT * from swt_rpt_stg.CS_Commission;
COMMIT;

TRUNCATE TABLE swt_rpt_stg.CS_Commission;

select do_tm_task('mergeout','swt_rpt_stg.CS_Commission_Hist');
select do_tm_task('mergeout','swt_rpt_base.CS_Commission');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.CS_Commission');
