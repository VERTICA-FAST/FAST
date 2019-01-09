
/****
****Script Name   : CS_DepositAdjustment.sql
****Description   : Incremental  data load for CS_DepositAdjustment
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
select 'CALLIDUS','CS_DepositAdjustment',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."CS_DepositAdjustment") ,null,'N';

Commit;

CREATE LOCAL TEMP TABLE CS_DepositAdjustment_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.CS_DepositAdjustment)
SEGMENTED BY HASH(PERIODSEQ) ALL NODES;



INSERT /*+DIRECT*/ INTO "swt_rpt_stg"."CS_DepositAdjustment_Hist"
(
DEPOSITADJUSTMENTSEQ
,DEPOSITSEQ
,CREATEDATE
,REMOVEDATE
,CREATEDBY
,MODIFIEDBY
,NAME
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,BUSINESSUNITMAP
,ADJUSTBYVALUE
,UNITTYPEFORADJUSTBYVALUE
,TOTALADJUSTVALUE
,UNITTYPEFORTOTALADJUSTVALUE
,ADJUSTTYPEFLAG
,MODIFICATIONMAP
,EARNINGGROUPID
,EARNINGCODEID
,RULESEQ
,ISHELD
,RELEASEDATE
,DEPOSITDATE
,REASONSEQ
,COMMENTS
,GENERICATTRIBUTE1
,GENERICATTRIBUTE2
,GENERICATTRIBUTE3
,GENERICATTRIBUTE4
,GENERICATTRIBUTE5
,GENERICATTRIBUTE6
,GENERICATTRIBUTE7
,GENERICATTRIBUTE8
,GENERICATTRIBUTE9
,GENERICATTRIBUTE10
,GENERICATTRIBUTE11
,GENERICATTRIBUTE12
,GENERICATTRIBUTE13
,GENERICATTRIBUTE14
,GENERICATTRIBUTE15
,GENERICATTRIBUTE16
,GENERICNUMBER1
,UNITTYPEFORGENERICNUMBER1
,GENERICNUMBER2
,UNITTYPEFORGENERICNUMBER2
,GENERICNUMBER3
,UNITTYPEFORGENERICNUMBER3
,GENERICNUMBER4
,UNITTYPEFORGENERICNUMBER4
,GENERICNUMBER5
,UNITTYPEFORGENERICNUMBER5
,GENERICNUMBER6
,UNITTYPEFORGENERICNUMBER6
,GENERICDATE1
,GENERICDATE2
,GENERICDATE3
,GENERICDATE4
,GENERICDATE5
,GENERICDATE6
,GENERICBOOLEAN1
,GENERICBOOLEAN2
,GENERICBOOLEAN3
,GENERICBOOLEAN4
,GENERICBOOLEAN5
,GENERICBOOLEAN6
,PROCESSINGUNITSEQ
,LD_DT
,SWT_INS_DT
,d_source
)
SELECT DISTINCT
DEPOSITADJUSTMENTSEQ
,DEPOSITSEQ
,CREATEDATE
,REMOVEDATE
,CREATEDBY
,MODIFIEDBY
,NAME
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,BUSINESSUNITMAP
,ADJUSTBYVALUE
,UNITTYPEFORADJUSTBYVALUE
,TOTALADJUSTVALUE
,UNITTYPEFORTOTALADJUSTVALUE
,ADJUSTTYPEFLAG
,MODIFICATIONMAP
,EARNINGGROUPID
,EARNINGCODEID
,RULESEQ
,ISHELD
,RELEASEDATE
,DEPOSITDATE
,REASONSEQ
,COMMENTS
,GENERICATTRIBUTE1
,GENERICATTRIBUTE2
,GENERICATTRIBUTE3
,GENERICATTRIBUTE4
,GENERICATTRIBUTE5
,GENERICATTRIBUTE6
,GENERICATTRIBUTE7
,GENERICATTRIBUTE8
,GENERICATTRIBUTE9
,GENERICATTRIBUTE10
,GENERICATTRIBUTE11
,GENERICATTRIBUTE12
,GENERICATTRIBUTE13
,GENERICATTRIBUTE14
,GENERICATTRIBUTE15
,GENERICATTRIBUTE16
,GENERICNUMBER1
,UNITTYPEFORGENERICNUMBER1
,GENERICNUMBER2
,UNITTYPEFORGENERICNUMBER2
,GENERICNUMBER3
,UNITTYPEFORGENERICNUMBER3
,GENERICNUMBER4
,UNITTYPEFORGENERICNUMBER4
,GENERICNUMBER5
,UNITTYPEFORGENERICNUMBER5
,GENERICNUMBER6
,UNITTYPEFORGENERICNUMBER6
,GENERICDATE1
,GENERICDATE2
,GENERICDATE3
,GENERICDATE4
,GENERICDATE5
,GENERICDATE6
,GENERICBOOLEAN1
,GENERICBOOLEAN2
,GENERICBOOLEAN3
,GENERICBOOLEAN4
,GENERICBOOLEAN5
,GENERICBOOLEAN6
,PROCESSINGUNITSEQ
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base"."CS_DepositAdjustment" WHERE PERIODSEQ IN (SELECT PERIODSEQ FROM CS_DepositAdjustment_stg_Tmp);


DELETE /*+DIRECT*/ from "swt_rpt_stg"."CS_DepositAdjustment_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


DELETE /*+DIRECT*/ FROM "swt_rpt_base"."CS_DepositAdjustment" WHERE PERIODSEQ IN (SELECT PERIODSEQ FROM CS_DepositAdjustment_stg_Tmp);


INSERT /*+DIRECT*/ INTO "swt_rpt_base"."CS_DepositAdjustment"
(
DEPOSITADJUSTMENTSEQ
,DEPOSITSEQ
,CREATEDATE
,REMOVEDATE
,CREATEDBY
,MODIFIEDBY
,NAME
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,BUSINESSUNITMAP
,ADJUSTBYVALUE
,UNITTYPEFORADJUSTBYVALUE
,TOTALADJUSTVALUE
,UNITTYPEFORTOTALADJUSTVALUE
,ADJUSTTYPEFLAG
,MODIFICATIONMAP
,EARNINGGROUPID
,EARNINGCODEID
,RULESEQ
,ISHELD
,RELEASEDATE
,DEPOSITDATE
,REASONSEQ
,COMMENTS
,GENERICATTRIBUTE1
,GENERICATTRIBUTE2
,GENERICATTRIBUTE3
,GENERICATTRIBUTE4
,GENERICATTRIBUTE5
,GENERICATTRIBUTE6
,GENERICATTRIBUTE7
,GENERICATTRIBUTE8
,GENERICATTRIBUTE9
,GENERICATTRIBUTE10
,GENERICATTRIBUTE11
,GENERICATTRIBUTE12
,GENERICATTRIBUTE13
,GENERICATTRIBUTE14
,GENERICATTRIBUTE15
,GENERICATTRIBUTE16
,GENERICNUMBER1
,UNITTYPEFORGENERICNUMBER1
,GENERICNUMBER2
,UNITTYPEFORGENERICNUMBER2
,GENERICNUMBER3
,UNITTYPEFORGENERICNUMBER3
,GENERICNUMBER4
,UNITTYPEFORGENERICNUMBER4
,GENERICNUMBER5
,UNITTYPEFORGENERICNUMBER5
,GENERICNUMBER6
,UNITTYPEFORGENERICNUMBER6
,GENERICDATE1
,GENERICDATE2
,GENERICDATE3
,GENERICDATE4
,GENERICDATE5
,GENERICDATE6
,GENERICBOOLEAN1
,GENERICBOOLEAN2
,GENERICBOOLEAN3
,GENERICBOOLEAN4
,GENERICBOOLEAN5
,GENERICBOOLEAN6
,PROCESSINGUNITSEQ
,SWT_INS_DT
)
SELECT DISTINCT
DEPOSITADJUSTMENTSEQ
,DEPOSITSEQ
,CREATEDATE
,REMOVEDATE
,CREATEDBY
,MODIFIEDBY
,NAME
,PAYEESEQ
,POSITIONSEQ
,PERIODSEQ
,ORIGINTYPEID
,PIPELINERUNDATE
,BUSINESSUNITMAP
,ADJUSTBYVALUE
,UNITTYPEFORADJUSTBYVALUE
,TOTALADJUSTVALUE
,UNITTYPEFORTOTALADJUSTVALUE
,ADJUSTTYPEFLAG
,MODIFICATIONMAP
,EARNINGGROUPID
,EARNINGCODEID
,RULESEQ
,ISHELD
,RELEASEDATE
,DEPOSITDATE
,REASONSEQ
,COMMENTS
,GENERICATTRIBUTE1
,GENERICATTRIBUTE2
,GENERICATTRIBUTE3
,GENERICATTRIBUTE4
,GENERICATTRIBUTE5
,GENERICATTRIBUTE6
,GENERICATTRIBUTE7
,GENERICATTRIBUTE8
,GENERICATTRIBUTE9
,GENERICATTRIBUTE10
,GENERICATTRIBUTE11
,GENERICATTRIBUTE12
,GENERICATTRIBUTE13
,GENERICATTRIBUTE14
,GENERICATTRIBUTE15
,GENERICATTRIBUTE16
,GENERICNUMBER1
,UNITTYPEFORGENERICNUMBER1
,GENERICNUMBER2
,UNITTYPEFORGENERICNUMBER2
,GENERICNUMBER3
,UNITTYPEFORGENERICNUMBER3
,GENERICNUMBER4
,UNITTYPEFORGENERICNUMBER4
,GENERICNUMBER5
,UNITTYPEFORGENERICNUMBER5
,GENERICNUMBER6
,UNITTYPEFORGENERICNUMBER6
,GENERICDATE1
,GENERICDATE2
,GENERICDATE3
,GENERICDATE4
,GENERICDATE5
,GENERICDATE6
,GENERICBOOLEAN1
,GENERICBOOLEAN2
,GENERICBOOLEAN3
,GENERICBOOLEAN4
,GENERICBOOLEAN5
,GENERICBOOLEAN6
,PROCESSINGUNITSEQ
,SYSDATE AS SWT_INS_DT
FROM CS_DepositAdjustment_stg_Tmp;

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'CALLIDUS' and
TBL_NM = 'CS_DepositAdjustment' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'CALLIDUS' and  TBL_NM = 'CS_DepositAdjustment' and  COMPLTN_STAT = 'N');


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
select 'CALLIDUS','CS_DepositAdjustment',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."CS_DepositAdjustment") ,(select count(*) from swt_rpt_base.CS_DepositAdjustment where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.CS_DepositAdjustment');
SELECT PURGE_TABLE('swt_rpt_stg.CS_DepositAdjustment_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.CS_DepositAdjustment');
INSERT /*+Direct*/ INTO swt_rpt_stg.CS_DepositAdjustment_Hist SELECT * from swt_rpt_stg.CS_DepositAdjustment;
COMMIT;
TRUNCATE TABLE swt_rpt_stg.CS_DepositAdjustment;