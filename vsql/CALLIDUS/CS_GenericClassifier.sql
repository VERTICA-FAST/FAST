/*Script Name   : CS_GenericClassifier.sql
****Description   : Incremental  data load for CS_GenericClassifier
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
select 'CALLIDUS','CS_GenericClassifier',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."CS_GenericClassifier") ,null,'N';

commit;

CREATE LOCAL TEMP TABLE CS_GenericClassifier_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT distinct * FROM swt_rpt_stg.CS_GenericClassifier)
SEGMENTED BY HASH(classifierseq,effectivestartdate) ALL NODES;


CREATE LOCAL TEMP TABLE CS_GenericClassifier_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT classifierseq,effectivestartdate FROM swt_rpt_base.CS_GenericClassifier)
SEGMENTED BY HASH(classifierseq,effectivestartdate) ALL NODES;

/* Inserting Stage table data into Historical Table */

INSERT /*+DIRECT*/ INTO "swt_rpt_stg"."CS_GenericClassifier_Hist"
(
CLASSIFIERSEQ
,EFFECTIVESTARTDATE
,EFFECTIVEENDDATE
,ISLAST
,CREATEDATE
,REMOVEDATE
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
,LD_DT
,SWT_INS_DT
,d_source
)
SELECT DISTINCT
CLASSIFIERSEQ
,EFFECTIVESTARTDATE
,EFFECTIVEENDDATE
,ISLAST
,CREATEDATE
,REMOVEDATE
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
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".CS_GenericClassifier
where exists(select 1 from CS_GenericClassifier_stg_Tmp where CS_GenericClassifier.classifierseq=CS_GenericClassifier_stg_Tmp.classifierseq
and CS_GenericClassifier.effectivestartdate=CS_GenericClassifier_stg_Tmp.effectivestartdate );

/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."CS_GenericClassifier_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base"."CS_GenericClassifier" where exists(select 1 from CS_GenericClassifier_stg_Tmp where CS_GenericClassifier.classifierseq=CS_GenericClassifier_stg_Tmp.classifierseq
and CS_GenericClassifier.effectivestartdate=CS_GenericClassifier_stg_Tmp.effectivestartdate );

INSERT /*+DIRECT*/ INTO "swt_rpt_base".CS_GenericClassifier
(
CLASSIFIERSEQ
,EFFECTIVESTARTDATE
,EFFECTIVEENDDATE
,ISLAST
,CREATEDATE
,REMOVEDATE
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
,SWT_INS_DT
)
SELECT DISTINCT
CLASSIFIERSEQ
,EFFECTIVESTARTDATE
,EFFECTIVEENDDATE
,ISLAST
,CREATEDATE
,REMOVEDATE
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
,SYSDATE
FROM CS_GenericClassifier_stg_Tmp
where Not exists(select 1 from "swt_rpt_base".CS_GenericClassifier where CS_GenericClassifier.classifierseq=CS_GenericClassifier_stg_Tmp.classifierseq
and CS_GenericClassifier.effectivestartdate=CS_GenericClassifier_stg_Tmp.effectivestartdate)
 and RemoveDate = '2200-01-01';

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'CALLIDUS' and
TBL_NM = 'CS_GenericClassifier' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'CALLIDUS' and  TBL_NM = 'CS_GenericClassifier' and  COMPLTN_STAT = 'N');


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
select 'CALLIDUS','CS_GenericClassifier',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."CS_GenericClassifier") ,(select count(*) from swt_rpt_base.CS_GenericClassifier where SWT_INS_DT::date = sysdate::date),'Y';


COMMIT;

SELECT PURGE_TABLE('swt_rpt_base.CS_GenericClassifier');
SELECT PURGE_TABLE('swt_rpt_stg.CS_GenericClassifier_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.CS_GenericClassifier');
INSERT /*+Direct*/ INTO swt_rpt_stg.CS_GenericClassifier_Hist SELECT * from swt_rpt_stg.CS_GenericClassifier;
COMMIT;
TRUNCATE TABLE swt_rpt_stg.CS_GenericClassifier;




