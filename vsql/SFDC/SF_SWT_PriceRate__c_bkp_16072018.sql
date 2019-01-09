/****
****Script Name	  : SF_SWT_PriceRate__c.sql
****Description   : Incremental data load for SF_SWT_PriceRate__c
****/

/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_SWT_PriceRate__c";

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
select 'SFDC','SF_SWT_PriceRate__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

  Commit; 

INSERT /*+Direct*/ INTO swt_rpt_stg.SF_SWT_PriceRate__c_Hist SELECT * from swt_rpt_stg.SF_SWT_PriceRate__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_SWT_PriceRate__c where id in (
select id from swt_rpt_stg.SF_SWT_PriceRate__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_SWT_PriceRate__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_SWT_PriceRate__c.id=t2.id and swt_rpt_stg.SF_SWT_PriceRate__c.auto_id<t2.auto_id);

Commit; 
  
  
CREATE LOCAL TEMP TABLE SF_SWT_PriceRate__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_SWT_PriceRate__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_SWT_PriceRate__c;

CREATE LOCAL TEMP TABLE SF_SWT_PriceRate__c_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_SWT_PriceRate__c)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_SWT_PriceRate__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_SWT_PriceRate__c_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;
  

/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_SWT_PriceRate__c_Hist
(
CurrencyIsoCode
,LastModifiedById
,Ownerid
,Name
,SWT_Currency__c
,SWT_EffectiveFromDate__c
,SWT_GroupID__c
,SWT_PriceRateFactor__c
,Id
,LastModifiedDate
,APTS_Ext_ID__c
,CreatedById
,CreatedDate
,IsDeleted
,LastActivityDate
,LastReferencedDate
,LastViewedDate
,SystemModstamp
,LD_DT
,SWT_INS_DT
,d_source
)
 select 
CurrencyIsoCode
,LastModifiedById
,Ownerid
,Name
,SWT_Currency__c
,SWT_EffectiveFromDate__c
,SWT_GroupID__c
,SWT_PriceRateFactor__c
,Id
,LastModifiedDate
,APTS_Ext_ID__c
,CreatedById
,CreatedDate
,IsDeleted
,LastActivityDate
,LastReferencedDate
,LastViewedDate
,SystemModstamp
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_SWT_PriceRate__c WHERE id in
(SELECT STG.id FROM SF_SWT_PriceRate__c_stg_Tmp_Key STG JOIN SF_SWT_PriceRate__c_base_Tmp
ON STG.id = SF_SWT_PriceRate__c_base_Tmp.id AND STG.LastModifiedDate >= SF_SWT_PriceRate__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */  

delete /*+DIRECT*/ from "swt_rpt_stg"."SF_SWT_PriceRate__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


 /* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_SWT_PriceRate__c WHERE id in
(SELECT STG.id FROM SF_SWT_PriceRate__c_stg_Tmp_Key STG JOIN SF_SWT_PriceRate__c_base_Tmp
ON STG.id = SF_SWT_PriceRate__c_base_Tmp.id AND STG.LastModifiedDate >= SF_SWT_PriceRate__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_SWT_PriceRate__c
(
CurrencyIsoCode
,LastModifiedById
,Ownerid
,Name
,SWT_Currency__c
,SWT_EffectiveFromDate__c
,SWT_GroupID__c
,SWT_PriceRateFactor__c
,Id
,LastModifiedDate
,APTS_Ext_ID__c
,CreatedById
,CreatedDate
,IsDeleted
,LastActivityDate
,LastReferencedDate
,LastViewedDate
,SystemModstamp
,SWT_INS_DT
)
SELECT DISTINCT
 CurrencyIsoCode
,LastModifiedById
,Ownerid
,Name
,SWT_Currency__c
,SWT_EffectiveFromDate__c
,SWT_GroupID__c
,SWT_PriceRateFactor__c
,SF_SWT_PriceRate__c_stg_Tmp.Id
,SF_SWT_PriceRate__c_stg_Tmp.LastModifiedDate
,APTS_Ext_ID__c
,CreatedById
,CreatedDate
,IsDeleted
,LastActivityDate
,LastReferencedDate
,LastViewedDate
,SystemModstamp
,SYSDATE
FROM SF_SWT_PriceRate__c_stg_Tmp JOIN SF_SWT_PriceRate__c_stg_Tmp_Key ON SF_SWT_PriceRate__c_stg_Tmp.id= SF_SWT_PriceRate__c_stg_Tmp_Key.id AND SF_SWT_PriceRate__c_stg_Tmp.LastModifiedDate=SF_SWT_PriceRate__c_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_SWT_PriceRate__c BASE
WHERE SF_SWT_PriceRate__c_stg_Tmp.id = BASE.id);	

/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_SWT_PriceRate__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_SWT_PriceRate__c' and  COMPLTN_STAT = 'N');


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
select 'SFDC','SF_SWT_PriceRate__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_SWT_PriceRate__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
    

select do_tm_task('mergeout','swt_rpt_stg.SF_SWT_PriceRate__c_Hist');
select do_tm_task('mergeout','swt_rpt_base.SF_SWT_PriceRate__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_SWT_PriceRate__c');

