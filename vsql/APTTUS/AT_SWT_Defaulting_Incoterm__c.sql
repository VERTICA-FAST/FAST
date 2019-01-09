/****
****Script Name	  : AT_SWT_Defaulting_Incoterm__c.sql
****Description   : Incremental data load for AT_SWT_Defaulting_Incoterm__c
****/

/* Setting timing on**/
\timing

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."AT_SWT_Defaulting_Incoterm__c";

/* Inserting values into the Audit table  */

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
select 'APTTUS','AT_SWT_Defaulting_Incoterm__c',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".AT_SWT_Defaulting_Incoterm__c_Hist select * from "swt_rpt_stg".AT_SWT_Defaulting_Incoterm__c;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c where id in (
select id from swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);

delete from swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c.id=t2.id and swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c.auto_id<t2.auto_id);

commit;

CREATE LOCAL TEMP TABLE AT_SWT_Defaulting_Incoterm__c_stg_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT * FROM swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;

TRUNCATE TABLE swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c;
 
CREATE LOCAL TEMP TABLE AT_SWT_Defaulting_Incoterm__c_base_Tmp ON COMMIT PRESERVE ROWS AS
( 
SELECT DISTINCT Id,LastModifiedDate FROM swt_rpt_base.AT_SWT_Defaulting_Incoterm__c)
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT Id, max(LastModifiedDate) as LastModifiedDate FROM AT_SWT_Defaulting_Incoterm__c_stg_Tmp group by Id) 
SEGMENTED BY HASH(Id,LastModifiedDate) ALL NODES; 


/* Inserting deleted data into the Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c_Hist
(
Id
,IsDeleted
,Name
,CurrencyIsoCode
,SetupOwnerId
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,SWT_Default_Incoterm_Value__c
,SWT_Second_Incoterm_Value__c
,SWT_Ship_To_Address_Country__c
,LD_DT
,SWT_INS_DT
,d_source
)
 select 
Id
,IsDeleted
,Name
,CurrencyIsoCode
,SetupOwnerId
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,SWT_Default_Incoterm_Value__c
,SWT_Second_Incoterm_Value__c
,SWT_Ship_To_Address_Country__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".AT_SWT_Defaulting_Incoterm__c WHERE id in
(SELECT STG.id FROM AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key STG JOIN AT_SWT_Defaulting_Incoterm__c_base_Tmp
ON STG.id = AT_SWT_Defaulting_Incoterm__c_base_Tmp.id AND STG.LastModifiedDate >= AT_SWT_Defaulting_Incoterm__c_base_Tmp.LastModifiedDate);


/* Deleting before seven days data from current date in the Historical Table */  

/*delete /*+DIRECT*/ from "swt_rpt_stg"."AT_SWT_Defaulting_Incoterm__c_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date; */

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".AT_SWT_Defaulting_Incoterm__c WHERE id in
(SELECT STG.id FROM AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key STG JOIN AT_SWT_Defaulting_Incoterm__c_base_Tmp
ON STG.id = AT_SWT_Defaulting_Incoterm__c_base_Tmp.id AND STG.LastModifiedDate >= AT_SWT_Defaulting_Incoterm__c_base_Tmp.LastModifiedDate);


INSERT /*+DIRECT*/ INTO "swt_rpt_base".AT_SWT_Defaulting_Incoterm__c
(
Id
,IsDeleted
,Name
,CurrencyIsoCode
,SetupOwnerId
,CreatedDate
,CreatedById
,LastModifiedDate
,LastModifiedById
,SystemModstamp
,SWT_Default_Incoterm_Value__c
,SWT_Second_Incoterm_Value__c
,SWT_Ship_To_Address_Country__c
,SWT_INS_DT
)
SELECT DISTINCT  
AT_SWT_Defaulting_Incoterm__c_stg_Tmp.Id
,IsDeleted
,Name
,CurrencyIsoCode
,SetupOwnerId
,CreatedDate
,CreatedById
,AT_SWT_Defaulting_Incoterm__c_stg_Tmp.LastModifiedDate
,LastModifiedById
,SystemModstamp
,SWT_Default_Incoterm_Value__c
,SWT_Second_Incoterm_Value__c
,SWT_Ship_To_Address_Country__c
,SYSDATE AS SWT_INS_DT
FROM AT_SWT_Defaulting_Incoterm__c_stg_Tmp JOIN AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key ON AT_SWT_Defaulting_Incoterm__c_stg_Tmp.id= AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key.id AND AT_SWT_Defaulting_Incoterm__c_stg_Tmp.LastModifiedDate=AT_SWT_Defaulting_Incoterm__c_stg_Tmp_Key.LastModifiedDate
	WHERE NOT EXISTS
	(SELECT 1 FROM "swt_rpt_base".AT_SWT_Defaulting_Incoterm__c BASE
		WHERE AT_SWT_Defaulting_Incoterm__c_stg_Tmp.id = BASE.id);
		
		
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
select 'APTTUS','AT_SWT_Defaulting_Incoterm__c',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.AT_SWT_Defaulting_Incoterm__c where SWT_INS_DT::date = sysdate::date),'Y';

Commit;
		
/* Deleting partial audit entry */

/*DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'APTTUS' and
TBL_NM = 'AT_SWT_Defaulting_Incoterm__c' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'APTTUS' and  TBL_NM = 'AT_SWT_Defaulting_Incoterm__c' and  COMPLTN_STAT = 'N');

Commit;
*/

SELECT DROP_PARTITIONS('swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.AT_SWT_Defaulting_Incoterm__c_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.AT_SWT_Defaulting_Incoterm__c');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
SELECT ANALYZE_STATISTICS('swt_rpt_base.AT_SWT_Defaulting_Incoterm__c');






