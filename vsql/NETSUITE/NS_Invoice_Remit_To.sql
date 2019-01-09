/****
****Script Name	  : NS_Invoice_Remit_To.sql
****Description   : Incremental data load for NS_Invoice_Remit_To
****/

/* Setting timing on**/
\timing
/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Invoice_Remit_To";

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
select 'NETSUITE','NS_Invoice_Remit_To',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;


INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Invoice_Remit_To_Hist SELECT * from "swt_rpt_stg".NS_Invoice_Remit_To;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select invoice_remit_to_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Invoice_Remit_To where invoice_remit_to_id in (
select invoice_remit_to_id from swt_rpt_stg.NS_Invoice_Remit_To group by invoice_remit_to_id,last_modified_date having count(1)>1)
group by invoice_remit_to_id);

delete from swt_rpt_stg.NS_Invoice_Remit_To where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Invoice_Remit_To.invoice_remit_to_id=t2.invoice_remit_to_id and swt_rpt_stg.NS_Invoice_Remit_To.auto_id<t2. auto_id);

COMMIT; 


CREATE LOCAL TEMP TABLE NS_Invoice_Remit_To_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Invoice_Remit_To)
SEGMENTED BY HASH(invoice_remit_to_id,last_modified_date) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_Invoice_Remit_To;

CREATE LOCAL TEMP TABLE NS_Invoice_Remit_To_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT invoice_remit_to_id,last_modified_date FROM swt_rpt_base.NS_Invoice_Remit_To)
SEGMENTED BY HASH(invoice_remit_to_id,last_modified_date) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Invoice_Remit_To_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT invoice_remit_to_id, max(last_modified_date) as last_modified_date FROM NS_Invoice_Remit_To_stg_Tmp group by invoice_remit_to_id)
SEGMENTED BY HASH(invoice_remit_to_id,last_modified_date) ALL NODES;



/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Invoice_Remit_To_Hist
(
date_created
,invoice_remit_to_extid
,invoice_remit_to_id
,invoice_remit_to_name
,is_inactive
,last_modified_date
,parent_id
,remit_to_account_number
,remit_to_address
,remit_to_bank
,remit_to_city
,remit_to_company_code
,remit_to_country_id
,remit_to_currency_id
,remit_to_legal_name
,remit_to_routing_number
,remit_to_subsidiary_id
,remit_to_swiftbic_code
,ic_applicable
,remit_to_state
,trade_applicable
,remit_to_zip_code
,LD_DT
,SWT_INS_DT
,d_source
)
select
date_created
,invoice_remit_to_extid
,invoice_remit_to_id
,invoice_remit_to_name
,is_inactive
,last_modified_date
,parent_id
,remit_to_account_number
,remit_to_address
,remit_to_bank
,remit_to_city
,remit_to_company_code
,remit_to_country_id
,remit_to_currency_id
,remit_to_legal_name
,remit_to_routing_number
,remit_to_subsidiary_id
,remit_to_swiftbic_code
,ic_applicable
,remit_to_state
,trade_applicable
,remit_to_zip_code
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Invoice_Remit_To WHERE invoice_remit_to_id in
(SELECT STG.invoice_remit_to_id FROM NS_Invoice_Remit_To_stg_Tmp_Key STG JOIN NS_Invoice_Remit_To_base_Tmp
ON STG.invoice_remit_to_id = NS_Invoice_Remit_To_base_Tmp.invoice_remit_to_id AND STG.last_modified_date >= NS_Invoice_Remit_To_base_Tmp.last_modified_date);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Invoice_Remit_To_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */


DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Invoice_Remit_To WHERE invoice_remit_to_id in
(SELECT STG.invoice_remit_to_id FROM NS_Invoice_Remit_To_stg_Tmp_Key STG JOIN NS_Invoice_Remit_To_base_Tmp
ON STG.invoice_remit_to_id = NS_Invoice_Remit_To_base_Tmp.invoice_remit_to_id AND STG.last_modified_date >= NS_Invoice_Remit_To_base_Tmp.last_modified_date);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Invoice_Remit_To
(
date_created
,invoice_remit_to_extid
,invoice_remit_to_id
,invoice_remit_to_name
,is_inactive
,last_modified_date
,parent_id
,remit_to_account_number
,remit_to_address
,remit_to_bank
,remit_to_city
,remit_to_company_code
,remit_to_country_id
,remit_to_currency_id
,remit_to_legal_name
,remit_to_routing_number
,remit_to_subsidiary_id
,remit_to_swiftbic_code
,ic_applicable
,remit_to_state
,trade_applicable
,remit_to_zip_code
,SWT_INS_DT
)
SELECT DISTINCT
date_created
,invoice_remit_to_extid
,NS_Invoice_Remit_To_stg_Tmp.invoice_remit_to_id
,invoice_remit_to_name
,is_inactive
,NS_Invoice_Remit_To_stg_Tmp.last_modified_date
,parent_id
,remit_to_account_number
,remit_to_address
,remit_to_bank
,remit_to_city
,remit_to_company_code
,remit_to_country_id
,remit_to_currency_id
,remit_to_legal_name
,remit_to_routing_number
,remit_to_subsidiary_id
,remit_to_swiftbic_code
,ic_applicable
,remit_to_state
,trade_applicable
,remit_to_zip_code
,SYSDATE as SWT_INS_DT
FROM NS_Invoice_Remit_To_stg_Tmp JOIN NS_Invoice_Remit_To_stg_Tmp_Key ON NS_Invoice_Remit_To_stg_Tmp.invoice_remit_to_id= NS_Invoice_Remit_To_stg_Tmp_Key.invoice_remit_to_id AND NS_Invoice_Remit_To_stg_Tmp.last_modified_date=NS_Invoice_Remit_To_stg_Tmp_Key.last_modified_date
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Invoice_Remit_To BASE
WHERE NS_Invoice_Remit_To_stg_Tmp.invoice_remit_to_id = BASE.invoice_remit_to_id);


/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Invoice_Remit_To' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Invoice_Remit_To' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Invoice_Remit_To',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Invoice_Remit_To where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Invoice_Remit_To');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Invoice_Remit_To_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_Invoice_Remit_To');


