/****
****Script Name	  : NS_Tax_items.sql
****Description   : Incremental data load for NS_Tax_items
****/
/* Setting timing on**/
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."NS_Tax_items";

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
select 'NETSUITE','NS_Tax_items',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;

INSERT /*+DIRECT*/ INTO "swt_rpt_stg".NS_Tax_items_Hist SELECT * from "swt_rpt_stg".NS_Tax_items;
COMMIT;

CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select item_id,max(auto_id) as auto_id from swt_rpt_stg.NS_Tax_items where item_id in (
select item_id from swt_rpt_stg.NS_Tax_items group by item_id,date_last_modified having count(1)>1)
group by item_id);

delete from swt_rpt_stg.NS_Tax_items where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.NS_Tax_items.item_id=t2.item_id and swt_rpt_stg.NS_Tax_items.auto_id<t2. auto_id);



COMMIT; 

CREATE LOCAL TEMP TABLE NS_Tax_items_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NS_Tax_items)
SEGMENTED BY HASH(item_id,date_last_modified) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.NS_Tax_items;

CREATE LOCAL TEMP TABLE NS_Tax_items_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT item_id,date_last_modified FROM swt_rpt_base.NS_Tax_items)
SEGMENTED BY HASH(item_id,date_last_modified) ALL NODES;


CREATE LOCAL TEMP TABLE NS_Tax_items_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT item_id, max(date_last_modified) as date_last_modified FROM NS_Tax_items_stg_Tmp group by item_id)
SEGMENTED BY HASH(item_id,date_last_modified) ALL NODES;




/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NS_Tax_items_Hist
(
billing_plan_id
,business_unit_id
,center_id
,code_of_supply_id
,commodity_code
,date_last_modified
,default_openair_billing_rul_id
,default_wt_code_id
,deferred_rev_acc_id
,description
,direct_posting
,ex_code
,ex_code_for_purchase_id
,export_to_openair
,export_to_openair_product
,family_id
,for_invoice
,for_purchase
,full_name
,functional_area_id
,funding_type_id
,idt_product_tax_code_descript
,idt_product_tax_code_id
,idt_product_tax_code_name
,income_account_id
,intercompany_product
,isinactive
,item_extid
,item_id
,lcci_id
,mark_up
,name
,nature_of_transaction_codes_id
,openair_export_error
,parent_id
,pillar_id
,product_line_id
,product_subtype_id
,product_type_id
,profit_center_id
,prompt_payment_discount_item
,rate
,source_subsidiary_id
,source_system_id
,supplementary_unit__abberviat
,supplementary_unit_id
,support_business_area_id
,tax_city
,tax_county
,tax_state
,tax_zipcode
,type_of_goods_id
,unspsc_code_id
,vendor_id
,revenue_subtype_id
,tax_type_id
,vendorname
,LD_DT
,SWT_INS_DT
,d_source
)
select
billing_plan_id
,business_unit_id
,center_id
,code_of_supply_id
,commodity_code
,date_last_modified
,default_openair_billing_rul_id
,default_wt_code_id
,deferred_rev_acc_id
,description
,direct_posting
,ex_code
,ex_code_for_purchase_id
,export_to_openair
,export_to_openair_product
,family_id
,for_invoice
,for_purchase
,full_name
,functional_area_id
,funding_type_id
,idt_product_tax_code_descript
,idt_product_tax_code_id
,idt_product_tax_code_name
,income_account_id
,intercompany_product
,isinactive
,item_extid
,item_id
,lcci_id
,mark_up
,name
,nature_of_transaction_codes_id
,openair_export_error
,parent_id
,pillar_id
,product_line_id
,product_subtype_id
,product_type_id
,profit_center_id
,prompt_payment_discount_item
,rate
,source_subsidiary_id
,source_system_id
,supplementary_unit__abberviat
,supplementary_unit_id
,support_business_area_id
,tax_city
,tax_county
,tax_state
,tax_zipcode
,type_of_goods_id
,unspsc_code_id
,vendor_id
,revenue_subtype_id
,tax_type_id
,vendorname
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NS_Tax_items WHERE item_id in
(SELECT STG.item_id FROM NS_Tax_items_stg_Tmp_Key STG JOIN NS_Tax_items_base_Tmp
ON STG.item_id = NS_Tax_items_base_Tmp.item_id AND STG.date_last_modified >= NS_Tax_items_base_Tmp.date_last_modified);



/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NS_Tax_items_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;



/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".NS_Tax_items WHERE item_id in
(SELECT STG.item_id FROM NS_Tax_items_stg_Tmp_Key STG JOIN NS_Tax_items_base_Tmp
ON STG.item_id = NS_Tax_items_base_Tmp.item_id AND STG.date_last_modified >= NS_Tax_items_base_Tmp.date_last_modified);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NS_Tax_items
(
billing_plan_id
,business_unit_id
,center_id
,code_of_supply_id
,commodity_code
,date_last_modified
,default_openair_billing_rul_id
,default_wt_code_id
,deferred_rev_acc_id
,description
,direct_posting
,ex_code
,ex_code_for_purchase_id
,export_to_openair
,export_to_openair_product
,family_id
,for_invoice
,for_purchase
,full_name
,functional_area_id
,funding_type_id
,idt_product_tax_code_descript
,idt_product_tax_code_id
,idt_product_tax_code_name
,income_account_id
,intercompany_product
,isinactive
,item_extid
,item_id
,lcci_id
,mark_up
,name
,nature_of_transaction_codes_id
,openair_export_error
,parent_id
,pillar_id
,product_line_id
,product_subtype_id
,product_type_id
,profit_center_id
,prompt_payment_discount_item
,rate
,source_subsidiary_id
,source_system_id
,supplementary_unit__abberviat
,supplementary_unit_id
,support_business_area_id
,tax_city
,tax_county
,tax_state
,tax_zipcode
,type_of_goods_id
,unspsc_code_id
,vendor_id
,revenue_subtype_id
,tax_type_id
,vendorname
,SWT_INS_DT
)

SELECT DISTINCT
billing_plan_id
,business_unit_id
,center_id
,code_of_supply_id
,commodity_code
,NS_Tax_items_stg_Tmp.date_last_modified
,default_openair_billing_rul_id
,default_wt_code_id
,deferred_rev_acc_id
,description
,direct_posting
,ex_code
,ex_code_for_purchase_id
,export_to_openair
,export_to_openair_product
,family_id
,for_invoice
,for_purchase
,full_name
,functional_area_id
,funding_type_id
,idt_product_tax_code_descript
,idt_product_tax_code_id
,idt_product_tax_code_name
,income_account_id
,intercompany_product
,isinactive
,item_extid
,NS_Tax_items_stg_Tmp.item_id
,lcci_id
,mark_up
,name
,nature_of_transaction_codes_id
,openair_export_error
,parent_id
,pillar_id
,product_line_id
,product_subtype_id
,product_type_id
,profit_center_id
,prompt_payment_discount_item
,rate
,source_subsidiary_id
,source_system_id
,supplementary_unit__abberviat
,supplementary_unit_id
,support_business_area_id
,tax_city
,tax_county
,tax_state
,tax_zipcode
,type_of_goods_id
,unspsc_code_id
,vendor_id
,revenue_subtype_id
,tax_type_id
,vendorname
,sysdate as SWT_INS_DT
FROM NS_Tax_items_stg_Tmp JOIN NS_Tax_items_stg_Tmp_Key ON NS_Tax_items_stg_Tmp.item_id= NS_Tax_items_stg_Tmp_Key.item_id AND NS_Tax_items_stg_Tmp.date_last_modified=NS_Tax_items_stg_Tmp_Key.date_last_modified
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NS_Tax_items BASE
WHERE NS_Tax_items_stg_Tmp.item_id = BASE.item_id);




/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITE' and
TBL_NM = 'NS_Tax_items' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITE' and  TBL_NM = 'NS_Tax_items' and  COMPLTN_STAT = 'N');
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
select 'NETSUITE','NS_Tax_items',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.NS_Tax_items where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


SELECT PURGE_TABLE('swt_rpt_base.NS_Tax_items');
SELECT PURGE_TABLE('swt_rpt_stg.NS_Tax_items_Hist');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NS_Tax_items');


