/****Script Name   : NO_Project.sql
****Description   : Incremental data load for NO_Project
****/
/* Setting timing on**/
\timing 
/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select sysdate st from dual;

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
select 'NETSUITEOPENAIR','NO_Project',sysdate::date,sysdate,null,(select count(*) from "swt_rpt_stg"."NO_Project") ,null,'N';

Commit;

CREATE LOCAL TEMP TABLE NO_Project_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.NO_Project)
SEGMENTED BY HASH(ID,UPDATED) ALL NODES;


CREATE LOCAL TEMP TABLE NO_Project_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,UPDATED FROM swt_rpt_base.NO_Project)
SEGMENTED BY HASH(ID,UPDATED) ALL NODES;


CREATE LOCAL TEMP TABLE NO_Project_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(UPDATED) as UPDATED FROM NO_Project_stg_Tmp group by id)
SEGMENTED BY HASH(ID,UPDATED) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.NO_Project_Hist
(
id
,auto_bill
,az_approver
,message
,po_approver
,auto_bill_cap
,invoice_layoutid
,rate
,notify_assignees
,sync_workspace
,notify_owner
,br_approvalprocess
,te_approver
,ta_approvalprocess
,te_approvalprocess
,auto_bill_cap_value
,updated
,code
,tb_approver
,tax_location_name
,active
,name
,hierarchy_node_ids
,externalid
,po_approvalprocess
,project_stageid
,tax_locationid
,ta_approver
,pr_approver
,locationid
,finish_date
,msp_link_type
,customerid
,customer_name
,only_owner_can_edit
,br_approver
,userid
,az_approvalprocess
,project_locationid
,budget
,currency
,cost_centerid
,sga_labor
,invoice_text
,budget_time
,start_date
,pr_approvalprocess
,billing_contactid
,billing_code
,created
,no_dirty
,create_workspace
,tb_approvalprocess
,auto_bill_override
,template_project_id
,copy_revenue_recognition_rules
,copy_revenue_recognition_auto_settings
,copy_project_billing_rules
,copy_project_billing_auto_settings
,copy_project_pricing
,copy_custom_fields
,copy_loaded_cost
,copy_approvers
,copy_issues
,copy_notification_settings
,copy_dashboard_settings
,copy_invoice_layout_settings
,pm_approver_1
,pm_approver_2
,shipping_contact_id
,sold_to_contact_id
,attachmentid
,rv_approver
,rv_approvalprocess
,portfolio_projectid
,is_portfolio_project
,copy_revenuerecognition_auto_settings
,filterset_ids
,notify_issue_assigned_to
,notify_issue_closed_assigned_to
,notify_issue_closed_customer_owner
,notify_issue_closed_project_owner
,notify_issue_created_customer_owner
,notify_issue_created_project_owner
,notify_sr_submitted_project_owner
,ta_include
,rm_approvalprocess
,rate_cardid
,pm_approver_3
,picklist_label
,user_filter
,category_filter
,timetype_filter
,payroll_type_filter
,te_include_rate_cardid
,notes_required_on_ts__c
,proj_allow_expenses__c
,proj_allow_time_entry__c
,netsuite_project_id__c
,proj_parentproj__c
,netsuite_project_expenses__c
,proj_parentproj_id__c
,proj_billtype__c
,proj_revtype__c
,proj_enddate__c
,proj_lookupTest__c
,proj_customer_ns_id__c
,project_type__c
,project_program__c
,project_subsidary_id__c
,proj_program_id__c
,prj_overall_status__c
,proj_EACcost__c
,proj_CMT__c
,ns_percent_complete__c
,ns_eac_value__c
,expected_close__c
,proj_DWObudget__c
,proj_entityid__c
,proj_businessarea__c
,proj_MRU__c
,proj_LCCI__c
,proj_ProfitCenter__c
,proj_functionalarea__c
,proj_businessareaID__c
,proj_functionalareaID__c
,proj_MRUID__c
,proj_LCCIID__c
,proj_ProfitCenterID__c
,proj_prepaid__c
,proj_project_type_labor_costing__c
,proj_Deliverymanager__c
,proj_legacy_id__c
,project_clientprincipal__c
,proj_region__c
,project_closed__c
,proj_market_offering__c
,proj_service_offering__c
,prog_stage__c
,project_id__c
,program_id__c
,proj_initiateforecast__c
,proj_ent_id__c
,skip_task_recalc__c
,eac_forecast_timestamp__c
,eac_forecast_fail_reason__c
,eac_forecast_check__c
,LD_DT
,SWT_INS_DT
,d_source
)
select
id
,auto_bill
,az_approver
,message
,po_approver
,auto_bill_cap
,invoice_layoutid
,rate
,notify_assignees
,sync_workspace
,notify_owner
,br_approvalprocess
,te_approver
,ta_approvalprocess
,te_approvalprocess
,auto_bill_cap_value
,updated
,code
,tb_approver
,tax_location_name
,active
,name
,hierarchy_node_ids
,externalid
,po_approvalprocess
,project_stageid
,tax_locationid
,ta_approver
,pr_approver
,locationid
,finish_date
,msp_link_type
,customerid
,customer_name
,only_owner_can_edit
,br_approver
,userid
,az_approvalprocess
,project_locationid
,budget
,currency
,cost_centerid
,sga_labor
,invoice_text
,budget_time
,start_date
,pr_approvalprocess
,billing_contactid
,billing_code
,created
,no_dirty
,create_workspace
,tb_approvalprocess
,auto_bill_override
,template_project_id
,copy_revenue_recognition_rules
,copy_revenue_recognition_auto_settings
,copy_project_billing_rules
,copy_project_billing_auto_settings
,copy_project_pricing
,copy_custom_fields
,copy_loaded_cost
,copy_approvers
,copy_issues
,copy_notification_settings
,copy_dashboard_settings
,copy_invoice_layout_settings
,pm_approver_1
,pm_approver_2
,shipping_contact_id
,sold_to_contact_id
,attachmentid
,rv_approver
,rv_approvalprocess
,portfolio_projectid
,is_portfolio_project
,copy_revenuerecognition_auto_settings
,filterset_ids
,notify_issue_assigned_to
,notify_issue_closed_assigned_to
,notify_issue_closed_customer_owner
,notify_issue_closed_project_owner
,notify_issue_created_customer_owner
,notify_issue_created_project_owner
,notify_sr_submitted_project_owner
,ta_include
,rm_approvalprocess
,rate_cardid
,pm_approver_3
,picklist_label
,user_filter
,category_filter
,timetype_filter
,payroll_type_filter
,te_include_rate_cardid
,notes_required_on_ts__c
,proj_allow_expenses__c
,proj_allow_time_entry__c
,netsuite_project_id__c
,proj_parentproj__c
,netsuite_project_expenses__c
,proj_parentproj_id__c
,proj_billtype__c
,proj_revtype__c
,proj_enddate__c
,proj_lookupTest__c
,proj_customer_ns_id__c
,project_type__c
,project_program__c
,project_subsidary_id__c
,proj_program_id__c
,prj_overall_status__c
,proj_EACcost__c
,proj_CMT__c
,ns_percent_complete__c
,ns_eac_value__c
,expected_close__c
,proj_DWObudget__c
,proj_entityid__c
,proj_businessarea__c
,proj_MRU__c
,proj_LCCI__c
,proj_ProfitCenter__c
,proj_functionalarea__c
,proj_businessareaID__c
,proj_functionalareaID__c
,proj_MRUID__c
,proj_LCCIID__c
,proj_ProfitCenterID__c
,proj_prepaid__c
,proj_project_type_labor_costing__c
,proj_Deliverymanager__c
,proj_legacy_id__c
,project_clientprincipal__c
,proj_region__c
,project_closed__c
,proj_market_offering__c
,proj_service_offering__c
,prog_stage__c
,project_id__c
,program_id__c
,proj_initiateforecast__c
,proj_ent_id__c
,skip_task_recalc__c
,eac_forecast_timestamp__c
,eac_forecast_fail_reason__c
,eac_forecast_check__c
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".NO_Project WHERE id in
(SELECT STG.id FROM NO_Project_stg_Tmp_Key STG JOIN NO_Project_base_Tmp
ON STG.id = NO_Project_base_Tmp.id AND STG.updated >= NO_Project_base_Tmp.updated);

/* Deleting before seven days data from current date in the Historical Table */

delete /*+DIRECT*/ from "swt_rpt_stg"."NO_Project_Hist"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;


/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".NO_Project WHERE id in
(SELECT STG.id FROM NO_Project_stg_Tmp_Key STG JOIN NO_Project_base_Tmp
ON STG.id = NO_Project_base_Tmp.id AND STG.updated >= NO_Project_base_Tmp.updated);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".NO_Project
(
id
,auto_bill
,az_approver
,message
,po_approver
,auto_bill_cap
,invoice_layoutid
,rate
,notify_assignees
,sync_workspace
,notify_owner
,br_approvalprocess
,te_approver
,ta_approvalprocess
,te_approvalprocess
,auto_bill_cap_value
,updated
,code
,tb_approver
,tax_location_name
,active
,name
,hierarchy_node_ids
,externalid
,po_approvalprocess
,project_stageid
,tax_locationid
,ta_approver
,pr_approver
,locationid
,finish_date
,msp_link_type
,customerid
,customer_name
,only_owner_can_edit
,br_approver
,userid
,az_approvalprocess
,project_locationid
,budget
,currency
,cost_centerid
,sga_labor
,invoice_text
,budget_time
,start_date
,pr_approvalprocess
,billing_contactid
,billing_code
,created
,no_dirty
,create_workspace
,tb_approvalprocess
,auto_bill_override
,template_project_id
,copy_revenue_recognition_rules
,copy_revenue_recognition_auto_settings
,copy_project_billing_rules
,copy_project_billing_auto_settings
,copy_project_pricing
,copy_custom_fields
,copy_loaded_cost
,copy_approvers
,copy_issues
,copy_notification_settings
,copy_dashboard_settings
,copy_invoice_layout_settings
,pm_approver_1
,pm_approver_2
,shipping_contact_id
,sold_to_contact_id
,attachmentid
,rv_approver
,rv_approvalprocess
,portfolio_projectid
,is_portfolio_project
,copy_revenuerecognition_auto_settings
,filterset_ids
,notify_issue_assigned_to
,notify_issue_closed_assigned_to
,notify_issue_closed_customer_owner
,notify_issue_closed_project_owner
,notify_issue_created_customer_owner
,notify_issue_created_project_owner
,notify_sr_submitted_project_owner
,ta_include
,rm_approvalprocess
,rate_cardid
,pm_approver_3
,picklist_label
,user_filter
,category_filter
,timetype_filter
,payroll_type_filter
,te_include_rate_cardid
,notes_required_on_ts__c
,proj_allow_expenses__c
,proj_allow_time_entry__c
,netsuite_project_id__c
,proj_parentproj__c
,netsuite_project_expenses__c
,proj_parentproj_id__c
,proj_billtype__c
,proj_revtype__c
,proj_enddate__c
,proj_lookupTest__c
,proj_customer_ns_id__c
,project_type__c
,project_program__c
,project_subsidary_id__c
,proj_program_id__c
,prj_overall_status__c
,proj_EACcost__c
,proj_CMT__c
,ns_percent_complete__c
,ns_eac_value__c
,expected_close__c
,proj_DWObudget__c
,proj_entityid__c
,proj_businessarea__c
,proj_MRU__c
,proj_LCCI__c
,proj_ProfitCenter__c
,proj_functionalarea__c
,proj_businessareaID__c
,proj_functionalareaID__c
,proj_MRUID__c
,proj_LCCIID__c
,proj_ProfitCenterID__c
,proj_prepaid__c
,proj_project_type_labor_costing__c
,proj_Deliverymanager__c
,proj_legacy_id__c
,project_clientprincipal__c
,proj_region__c
,project_closed__c
,proj_market_offering__c
,proj_service_offering__c
,prog_stage__c
,project_id__c
,program_id__c
,proj_initiateforecast__c
,proj_ent_id__c
,skip_task_recalc__c
,eac_forecast_timestamp__c
,eac_forecast_fail_reason__c
,eac_forecast_check__c
,SWT_INS_DT
)
SELECT DISTINCT
NO_Project_stg_Tmp.id
,auto_bill
,az_approver
,message
,po_approver
,auto_bill_cap
,invoice_layoutid
,rate
,notify_assignees
,sync_workspace
,notify_owner
,br_approvalprocess
,te_approver
,ta_approvalprocess
,te_approvalprocess
,auto_bill_cap_value
,NO_Project_stg_Tmp.updated
,code
,tb_approver
,tax_location_name
,active
,name
,hierarchy_node_ids
,externalid
,po_approvalprocess
,project_stageid
,tax_locationid
,ta_approver
,pr_approver
,locationid
,finish_date
,msp_link_type
,customerid
,customer_name
,only_owner_can_edit
,br_approver
,userid
,az_approvalprocess
,project_locationid
,budget
,currency
,cost_centerid
,sga_labor
,invoice_text
,budget_time
,start_date
,pr_approvalprocess
,billing_contactid
,billing_code
,created
,no_dirty
,create_workspace
,tb_approvalprocess
,auto_bill_override
,template_project_id
,copy_revenue_recognition_rules
,copy_revenue_recognition_auto_settings
,copy_project_billing_rules
,copy_project_billing_auto_settings
,copy_project_pricing
,copy_custom_fields
,copy_loaded_cost
,copy_approvers
,copy_issues
,copy_notification_settings
,copy_dashboard_settings
,copy_invoice_layout_settings
,pm_approver_1
,pm_approver_2
,shipping_contact_id
,sold_to_contact_id
,attachmentid
,rv_approver
,rv_approvalprocess
,portfolio_projectid
,is_portfolio_project
,copy_revenuerecognition_auto_settings
,filterset_ids
,notify_issue_assigned_to
,notify_issue_closed_assigned_to
,notify_issue_closed_customer_owner
,notify_issue_closed_project_owner
,notify_issue_created_customer_owner
,notify_issue_created_project_owner
,notify_sr_submitted_project_owner
,ta_include
,rm_approvalprocess
,rate_cardid
,pm_approver_3
,picklist_label
,user_filter
,category_filter
,timetype_filter
,payroll_type_filter
,te_include_rate_cardid
,notes_required_on_ts__c
,proj_allow_expenses__c
,proj_allow_time_entry__c
,netsuite_project_id__c
,proj_parentproj__c
,netsuite_project_expenses__c
,proj_parentproj_id__c
,proj_billtype__c
,proj_revtype__c
,proj_enddate__c
,proj_lookupTest__c
,proj_customer_ns_id__c
,project_type__c
,project_program__c
,project_subsidary_id__c
,proj_program_id__c
,prj_overall_status__c
,proj_EACcost__c
,proj_CMT__c
,ns_percent_complete__c
,ns_eac_value__c
,expected_close__c
,proj_DWObudget__c
,proj_entityid__c
,proj_businessarea__c
,proj_MRU__c
,proj_LCCI__c
,proj_ProfitCenter__c
,proj_functionalarea__c
,proj_businessareaID__c
,proj_functionalareaID__c
,proj_MRUID__c
,proj_LCCIID__c
,proj_ProfitCenterID__c
,proj_prepaid__c
,proj_project_type_labor_costing__c
,proj_Deliverymanager__c
,proj_legacy_id__c
,project_clientprincipal__c
,proj_region__c
,project_closed__c
,proj_market_offering__c
,proj_service_offering__c
,prog_stage__c
,project_id__c
,program_id__c
,proj_initiateforecast__c
,proj_ent_id__c
,skip_task_recalc__c
,eac_forecast_timestamp__c
,eac_forecast_fail_reason__c
,eac_forecast_check__c
,SYSDATE AS SWT_INS_DT
FROM NO_Project_stg_Tmp JOIN NO_Project_stg_Tmp_Key ON NO_Project_stg_Tmp.id= NO_Project_stg_Tmp_Key.id AND NO_Project_stg_Tmp.updated=NO_Project_stg_Tmp_Key.updated
WHERE NOT EXISTS
(SELECT 1 FROM "swt_rpt_base".NO_Project BASE
WHERE NO_Project_stg_Tmp.id = BASE.id);


/* Deleting partial audit entry */

DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'NETSUITEOPENAIR' and
TBL_NM = 'NO_Project' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and 
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'NETSUITEOPENAIR' and  TBL_NM = 'NO_Project' and  COMPLTN_STAT = 'N');


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
select 'NETSUITEOPENAIR','NO_Project',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count(*) from "swt_rpt_stg"."NO_Project") ,(select count(*) from swt_rpt_base.NO_Project where SWT_INS_DT::date = sysdate::date),'Y';

Commit;


select do_tm_task('mergeout','swt_rpt_stg.NO_Project_Hist');
select do_tm_task('mergeout','swt_rpt_base.NO_Project');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
SELECT ANALYZE_STATISTICS('swt_rpt_base.NO_Project');
INSERT /*+Direct*/ INTO swt_rpt_stg.NO_Project_Hist SELECT * from swt_rpt_stg.NO_Project;
COMMIT;
TRUNCATE TABLE swt_rpt_stg.NO_Project;


