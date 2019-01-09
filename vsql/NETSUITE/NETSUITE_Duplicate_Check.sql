select 'NS_Accounting_books',count(*) from (select accounting_book_id,count(*) from swt_rpt_base.NS_Accounting_books group by accounting_book_id having count(*)>1)A;
select 'NS_Accounting_Periods',count(*) from (select accounting_period_id,fiscal_calendar_id,count(*) from swt_rpt_base.NS_Accounting_Periods group by accounting_period_id,fiscal_calendar_id having count(*)>1)A;
select 'NS_Accounts',count(*) from (select account_id,count(*) from swt_rpt_base.NS_Accounts group by account_id having count(*)>1)A;
select 'NS_Classes',count(*) from (select class_id,count(*) from swt_rpt_base.NS_Classes group by class_id having count(*)>1)A;
select 'NS_Cost_Center_Segment_Mapper',count(*) from (select cost_center_segment_mapper_id,count(*) from swt_rpt_base.NS_Cost_Center_Segment_Mapper group by cost_center_segment_mapper_id having count(*)>1)A;
select 'NS_Currencyrates',count(*) from (select currencyrate_id,count(*) from swt_rpt_base.NS_Currencyrates group by currencyrate_id having count(*)>1)A;
select 'NS_Customers',count(*) from (select customer_id,count(*) from swt_rpt_base.NS_Customers group by customer_id having count(*)>1)A;
select 'NS_Departments',count(*) from (select department_id,count(*) from swt_rpt_base.NS_Departments group by department_id having count(*)>1)A;
select 'NS_Employees',count(*) from (select employee_id,count(*) from swt_rpt_base.NS_Employees group by employee_id having count(*)>1)A;
select 'NS_Entity',count(*) from (select entity_id,count(*) from swt_rpt_base.NS_Entity group by entity_id having count(*)>1)A;
select 'NS_Expense_accounts',count(*) from (select expense_account_id,count(*) from swt_rpt_base.NS_Expense_accounts group by expense_account_id having count(*)>1)A;
select 'NS_Fam_Asset',count(*) from (select fam_asset_id,count(*) from swt_rpt_base.NS_Fam_Asset group by fam_asset_id having count(*)>1)A;
select 'NS_FAM_Alternate_Depreciation',count(*) from (select fam_alternate_depreciation_id,count(*) from swt_rpt_base.NS_FAM_Alternate_Depreciation group by fam_alternate_depreciation_id having count(*)>1)A;
select 'NS_FAM_Alternate_Methods',count(*) from (select fam_alternate_methods_id,count(*) from swt_rpt_base.NS_FAM_Alternate_Methods group by fam_alternate_methods_id having count(*)>1)A;
select 'NS_Fam_Asset_type',count(*) from (select fam_asset_type_id,count(*) from swt_rpt_base.NS_Fam_Asset_type group by fam_asset_type_id having count(*)>1)A;
select 'NS_FAM_Depreciation_History',count(*) from (select fam_depreciation_history_id,count(*) from swt_rpt_base.NS_FAM_Depreciation_History group by fam_depreciation_history_id having count(*)>1)A;
select 'NS_Finance_Owner_ID',count(*) from (select finance_owner_id_id,count(*) from swt_rpt_base.NS_Finance_Owner_ID group by finance_owner_id_id having count(*)>1)A;
select 'NS_Functional_Area',count(*) from (select functional_area_id,count(*) from swt_rpt_base.NS_Functional_Area group by functional_area_id having count(*)>1)A;
select 'NS_Global_Account_Map',count(*) from (select global_account_map_id,count(*) from swt_rpt_base.NS_Global_Account_Map group by global_account_map_id having count(*)>1)A;
select 'NS_HPE_Country__Code_Internal_ID',count(*) from (select hpe_country__code_internal__id,count(*) from swt_rpt_base.NS_HPE_Country__Code_Internal_ID group by hpe_country__code_internal__id having count(*)>1)A;
select 'NS_HPE_Fiscal_Periods_Text_To_Nu',count(*) from (select hpe_fiscal_periods_text_to__id,count(*) from swt_rpt_base.NS_HPE_Fiscal_Periods_Text_To_Nu group by hpe_fiscal_periods_text_to__id having count(*)>1)A;
select 'NS_HPE_Subsidiary_Codes',count(*) from (select hpe_subsidiary_codes_id,count(*) from swt_rpt_base.NS_HPE_Subsidiary_Codes group by hpe_subsidiary_codes_id having count(*)>1)A;
select 'NS_Income_Accounts',count(*) from (select income_account_id,count(*) from swt_rpt_base.NS_Income_Accounts group by income_account_id having count(*)>1)A;
select 'NS_Inventory_items',count(*) from (select item_id,count(*) from swt_rpt_base.NS_Inventory_items group by  item_id having count(*)>1)A;
select 'NS_Invoice_Remit_To',count(*) from (select invoice_remit_to_id,count(*) from swt_rpt_base.NS_Invoice_Remit_To group by  invoice_remit_to_id having count(*)>1)A;
select 'NS_Item_account_map',count(*) from (select item_account_map_id,count(*) from swt_rpt_base.NS_Item_account_map group by  item_account_map_id having count(*)>1)A;
select 'NS_Item_family',count(*) from (select item_family_id,count(*) from swt_rpt_base.NS_Item_family group by  item_family_id having count(*)>1)A;
select 'NS_Items',count(*) from (select item_id,count(*) from swt_rpt_base.NS_Items group by  item_id having count(*)>1)A;
select 'NS_LCCI',count(*) from (select lcci_id,count(*) from swt_rpt_base.NS_LCCI group by  lcci_id having count(*)>1)A;
select 'NS_Locations',count(*) from (select location_id,count(*) from swt_rpt_base.NS_Locations group by  location_id having count(*)>1)A;
select 'NS_MRU_Hierarchy_Level',count(*) from (select mru_hierarchy_level_id,count(*) from swt_rpt_base.NS_MRU_Hierarchy_Level group by  mru_hierarchy_level_id having count(*)>1)A;
select 'NS_Partners',count(*) from (select partner_id,count(*) from swt_rpt_base.NS_Partners group by  partner_id having count(*)>1)A;
select 'NS_Payment_terms',count(*) from (select payment_terms_id,count(*) from swt_rpt_base.NS_Payment_terms group by  payment_terms_id having count(*)>1)A;
select 'NS_Profit_center',count(*) from (select profit_center_id,count(*) from swt_rpt_base.NS_Profit_center group by  profit_center_id having count(*)>1)A;
select 'NS_Revenue_elements',count(*) from (select revenue_element_id,count(*) from swt_rpt_base.NS_Revenue_elements group by  revenue_element_id having count(*)>1)A;
select 'NS_Revenue_plans',count(*) from (select plan_id,count(*) from swt_rpt_base.NS_Revenue_plans group by  plan_id having count(*)>1)A;
select 'NS_Revenue_recognition_rules',count(*) from (select rev_rec_rule_id,count(*) from swt_rpt_base.NS_Revenue_recognition_rules group by  rev_rec_rule_id having count(*)>1)A;
select 'NS_Subsidiaries',count(*) from (select subsidiary_id,count(*) from swt_rpt_base.NS_Subsidiaries group by  subsidiary_id having count(*)>1)A;
select 'NS_Tax_items',count(*) from (select item_id,count(*) from swt_rpt_base.NS_Tax_items group by  item_id having count(*)>1)A;
select 'NS_Transaction_address',count(*) from (select transaction_address_id,transaction_id,count(*) from swt_rpt_base.NS_Transaction_address group by  transaction_address_id,transaction_id having count(*)>1)A;
select 'NS_Transaction_Lines',count(*) from (select transaction_line_id,transaction_id,count(*) from swt_rpt_base.NS_Transaction_Lines group by  transaction_line_id,transaction_id having count(*)>1)A;
select 'NS_Transaction_Links',count(*) from (select original_transaction_id ,applied_transaction_id ,applied_transaction_line_id ,original_transaction_line_id ,inventory_number,link_type,count(*) from swt_rpt_base.NS_Transaction_Links group by  original_transaction_id ,applied_transaction_id ,applied_transaction_line_id ,original_transaction_line_id ,inventory_number,link_type having count(*)>1)A;
select 'NS_Transactions',count(*) from (select transaction_id,count(*) from swt_rpt_base.NS_Transactions group by  transaction_id having count(*)>1)A;
select 'NS_Transaction_lines_USD_Extract',count(*) from (select Transaction_internalid,Transaction_lineid,Period_internalid,count(*) from swt_rpt_base.NS_Transaction_lines_USD_Extract group by  Transaction_internalid,Transaction_lineid,Period_internalid  having count(*)>1)A;
select 'NS_Vendors',count(*) from (select vendor_id,count(*) from swt_rpt_base.NS_Vendors group by  vendor_id having count(*)>1)A;
