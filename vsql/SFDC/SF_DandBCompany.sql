/****
****Script Name   : SF_DandBCompany.sql
****Description   : Incremental data load for SF_DandBCompany
****/


/*Setting timing on */
\timing

/**SET SESSION AUTOCOMMIT TO OFF;**/

\set ON_ERROR_STOP on

CREATE LOCAL TEMP TABLE Start_Time_Tmp ON COMMIT PRESERVE ROWS AS select count(*) count, sysdate st from "swt_rpt_stg"."SF_DandBCompany";

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
select 'SFDC','SF_DandBCompany',sysdate::date,sysdate,null,(select count from Start_Time_Tmp) ,null,'N';

Commit;


INSERT /*+Direct*/ INTO swt_rpt_stg.SF_DandBCompany_Hist SELECT * from swt_rpt_stg.SF_DandBCompany;
COMMIT;


CREATE LOCAL TEMP TABLE duplicates_records ON COMMIT PRESERVE ROWS AS (
select id,max(auto_id) as auto_id from swt_rpt_stg.SF_DandBCompany where id in (
select id from swt_rpt_stg.SF_DandBCompany group by id,LASTMODIFIEDDATE having count(1)>1)
group by id);
delete from swt_rpt_stg.SF_DandBCompany where exists(
select 1 from duplicates_records t2 where swt_rpt_stg.SF_DandBCompany.id=t2.id and swt_rpt_stg.SF_DandBCompany.auto_id<t2.auto_id);

Commit; 

CREATE LOCAL TEMP TABLE SF_DandBCompany_stg_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT * FROM swt_rpt_stg.SF_DandBCompany)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


TRUNCATE TABLE swt_rpt_stg.SF_DandBCompany;

CREATE LOCAL TEMP TABLE SF_DandBCompany_base_Tmp ON COMMIT PRESERVE ROWS AS
(
SELECT DISTINCT ID,LastModifiedDate FROM swt_rpt_base.SF_DandBCompany)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


CREATE LOCAL TEMP TABLE SF_DandBCompany_stg_Tmp_Key ON COMMIT PRESERVE ROWS AS
(
SELECT id, max(LastModifiedDate) as LastModifiedDate FROM SF_DandBCompany_stg_Tmp group by id)
SEGMENTED BY HASH(ID,LastModifiedDate) ALL NODES;


/* Inserting Stage table data into Historical Table */

insert /*+DIRECT*/ into swt_rpt_stg.SF_DandBCompany_Hist
(
SalesTurnoverGrowthRate
,SalesVolume
,SalesVolumeReliability
,Description
,CurrencyIsoCode
,MarketingPreScreen
,DomesticUltimateBusinessName
,DomesticUltimateDunsNumber
,DunsNumber
,EmployeeQuantityGrowthRate
,Fax
,FifthNaics
,FifthNaicsDesc
,FifthSic8
,FifthSic8Desc
,FifthSic
,FifthSicDesc
,TradeStyle5
,FipsMsaCode
,FipsMsaDesc
,FortuneRank
,FourthNaics
,FourthNaicsDesc
,FourthSic8
,FourthSic8Desc
,FourthSic
,FourthSicDesc
,TradeStyle4
,GeoCodeAccuracy
,GlobalUltimateBusinessName
,GlobalUltimateDunsNumber
,ImportExportAgent
,CountryAccessCode
,Latitude
,LegalStatus
,CurrencyCode
,CompanyCurrencyIsoCode
,OwnOrRent
,PremisesMeasure
,PremisesMeasureReliability
,PremisesMeasureUnit
,LocationStatus
,Longitude
,MailingAddress
,MarketingSegmentationCluster
,MinorityOwned
,NationalId
,NationalIdType
,FamilyMembers
,GlobalUltimateTotalEmployees
,EmployeesHere
,EmployeesHereReliability
,EmployeesTotal
,EmployeesTotalReliability
,OutOfBusiness
,PublicIndicator
,ParentOrHqBusinessName
,ParentOrHqDunsNumber
,Address
,Name
,PrimaryNaics
,PrimaryNaicsDesc
,PrimarySic8
,PrimarySic8Desc
,PrimarySic
,PrimarySicDesc
,TradeStyle1
,PriorYearEmployees
,PriorYearRevenue
,IncludedInSnP500
,SecondNaics
,SecondNaicsDesc
,SecondSic8
,SecondSic8Desc
,SecondSic
,SecondSicDesc
,TradeStyle2
,SixthNaics
,SixthNaicsDesc
,SixthSic8
,SixthSic8Desc
,SixthSic
,SixthSicDesc
,SmallBusiness
,StockExchange
,Subsidiary
,Phone
,ThirdNaics
,ThirdNaicsDesc
,ThirdSic8
,ThirdSic8Desc
,ThirdSic
,ThirdSicDesc
,TradeStyle3
,StockSymbol
,URL
,UsTaxId
,WomenOwned
,YearStarted
,Id
,LastModifiedDate
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedById
,SystemModstamp
,Street
,City
,State
,PostalCode
,Country
,GeocodeAccuracyStandard
,MailingStreet
,MailingCity
,MailingState
,MailingPostalCode
,MailingCountry
,MailingGeocodeAccuracy
,LD_DT
,SWT_INS_DT
,d_source
)
select
SalesTurnoverGrowthRate
,SalesVolume
,SalesVolumeReliability
,Description
,CurrencyIsoCode
,MarketingPreScreen
,DomesticUltimateBusinessName
,DomesticUltimateDunsNumber
,DunsNumber
,EmployeeQuantityGrowthRate
,Fax
,FifthNaics
,FifthNaicsDesc
,FifthSic8
,FifthSic8Desc
,FifthSic
,FifthSicDesc
,TradeStyle5
,FipsMsaCode
,FipsMsaDesc
,FortuneRank
,FourthNaics
,FourthNaicsDesc
,FourthSic8
,FourthSic8Desc
,FourthSic
,FourthSicDesc
,TradeStyle4
,GeoCodeAccuracy
,GlobalUltimateBusinessName
,GlobalUltimateDunsNumber
,ImportExportAgent
,CountryAccessCode
,Latitude
,LegalStatus
,CurrencyCode
,CompanyCurrencyIsoCode
,OwnOrRent
,PremisesMeasure
,PremisesMeasureReliability
,PremisesMeasureUnit
,LocationStatus
,Longitude
,MailingAddress
,MarketingSegmentationCluster
,MinorityOwned
,NationalId
,NationalIdType
,FamilyMembers
,GlobalUltimateTotalEmployees
,EmployeesHere
,EmployeesHereReliability
,EmployeesTotal
,EmployeesTotalReliability
,OutOfBusiness
,PublicIndicator
,ParentOrHqBusinessName
,ParentOrHqDunsNumber
,Address
,Name
,PrimaryNaics
,PrimaryNaicsDesc
,PrimarySic8
,PrimarySic8Desc
,PrimarySic
,PrimarySicDesc
,TradeStyle1
,PriorYearEmployees
,PriorYearRevenue
,IncludedInSnP500
,SecondNaics
,SecondNaicsDesc
,SecondSic8
,SecondSic8Desc
,SecondSic
,SecondSicDesc
,TradeStyle2
,SixthNaics
,SixthNaicsDesc
,SixthSic8
,SixthSic8Desc
,SixthSic
,SixthSicDesc
,SmallBusiness
,StockExchange
,Subsidiary
,Phone
,ThirdNaics
,ThirdNaicsDesc
,ThirdSic8
,ThirdSic8Desc
,ThirdSic
,ThirdSicDesc
,TradeStyle3
,StockSymbol
,URL
,UsTaxId
,WomenOwned
,YearStarted
,Id
,LastModifiedDate
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedById
,SystemModstamp
,Street
,City
,State
,PostalCode
,Country
,GeocodeAccuracyStandard
,MailingStreet
,MailingCity
,MailingState
,MailingPostalCode
,MailingCountry
,MailingGeocodeAccuracy
,SYSDATE AS LD_DT
,SWT_INS_DT
,'base'
FROM "swt_rpt_base".SF_DandBCompany WHERE id in
(SELECT STG.id FROM SF_DandBCompany_stg_Tmp_Key STG JOIN SF_DandBCompany_base_Tmp
ON STG.id = SF_DandBCompany_base_Tmp.id AND STG.LastModifiedDate >= SF_DandBCompany_base_Tmp.LastModifiedDate);

/* Deleting before seven days data from current date in the Historical Table */

/*delete /*+DIRECT*/ from "swt_rpt_stg"."SF_DandBCompany_HIST"  where LD_DT::date <= TIMESTAMPADD(DAY,-7,sysdate)::date;*/

/* Incremental VSQL script for loading data from Stage to Base */

DELETE /*+DIRECT*/ FROM "swt_rpt_base".SF_DandBCompany WHERE id in
(SELECT STG.id FROM SF_DandBCompany_stg_Tmp_Key STG JOIN SF_DandBCompany_base_Tmp
ON STG.id = SF_DandBCompany_base_Tmp.id AND STG.LastModifiedDate >= SF_DandBCompany_base_Tmp.LastModifiedDate);

INSERT /*+DIRECT*/ INTO "swt_rpt_base".SF_DandBCompany
(
SalesTurnoverGrowthRate
,SalesVolume
,SalesVolumeReliability
,Description
,CurrencyIsoCode
,MarketingPreScreen
,DomesticUltimateBusinessName
,DomesticUltimateDunsNumber
,DunsNumber
,EmployeeQuantityGrowthRate
,Fax
,FifthNaics
,FifthNaicsDesc
,FifthSic8
,FifthSic8Desc
,FifthSic
,FifthSicDesc
,TradeStyle5
,FipsMsaCode
,FipsMsaDesc
,FortuneRank
,FourthNaics
,FourthNaicsDesc
,FourthSic8
,FourthSic8Desc
,FourthSic
,FourthSicDesc
,TradeStyle4
,GeoCodeAccuracy
,GlobalUltimateBusinessName
,GlobalUltimateDunsNumber
,ImportExportAgent
,CountryAccessCode
,Latitude
,LegalStatus
,CurrencyCode
,CompanyCurrencyIsoCode
,OwnOrRent
,PremisesMeasure
,PremisesMeasureReliability
,PremisesMeasureUnit
,LocationStatus
,Longitude
,MailingAddress
,MarketingSegmentationCluster
,MinorityOwned
,NationalId
,NationalIdType
,FamilyMembers
,GlobalUltimateTotalEmployees
,EmployeesHere
,EmployeesHereReliability
,EmployeesTotal
,EmployeesTotalReliability
,OutOfBusiness
,PublicIndicator
,ParentOrHqBusinessName
,ParentOrHqDunsNumber
,Address
,Name
,PrimaryNaics
,PrimaryNaicsDesc
,PrimarySic8
,PrimarySic8Desc
,PrimarySic
,PrimarySicDesc
,TradeStyle1
,PriorYearEmployees
,PriorYearRevenue
,IncludedInSnP500
,SecondNaics
,SecondNaicsDesc
,SecondSic8
,SecondSic8Desc
,SecondSic
,SecondSicDesc
,TradeStyle2
,SixthNaics
,SixthNaicsDesc
,SixthSic8
,SixthSic8Desc
,SixthSic
,SixthSicDesc
,SmallBusiness
,StockExchange
,Subsidiary
,Phone
,ThirdNaics
,ThirdNaicsDesc
,ThirdSic8
,ThirdSic8Desc
,ThirdSic
,ThirdSicDesc
,TradeStyle3
,StockSymbol
,URL
,UsTaxId
,WomenOwned
,YearStarted
,Id
,LastModifiedDate
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedById
,SystemModstamp
,Street
,City
,State
,PostalCode
,Country
,GeocodeAccuracyStandard
,MailingStreet
,MailingCity
,MailingState
,MailingPostalCode
,MailingCountry
,MailingGeocodeAccuracy
,SWT_INS_DT
)
SELECT DISTINCT
SalesTurnoverGrowthRate
,SalesVolume
,SalesVolumeReliability
,Description
,CurrencyIsoCode
,MarketingPreScreen
,DomesticUltimateBusinessName
,DomesticUltimateDunsNumber
,DunsNumber
,EmployeeQuantityGrowthRate
,Fax
,FifthNaics
,FifthNaicsDesc
,FifthSic8
,FifthSic8Desc
,FifthSic
,FifthSicDesc
,TradeStyle5
,FipsMsaCode
,FipsMsaDesc
,FortuneRank
,FourthNaics
,FourthNaicsDesc
,FourthSic8
,FourthSic8Desc
,FourthSic
,FourthSicDesc
,TradeStyle4
,GeoCodeAccuracy
,GlobalUltimateBusinessName
,GlobalUltimateDunsNumber
,ImportExportAgent
,CountryAccessCode
,Latitude
,LegalStatus
,CurrencyCode
,CompanyCurrencyIsoCode
,OwnOrRent
,PremisesMeasure
,PremisesMeasureReliability
,PremisesMeasureUnit
,LocationStatus
,Longitude
,MailingAddress
,MarketingSegmentationCluster
,MinorityOwned
,NationalId
,NationalIdType
,FamilyMembers
,GlobalUltimateTotalEmployees
,EmployeesHere
,EmployeesHereReliability
,EmployeesTotal
,EmployeesTotalReliability
,OutOfBusiness
,PublicIndicator
,ParentOrHqBusinessName
,ParentOrHqDunsNumber
,Address
,Name
,PrimaryNaics
,PrimaryNaicsDesc
,PrimarySic8
,PrimarySic8Desc
,PrimarySic
,PrimarySicDesc
,TradeStyle1
,PriorYearEmployees
,PriorYearRevenue
,IncludedInSnP500
,SecondNaics
,SecondNaicsDesc
,SecondSic8
,SecondSic8Desc
,SecondSic
,SecondSicDesc
,TradeStyle2
,SixthNaics
,SixthNaicsDesc
,SixthSic8
,SixthSic8Desc
,SixthSic
,SixthSicDesc
,SmallBusiness
,StockExchange
,Subsidiary
,Phone
,ThirdNaics
,ThirdNaicsDesc
,ThirdSic8
,ThirdSic8Desc
,ThirdSic
,ThirdSicDesc
,TradeStyle3
,StockSymbol
,URL
,UsTaxId
,WomenOwned
,YearStarted
,SF_DandBCompany_stg_Tmp.Id
,SF_DandBCompany_stg_Tmp.LastModifiedDate
,IsDeleted
,CreatedDate
,CreatedById
,LastModifiedById
,SystemModstamp
,Street
,City
,State
,PostalCode
,Country
,GeocodeAccuracyStandard
,MailingStreet
,MailingCity
,MailingState
,MailingPostalCode
,MailingCountry
,MailingGeocodeAccuracy
,SYSDATE AS SWT_INS_DT
FROM SF_DandBCompany_stg_Tmp JOIN SF_DandBCompany_stg_Tmp_Key ON SF_DandBCompany_stg_Tmp.id= SF_DandBCompany_stg_Tmp_Key.id AND SF_DandBCompany_stg_Tmp.LastModifiedDate=SF_DandBCompany_stg_Tmp_Key.LastModifiedDate
WHERE NOT EXISTS
(SELECT 1 from "swt_rpt_base".SF_DandBCompany BASE
WHERE SF_DandBCompany_stg_Tmp.id = BASE.id);



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
select 'SFDC','SF_DandBCompany',sysdate::date,(select st from Start_Time_Tmp),sysdate,(select count from Start_Time_Tmp) ,(select count(*) from swt_rpt_base.SF_DandBCompany where SWT_INS_DT::date = sysdate::date),'Y';

Commit;

/* Deleting partial audit entry */
/*
DELETE FROM swt_rpt_stg.FAST_LD_AUDT where SUBJECT_AREA = 'SFDC' and
TBL_NM = 'SF_DandBCompany' and
COMPLTN_STAT = 'N' and
LD_DT=sysdate::date and
SEQ_ID = (select max(SEQ_ID) from swt_rpt_stg.FAST_LD_AUDT where  SUBJECT_AREA = 'SFDC' and  TBL_NM = 'SF_DandBCompany' and  COMPLTN_STAT = 'N');

commit;
*/

SELECT DROP_PARTITION('swt_rpt_stg.SF_DandBCompany_Hist', TIMESTAMPADD(day,-7,getdate())::date);
/*select do_tm_task('mergeout','swt_rpt_stg.SF_DandBCompany_Hist');*/
select do_tm_task('mergeout','swt_rpt_base.SF_DandBCompany');
select do_tm_task('mergeout','swt_rpt_stg.FAST_LD_AUDT');
select ANALYZE_STATISTICS('swt_rpt_base.SF_DandBCompany');



