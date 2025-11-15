
-- probably better to get this from naviga informer, so may be deleted

with categories as (
    select 'Retail' as SMI_PRODUCT_CATEGORY,'Retail' AS IAB_CATEGORY, '2203 General' AS TM1_CATEGORY
    union all select 'Specialty Retailers' AS SMI_PRODUCT_CATEGORY, 'Retail' AS IAB_CATEGORY, '2203 General' AS TM1_CATEGORY
    union all select 'Airlines/Travel Agents' AS SMI_PRODUCT_CATEGORY, 'Travel and Accomodation' AS IAB_CATEGORY, '2402 Travel' AS TM1_CATEGORY
    union all select 'Government' AS SMI_PRODUCT_CATEGORY, 'Government Departments, Services and Communities' AS IAB_CATEGORY, '1601 Government' AS TM1_CATEGORY
    union all select 'Real Estate Agents/Developers' AS SMI_PRODUCT_CATEGORY, 'Real Estate' AS IAB_CATEGORY, '2101 Real Estate' AS TM1_CATEGORY
    union all select 'Banking' AS SMI_PRODUCT_CATEGORY, 'Investment, Finance and Banking' AS IAB_CATEGORY, '1401 Finance/Banking/Insurance' AS TM1_CATEGORY
    union all select 'Cosmetics/Toiletries/Personal Care' AS SMI_PRODUCT_CATEGORY, 'Health, Beauty and Pharmaceuticals' AS IAB_CATEGORY, '2204 Toiletries/Cosmetics' AS TM1_CATEGORY
    union all select 'Home Furnishings/Appliances' AS SMI_PRODUCT_CATEGORY, 'Home and Garden' AS IAB_CATEGORY, '1704 Household Furnishings' AS TM1_CATEGORY
    union all select 'Communications' AS SMI_PRODUCT_CATEGORY, 'Telecommunication' AS IAB_CATEGORY, '2301 Telecommunications' AS TM1_CATEGORY
    union all select 'Education' AS SMI_PRODUCT_CATEGORY, 'Government Departments, Services and Communities' AS IAB_CATEGORY, '1601 Government' AS TM1_CATEGORY
    union all select 'Building/Construction' AS SMI_PRODUCT_CATEGORY, 'Manufacturing and Construction' AS IAB_CATEGORY, '2101 Real Estate' AS TM1_CATEGORY
    union all select 'Tourism/Accommodation/Travel Services' AS SMI_PRODUCT_CATEGORY, 'Travel and Accomodation' AS IAB_CATEGORY, '2402 Travel' AS TM1_CATEGORY
    union all select 'Business Services/Industrial Services' AS SMI_PRODUCT_CATEGORY, 'Business and Professional/ Technical Services and Office Products' AS IAB_CATEGORY, '9103 Business Services' AS TM1_CATEGORY
    union all select 'Insurance' AS SMI_PRODUCT_CATEGORY, 'Insurance' AS IAB_CATEGORY, '1401 Finance/Banking/Insurance' AS TM1_CATEGORY
    union all select 'Other Financial Services' AS SMI_PRODUCT_CATEGORY, 'Investment, Finance and Banking' AS IAB_CATEGORY, '1401 Finance/Banking/Insurance' AS TM1_CATEGORY
    union all select 'Entertainment' AS SMI_PRODUCT_CATEGORY, 'Leisure, Entertainment and Media' AS IAB_CATEGORY, '1801 Leisure and Entertainment' AS TM1_CATEGORY
    union all select 'Food/Produce/Dairy' AS SMI_PRODUCT_CATEGORY, 'Food and Beverages' AS IAB_CATEGORY, '1500 Food' AS TM1_CATEGORY
    union all select 'Utilities/Fuel/Energy' AS SMI_PRODUCT_CATEGORY, 'Government Departments, Services and Communities' AS IAB_CATEGORY, '9107 Services/Communities/Utilities' AS TM1_CATEGORY
    union all select 'Health/Medical Products/Services' AS SMI_PRODUCT_CATEGORY, 'Health, Beauty and Pharmaceuticals' AS IAB_CATEGORY, '2001 Pharmaceuticals/Health' AS TM1_CATEGORY
    union all select 'Automotive Brand' AS SMI_PRODUCT_CATEGORY, 'Automotive' AS IAB_CATEGORY, '1101 Automotive' AS TM1_CATEGORY
    union all select 'Organisations/Associations' AS SMI_PRODUCT_CATEGORY, 'Government Departments, Services and Communities' AS IAB_CATEGORY, '9105 Miscellaneous' AS TM1_CATEGORY
    union all select 'Alcoholic Beverages' AS SMI_PRODUCT_CATEGORY, 'Food and Beverages' AS IAB_CATEGORY, '1502 Beverages' AS TM1_CATEGORY
    union all select 'Gardening/Agriculture' AS SMI_PRODUCT_CATEGORY, 'Agriculture, Forestry and Fishing' AS IAB_CATEGORY, '9102 Agricultural' AS TM1_CATEGORY
    union all select 'Media' AS SMI_PRODUCT_CATEGORY, 'Leisure, Entertainment and Media' AS IAB_CATEGORY, '1801 Leisure and Entertainment' AS TM1_CATEGORY
    union all select 'Auto Dealers/ Parts/Commercial' AS SMI_PRODUCT_CATEGORY, 'Automotive' AS IAB_CATEGORY, '1101 Automotive' AS TM1_CATEGORY
    union all select 'Computer/Software Companies' AS SMI_PRODUCT_CATEGORY, 'Computers' AS IAB_CATEGORY, '1201 Computers/IT' AS TM1_CATEGORY
    union all select 'Pharmaceuticals' AS SMI_PRODUCT_CATEGORY, 'Health, Beauty and Pharmaceuticals' AS IAB_CATEGORY, '2001 Pharmaceuticals/Health' AS TM1_CATEGORY
    union all select 'Clothing/Jewellery/Fashion Accessories' AS SMI_PRODUCT_CATEGORY, 'Retail' AS IAB_CATEGORY, '2201 Clothing' AS TM1_CATEGORY
    union all select 'Restaurants' AS SMI_PRODUCT_CATEGORY, 'Leisure, Entertainment and Media' AS IAB_CATEGORY, '1801 Leisure and Entertainment' AS TM1_CATEGORY
    union all select 'Consumer Electronics' AS SMI_PRODUCT_CATEGORY, 'Computers' AS IAB_CATEGORY, '1201 Computers/IT' AS TM1_CATEGORY
    union all select 'Motion Pictures/Cinemas' AS SMI_PRODUCT_CATEGORY, 'Leisure, Entertainment and Media' AS IAB_CATEGORY, '1801 Leisure and Entertainment' AS TM1_CATEGORY
    union all select 'Non-Alcoholic Beverages' AS SMI_PRODUCT_CATEGORY, 'Food and Beverages' AS IAB_CATEGORY, '1501 Foodhexas' AS TM1_CATEGORY
    union all select 'Household Supplies/Services' AS SMI_PRODUCT_CATEGORY, 'Home and Garden' AS IAB_CATEGORY, '1706 Household Items' AS TM1_CATEGORY
    union all select 'Sports Apparel/Recreation Equipment' AS SMI_PRODUCT_CATEGORY, 'Retail' AS IAB_CATEGORY, '2203 General' AS TM1_CATEGORY
    union all select 'Other/Miscellaneous' AS SMI_PRODUCT_CATEGORY, 'Other' AS IAB_CATEGORY, '9105 Miscellaneous' AS TM1_CATEGORY
    union all select 'Toys/Video Games' AS SMI_PRODUCT_CATEGORY, 'Retail' AS IAB_CATEGORY, '2203 General' AS TM1_CATEGORY
)

select * from categories


--
{#

Heya, so the data is already sitting in the DWH.
it is a view called [FFXDW].[dbo].[VW_Customer_Segmentation]
Fields you need are:
[AccountExternalAccountNumber] = SAP Number (Legacy ID)
[SegmentationIndustryType] = IndustryType_Salesforce (this is the one Linda has asked for)
You may also need:
[AccountIndustryType] = IndustryType_Genera

CREATE VIEW [dbo].[VW_Customer_Segmentation]
AS
SELECT CAST([AccountID] AS VARCHAR(10)) AS AccountID
      ,[Account]
	  ,AccountPrimaryAddress
	  ,AccountPrimaryAddressCity
	  ,AccountPrimaryAddressPostCode
	  ,AccountPrimaryPhone
	  ,CAST([AccountcloseDate] AS DATE) AS [AccountcloseDate]
      ,[AccountExternalAccountNumber]
	  ,[NationalAccountLevel1Name]
	  ,[NationalAccountLevel2Name]
	  ,[AccountIndustryType]
	  ,[AccountSalesRep]
      ,[SegmentationCustomerType]
      ,[SegmentationBusinessUnit]
      ,[SegmentationSalesRegion]
      ,RTRIM([SegmentationTerritoryCode]) AS [SegmentationTerritoryCode]
      ,[SegmentationTerritory]
      ,[SegmentationIsAutoFlag]
      ,[SegmentationIsRealEstateFlag]
      ,[SegmentationLongitude]
      ,[SegmentationLatitude]
      ,[SegmentationPointOfCoOrdinates]
      ,CASE WHEN ISNULL([SegmentationCustomerName], 'Unknown') = 'Unknown' THEN AccountCustomerName ELSE [SegmentationCustomerName] END AS [SegmentationCustomerName]
      ,[SegmentationSpendType]
      ,[SegmentationNetRevenue]
      ,[SegmentationIndustryType]
      ,[SegmentationClassifiedOnly]
	  ,[SegmentationAddressFinal]
	  ,[SegmentationPointID]
	  ,[SegmentationPOBoxFlag]
	  ,'DimAccount' AS RecordSource
	  ,[ActiveAccount]
FROM FFXDW.DBO.DimAccount

UNION

SELECT   'SEG' + CAST(ROW_Number()OVER(ORDER BY SegmentationCustomerName) AS VARCHAR(10)) AS AccountID
       , ISNULL([SegmentationAccountNo], -1) AS Account
	   ,NULL AS AccountPrimaryAddress
	   ,NULL AS AccountPrimaryAddressCity
	   ,NULL AS AccountPrimaryAddressPostCode
	   ,NULL AS AccountPrimaryPhone
	   , '9999-12-31' AS [AccountcloseDate]
       ,'' AS AccountExternalAccountNumber
	   ,'' AS NationalAccountLevel1Name
	   ,'' AS NationalAccountLevel2Name
	   ,'' AS AccountIndustryType
	   ,'' AS AccountSalesRep
       ,[SegmentationCustomerType]
       ,[SegmentationBusinessUnit]
       ,[SegmentationSalesRegion]
       ,RTRIM([SegmentationTerritoryCode]) AS [SegmentationTerritoryCode]
       ,[SegmentationTerritory]
       ,[SegmentationIsAutoFlag]
       ,[SegmentationIsRealEstateFlag]
       ,[SegmentationLongitude]
       ,[SegmentationLatitude]
       ,[SegmentationPointOfCoOrdinates]
       ,[SegmentationCustomerName]
       ,[SegmentationSpendType]
       ,[SegmentationNetRevenue]
       ,[SegmentationIndustryType] -- from salesforce
       ,[SegmentationClassifiedOnly]
	   ,[SegmentationAddressFinal]
	   ,[SegmentationPointID]
	   ,[SegmentationPOBoxFlag]
       ,'ODSActiveProspects' AS RecordSource
	   ,'' AS [ActiveAccount]
FROM FFXDW.DBO.ODSActiveProspects #}
