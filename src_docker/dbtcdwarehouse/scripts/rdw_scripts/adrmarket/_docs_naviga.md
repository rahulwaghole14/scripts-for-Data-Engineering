
alteryx workflow runs daily around 6:15am:
https://github.com/hexaNZ/hexa-data-alteryx-workflows/blob/main/src/naviga__informer_reports_to_rdw/Naviga_informer_report.yxmd

it will overwrite to the tables in rdw, it runs a python script to load a file into network drive and then uploads to rdw into the table:
odbc:DSN=PDC102|||ffxdw.dbo.NavigaTest

tables to use for TM1 view/cube (currently sitting in RDW FFXDW database):

we are currently investigating can we move this data into bigquery and integrate with dbt project, just waiting to hear back from fusion5 if this will work for them (have provided them with details on odbc driver for bigquery https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers). once that is ok to go ahead we can switch alteryx workflow to push data into bigquery from naviga instead of RDW

the data will be staged in bigquery into
dataset: adw_stage
  table: naviga__adrevenue
  table: naviga__adaccountmapping
  table: genera__adrevenue
  table: adbook__adrevenue
  table: showcaseturbo__adrevenue

```sql
-- historic data for genera (one off migration)
SELECT TOP (1000) * FROM [FFXDW].[dbo].[GeneraNavigaHistoric]
-- historic data for adbook (one off migration)
SELECT TOP (1000) * FROM [FFXDW].[dbo].[AdBookNavigaHistoric]
-- historic data for showcaseturbo (one off migration)
SELECT TOP (1000) * FROM [FFXDW].[dbo].[ShowcaseTurbo]
-- naviga new data table rdw (one off migration and switch to bigquery?)
SELECT TOP (1000) * FROM [FFXDW].[dbo].[NavigaTest]
-- naviga accounts to genera accounts mapping table (one off migration)
SELECT TOP (1000) * FROM [FFXDW].[dbo].[NavigaAccountMapping]
```
