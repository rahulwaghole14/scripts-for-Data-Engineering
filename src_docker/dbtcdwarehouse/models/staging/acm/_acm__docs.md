bigquery:

```sql
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.acm__print_blacklist_final` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.acm__print_data_blacklist` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.acm__print_data_generic` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.acm__print_generic_final` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.acm__print_subbenefit_final` LIMIT 1000;

SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.matrix__customer` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.matrix__subord_cancel` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.matrix__subscriber` LIMIT 1000;
SELECT * FROM `hexa-data-report-etl-prod.cdw_stage.matrix__subscription` LIMIT 1000;
```
