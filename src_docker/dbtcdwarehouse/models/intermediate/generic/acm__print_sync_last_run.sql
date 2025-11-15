/*
 ACM Print data sync process is not being run.
 This table have been added to specify the last run timestamp of the sync process.
 Since ACM tables are being used to build print data for Braze profiles,
 we need to know last 'updated_at' for every row used on the process.
 That to avoid sending to Braze only the updated profiles.
 On first run, all the profiles will be considered updated,
 but on subsequent runs, the updated_at for the profile will be calculated
 based on other tables used on the process, but not the acm__print_data tables.
 */

{{ config(
    tags=['braze'],
    materialized='table'
) }}

SELECT TIMESTAMP '2024-11-27 00:00:00 UTC' AS last_run_dts
