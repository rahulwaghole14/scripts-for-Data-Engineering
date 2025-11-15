# matrix
matrix database print subscription, circulation management database

## table schema & alteryx behavior

<table>
<tr>
<td> Schema </td>
<td> Table </td>
<td> Primary Key(s) </td>
<td> record_source </td>
<td> hash_key </td>
<td> hash_diff </td>
<td> alteryx output </td>
<td> alteryx workflow </td>
</tr>

<tr>
<td> matrix </td>
<td> matrix_pub_pricing_increase_data </td>
<td> (not set) </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> (not set) </td>
<td> (not set) </td>
</tr>
<tr>
<td> matrix </td>
<td> active_subscribers_filter </td>
<td> (not set) </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> (not set) </td>
<td> (not set) </td>
</tr>
<tr>
<td> matrix </td>
<td> mosaicTypeProfile </td>
<td> (not set) </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> (not set) </td>
<td> (not set) </td>
</tr>
<tr>
<td> matrix </td>
<td> ffx_pricing_data </td>
<td> (not set) </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> overwrite table (drop) * </td>
<td> //PDCFILCL102/group$/FFX/Datateam\newspaper-perf\cdwstage_matrix_active_subscriber_all_weekly_refresh.yxmd </td>
</tr>
<tr>
<td> matrix </td>
<td> MosaicTypeProfileStatistic </td>
<td> (not set) </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> (not set) </td>
<td> (not set) </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_person </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+ toString([person_pointer]))
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>
<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_contact_number </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+ toString([ContactPointer]))
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_location </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+ toString([loc_pointer]))
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_country </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+[Country_Code])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> subscriber </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+[subs_id])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> subscription </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+[sord_id]+'|'+[ServiceID]+'|'+[ProductID]+'|'+[period_id])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_service </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII('MATRIX|'+[ServiceID])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>

<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> all_active_subscriber_all </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> overwrite table (drop) * </td>

<td> //PDCFILCL102/group$/FFX/Datateam\newspaper-perf\cdwstage_matrix_active_subscriber_all_weekly_refresh.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> customer </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
-- N/A
```

</td> <td>

```SQL
-- N/A
```

</td> <td> (not set) </td>

<td> updated by stored procedure here `scripts/stage/programmability/stored-procedures/matrix.load_vault.sql` </td>
</tr>

<tr>
<td> matrix </td>
<td> cancellation_reason </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII('MATRIX|'+[canx_id])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>
<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_product </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII('MATRIX|'+[ProductID])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>
<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> tbl_period </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII('MATRIX|'+[PeriodID])
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>
<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

<tr>
<td> matrix </td>
<td> subord_cancel </td>
<td> hash_key </td>
<td> 'MATRIX' </td>
<td>

```SQL
MD5_ASCII([record_source]+'|'+toString([ObjectPointer]))
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
```

</td> <td> delete data and append * </td>
<td> C:\projects\CDW_PROD\matrix_source_to_stage.yxmd </td>
</tr>

</table>

`*`  Delete Data & Append: Deletes all the original records from the table and then appends the data into the existing table. Note that this mode is different depending on the database you write to:
  - Oracle Databases: Uses DELETE statement.
  - SQL Server Databases: Uses TRUNCATE TABLE statement because this is a more efficient method. You need either the ALTER table or TRUNCATE permissions on the table.
  https://learn.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql?view=sql-server-ver16
