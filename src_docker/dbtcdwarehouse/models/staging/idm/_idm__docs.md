# idm
hexa website login management

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
<td> idm </td>
<td> user_profile </td> <!-- add another table row for drupal__user_profiles -->
<td> hash_key </td>
<td> 'IDM' </td>
<td>

```SQL
MD5_ASCII([record_source] + '|' + toString([user_id]))
-- https://help.alteryx.com/20223/designer/string-functions
```

</td> <td>

```SQL
MD5_ASCII([record_source] + '|' + concat_diff)
-- https://help.alteryx.com/20223/designer/string-functions
```

</td> <td> overwrite table (drop) * </td>
<td> C:\projects\CDW_PROD\idm_source_to_stage.yxmd </td>
</tr>
<tr>
<td> idm </td>
<td> drupal__user_profiles </td> <!-- add another table row for drupal__user_profiles -->
<td> user_id </td>
<td> 'IDM' </td>
<td>

```SQL
-- na
```

</td> <td>

```SQL
-- na
```

</td> <td> insert (new) and update (existing) (python script) </td>
<td> C:\projects\hexa-data-alteryx-workflows\src\idm_api__user_profiles_to_cdw\main.py </td>
</tr>
</table>

`*` Overwrite Table (Drop): Completely drops the existing table and creates a new one.
