we want to update external id in matrix with matched user_id (hexa_id) from idm drupal
currently it can match on sub_id as follows, or we can match on email

we want to write it once, and not update existing ids on change, only nulls.

```
select top(10) * from stage.idm.drupal__user_profiles where user_id = 11071717 -- subscriber_id 3262838 (matrix) -->
select top(10) * from stage.matrix.subscriber where subs_id = 3262838; -- subs_perid 2319017  -->
select top(10) * from stage.matrix.tbl_person where person_pointer = 2319017; -- confirmed same person
```
