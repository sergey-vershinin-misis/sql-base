```sql
alter table staff 
add column staff_first_name varchar(30)


alter table staff  
add column staff_last_name varchar(30)

update staff 
set 
  staff_first_name = split_part(staf_name,' ',1),  
  staff_last_name = split_part(staf_name,' ',2)

alter table staff 
drop column staf_name
```

```sql

```
