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

формируем новую таблицу client_new с целочисленным первичным ключом и отдельными полями для имени и фамилии
```sql
select 
  row_number() over() as cli_id, 
  split_part(cli_name,' ',1) as cli_first_name,
  split_part(cli_name,' ',2) as cli_last_name,
  cli_email, 
  cli_phone, 
  cli_secret 
into client_new
from client 

alter table client_new 
  add primary key (cli_id)
```

добавляем в order_components столбцец cli_id и заполняем его идентификаторами из client_new, соответствующими cli_name
```sql
alter table order_components 
add column cli_id int8

update order_components oc
set cli_id = (select c.cli_id from client_new c where oc.cli_name = c.cli_name limit 1)
```

снимаем ограничение внешнего ключа в таблице order_components для таблицы client,
подменяем client на client_new, а старый client удаляем
```sql
alter table order_components 
  drop constraint fk_components_client

drop table client  

alter table client_new
rename to client

alter table order_components 
  add constraint fk_components_client foreign key (cli_id) references client (cli_id)
```
