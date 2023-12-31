# Доработка итогового задания
По результатам выполнения задания подтребовалос сделать пару доработок:
- разбить столбцец с именем и фамилией сотрудника на отдельные столбцы с именем и фамилией;
- сделать то же самое с именем и фамилией клиента. 

## Разбиение имени и фамилии сотрудника на отдельные столбцы
Добавляем в таблице ```staff``` два новых столбца и заполняем их значениями имени и фамилии. После этого 
удаляем старый столбец
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
## Разбиение имени и фамилии клиента на отдельные столбцы
Сложность разбиения в данном случае состоит в том, что поле ```cli_name``` таблице ```client``` является первичным 
ключем и участвует в foreign key constraint таблицы ```order_components```. Поэтому изменения будем выполнять следующим образом

1. Формируем новую таблицу ```client_new``` с целочисленным первичным ключом и отдельными полями для имени и фамилии
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
2. Добавляем в ```order_components``` столбцец ```cli_id``` и заполняем его идентификаторами из ```client_new```, соответствующими ```cli_name```
```sql
alter table order_components 
add column cli_id int8

update order_components oc
set cli_id = (select c.cli_id from client_new c where oc.cli_name = c.cli_name limit 1)
```
3. Снимаем ограничение внешнего ключа в таблице ```order_components``` для таблицы ```client```, подменяем ```client``` на ```client_new```,
а старый ```client``` удаляем, после чего устанавливаем ограничение внешнего ключа на связь с ```client``` по ```cli_id```, а поле ```cli_name``` удаляем
```sql
alter table order_components 
  drop constraint fk_components_client

drop table client  

alter table client_new
rename to client

alter table order_components 
  add constraint fk_components_client foreign key (cli_id) references client (cli_id)

alter table order_components 
  drop column cli_name
```
