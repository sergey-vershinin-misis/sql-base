# Итоговое задание по предмету "Базы данных и SQL"
Репозиторий содержит: 
- [дамп итоговой БД](https://github.com/sergey-vershinin-misis/sql-base/blob/main/orders-db-dump-default) (сделан на PostgreSQL 16 с настройками по-умолчанию),
- [дамп итоговой БД в виде простых SQL запросов](https://github.com/sergey-vershinin-misis/sql-base/blob/main/orders-db-dump-plain.sql),
- [SQL-скрипты](https://github.com/sergey-vershinin-misis/sql-base/blob/main/task.sql), использовавшиеся для приведение исходного набора данных к итоговому,
- настоящее описание процесса решения задачи.

## Описание решения
### 1. Создание таблицы и импорт данных из файла csv
Сначала создаем таблицу для загрузки набора данных с помощью следующего запроса (размерности целочисленных типов и длины для 
varchar взяты на основе максимальных значений в csv-файле, но с небольшим запасом):
```sql
CREATE TABLE sample (
	staf_name varchar(30),
	staff_age smallint,
	staff_id varchar(10),
	staff_lang varchar(30),
	order_pk smallint, 
	order_address varchar(100),
	order_country varchar(50),
	order_company varchar(100),
	order_price numeric(10,2),
	order_dt timestamp,
	order_list varchar(100)[],
	cli_name varchar(30),
	cli_email varchar(100),
	cli_phone varchar(30),
	cli_secret varchar(30),
	c_token varchar(100),
	c_pin smallint,
	c_gen varchar(30),
	c_type varchar(50)
)
```

После этого загружаем csv-файл в созданную таблицу:
```sql
COPY sample
FROM '<data-folder>\sample_0.csv'
DELIMITER ',' CSV HEADER
```
И проверяем, что в таблицу были загружены все записи (в файле их 200000): 
```sql
select count(*) from sample
```

### 2. Заполнение ячеек с NULL-значениями
Заполнение ячеек, в которых значения отсутствуют, выполняем в два этапа. Сначала копируем данные из sample в sample_unnulled1, заменяя в столбцах, похожих на первичные ключи сущностей, null на значения (*все изменения в исходной таблице с данными будем делать не через `update`, а путем копирования данных в новую таблицу через `select into` с одновременным их изменением*) 

Для заполнения поля ищем в таблице другие строки, имеющие в связанных полях те же значения. 
```sql
select 
   coalesce (s.staf_name, (select inner_s.staf_name from sample inner_s where inner_s.staf_name is not null and inner_s.staff_id = s.staff_id limit 1)) as staf_name, 
   s.staff_age, s.staff_id, s.staff_lang, 
   
   coalesce (s.order_pk, 
      (select inner_s.order_pk from sample inner_s where inner_s.order_pk is not null and inner_s.order_list = s.order_list limit 1), 
      (select inner_s.order_pk from sample inner_s where inner_s.order_pk is not null and inner_s.order_address = s.order_address and inner_s.order_country = s.order_country and inner_s.order_company = s.order_company limit 1)) as order_pk,
   s.order_address, s.order_country, s.order_company, s.order_price, s.order_dt, s.order_list,
   
   coalesce (s.cli_name, (select inner_s.cli_name from sample inner_s where inner_s.cli_name is not null and inner_s.cli_email = s.cli_email limit 1)) as cli_name,
   s.cli_email, s.cli_phone, s.cli_secret, 
   
   coalesce (s.c_token, 
       (select inner_s.c_token from sample inner_s where inner_s.c_token is not null and inner_s.c_pin = s.c_pin limit 1),
       (concat('generated_', cast (c_pin as varchar)))) as c_token,
   s.c_pin, s.c_gen, s.c_type
into sample_unnulled1
from sample s
```
После этого копируем данные из sample_unnulled1 в sample_unnulled2, заменяя null на соответствующие значения в для остальных столбцов таблицы
```sql
select 
   s.staf_name, 
   coalesce (s.staff_age, (select inner_s.staff_age from sample_unnulled2 inner_s where inner_s.staff_age is not null and inner_s.staf_name = s.staf_name limit 1)) as staff_age, 
   coalesce (s.staff_id, (select inner_s.staff_id from sample_unnulled2 inner_s where inner_s.staff_id is not null and inner_s.staf_name = s.staf_name limit 1)) as staff_id, 
   coalesce (s.staff_lang, (select inner_s.staff_lang from sample_unnulled2 inner_s where inner_s.staff_lang is not null and inner_s.staf_name = s.staf_name limit 1)) as staff_lang,
   
   s.order_pk, 
   coalesce (s.order_address, (select inner_s.order_address from sample_unnulled2 inner_s where inner_s.order_address is not null and inner_s.order_pk = s.order_pk limit 1)) as order_address,
   coalesce (s.order_country, (select inner_s.order_country from sample_unnulled2 inner_s where inner_s.order_country is not null and inner_s.order_pk = s.order_pk limit 1)) as order_country,
   coalesce (s.order_company, (select inner_s.order_company from sample_unnulled2 inner_s where inner_s.order_company is not null and inner_s.order_pk = s.order_pk limit 1)) as order_company,
   coalesce (s.order_price, (select inner_s.order_price from sample_unnulled2 inner_s where inner_s.order_price is not null and inner_s.order_pk = s.order_pk limit 1)) as order_price,
   coalesce (s.order_dt, (select inner_s.order_dt from sample_unnulled2 inner_s where inner_s.order_dt is not null and inner_s.order_pk = s.order_pk limit 1)) as order_dt,
   coalesce (s.order_list, (select inner_s.order_list from sample_unnulled2 inner_s where inner_s.order_list is not null and inner_s.order_pk = s.order_pk limit 1)) as order_list,
   
   s.cli_name,
   coalesce (s.cli_email, (select inner_s.cli_email from sample_unnulled2 inner_s where inner_s.cli_email is not null and inner_s.cli_name = s.cli_name limit 1)) as cli_email,
   coalesce (s.cli_phone, (select inner_s.cli_phone from sample_unnulled2 inner_s where inner_s.cli_phone is not null and inner_s.cli_name = s.cli_name limit 1)) as cli_phone,
   coalesce (s.cli_secret, (select inner_s.cli_secret from sample_unnulled2 inner_s where inner_s.cli_secret is not null and inner_s.cli_name = s.cli_name limit 1)) as cli_secret,
   
   s.c_token, 
   coalesce (s.c_pin, (select inner_s.c_pin from sample_unnulled2 inner_s where inner_s.c_pin is not null and inner_s.c_token = s.c_token limit 1)) as c_pin,
   coalesce (s.c_gen, (select inner_s.c_gen from sample_unnulled2 inner_s where inner_s.c_gen is not null and inner_s.c_token = s.c_token limit 1)) as c_gen,
   coalesce (s.c_type, (select inner_s.c_type from sample_unnulled2 inner_s where inner_s.c_type is not null and inner_s.c_token = s.c_token limit 1)) as c_type
into sample_unnulled2
from sample_unnulled1 s
```

В конце проверяем, что в таблице нет дубликатов (запрос должен вернуть наши 200 000): 
```sql
select count(*) from (select distinct * from sample_unnulled2)
```

### 3. Создание поля для первичного ключа груза (cargo)
Поскольку даже после выполнения различных операций по заполнению пустых ячеек в столбцах, относящихся к *cargo*, и устранения дублирующих строк в этих столбцах так и не удалось выбрать первичный ключ, создадим его сами. Для этого каждому уникальному набору полей, относящихся к *cargo* поставим в соответствие уникальное целое число.

Сначала с помощью `row_number()` сгенерируем для всех строк в исходном наборе порядковый номер записи 
```sql
select 
   s.staf_name, s.staff_age, s.staff_id, s.staff_lang, 
   s.order_pk, s.order_address, s.order_country, s.order_company, s.order_price, s.order_dt, s.order_list,
   s.cli_name, s.cli_email, s.cli_phone, s.cli_secret, 
      
   row_number() over() as c_id,
   s.c_token, s.c_pin, s.c_gen, s.c_type
into sample_unnulled3
from sample_unnulled2 s
```
После этого заменим значения порядкового номера, на значение, одинаковое для всех записей, где поля, относящиеся к *cargo*, одинаковы
```sql
select 
   s.staf_name, s.staff_age, s.staff_id, s.staff_lang, 
   s.order_pk, s.order_address, s.order_country, s.order_company, s.order_price, s.order_dt, s.order_list,
   s.cli_name, s.cli_email, s.cli_phone, s.cli_secret, 
   
   min(s.c_id) over (partition by s.c_token, s.c_pin, s.c_gen, s.c_type) as c_id,
   s.c_token, s.c_pin, s.c_gen, s.c_type
into sample_unnulled4
from sample_unnulled3 s
```
### 4. Формирование таблиц для сотрудников, клиентов, заказов и грузов, а также таблицы связи
Когда вся предварительная обработка исходной таблицы завершена, разобъем ее на отдельные сущности:
- таблицу с сотрудниками;
```sql
select distinct staf_name, staff_age, staff_id, staff_lang into staff from sample_unnulled4
```
- таблицу с клиентами;
```sql
select distinct cli_name, cli_email, cli_phone, cli_secret into client from sample_unnulled4
```
- таблицу с грузами;
```sql
select distinct с_id, c_token, c_pin, c_gen, c_type into cargo from sample_unnulled4
```
- таблицу с заказами.
```sql
select distinct order_pk, order_address, order_country, order_company, order_price, order_dt, order_list
into orders
from sample_unnulled4
```

Также сформируем таблицу, строки которой задают связи сформированных выше сущностей, объединяющие их в исходный датасет
```sql
select distinct order_pk, staff_id, cli_name, c_id into order_components from sample_unnulled4
```

Чтобы убедиться, что разделение прошло корректно и никакие новые данные не добавились (и не удалились имеющиеся), дополнительно проверим, что соединение полученных таблиц дает исходный набор данных (мы сравним только количество записей и убедимся, что их 200 000)
```sql
select count(*) 
from (
	select 
	   s.staf_name, s.staff_age, s.staff_id, s.staff_lang, 
	   o.order_pk, o.order_address, o.order_country, o.order_company,
	   o.order_price, o.order_dt, o.order_list,
	   cl.cli_name, cl.cli_email, cl.cli_phone, cl.cli_secret, 
	   c.c_token, c.c_pin, c.c_gen, c.c_type
	from 
	   order_components oc 
 	   join staff s on s.staff_id = oc.staff_id
 	   join client cl on cl.cli_name = oc.cli_name
           join orders o on o.order_pk = oc.order_pk
 	   join cargo2 c on c.c_id = oc.c_id)
```
### 5. Вынос значений order_list из массива в отдельную таблицу
```sql
select 
   order_pk, 
   uuid(replace(unnest(order_list),'''','')) as order_element_id
into order_elements
from orders
```

```sql
alter table orders 
drop column order_list
```

Проверим, что можем собрать исходный набор данных с помощью JOIN-ов
```sql
select count(*) 
from (
	select 
	   s.staf_name, s.staff_age, s.staff_id, s.staff_lang, 
	   o.order_pk, o.order_address, o.order_country, o.order_company, o.order_price,
	   o.order_dt, array_agg(oe.order_element_id) as order_list,
	   cl.cli_name, cl.cli_email, cl.cli_phone, cl.cli_secret, 
	   c.c_token, c.c_pin, c.c_gen, c.c_type
	from 
	   order_components oc 
	   join staff s on s.staff_id = oc.staff_id
	   join client cl on cl.cli_name = oc.cli_name
	   join orders o on o.order_pk = oc.order_pk
	   left join order_elements oe on oe.order_pk = o.order_pk
	   join cargo c on c.c_id = oc.c_id
	group by
	   s.staf_name, s.staff_age, s.staff_id, s.staff_lang, 
	   o.order_pk, o.order_address, o.order_country, o.order_company, o.order_price, o.order_dt,
	   cl.cli_name, cl.cli_email, cl.cli_phone, cl.cli_secret, 
	   c.c_token, c.c_pin, c.c_gen, c.c_type
)
```


### 6.
```sql
alter table staff add primary key (staff_id)

alter table client add primary key (cli_name)

alter table cargo add primary key (c_id)

alter table orders add primary key (order_pk)

alter table order_elements add primary key (order_element_id)
```


```sql
alter table order_elements 
  add constraint fk_order_elements_orders foreign key (order_pk) references orders (order_pk)

alter table order_components 
  add constraint fk_components_orders foreign key (order_pk) references orders (order_pk)

alter table order_components 
  add constraint fk_components_staff foreign key (staff_id) references staff (staff_id)

alter table order_components 
  add constraint fk_components_client foreign key (cli_name) references client (cli_name)

alter table order_components 
  add constraint fk_components_cargo foreign key (c_id) references cargo (c_id)
```

