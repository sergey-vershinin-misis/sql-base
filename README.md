# Итоговое задание по предмету "Базы данных и SQL"
Репозиторий содержит дамп итоговой БД, скрипты, использовавшиеся для приведение исходного набора данных к итоговому, и описание процесса решения задачи
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

Заполнение ячеек, в которых значения отсутствуют, выполняем в два этапа.  
Сначала копируем данные из sample в sample_unnulled1, заменяя в столбцах, похожих по сути на первичные ключи сущностей, null на значения. Для cargo генерируем token по pin
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
После этого копируем данные из sample_unnulled1 в sample_unnulled2, заменяя null на соответствующие значения в для остальных столбцов
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

В конце проверим, что в таблице нет дубликатов (запрос должен вернуть наши 200 000): 
```sql
select count(*) from (select distinct * from sample_unnulled2)
```

### 3. Создание поля для первичного ключа груза (cargo)
Генерируем порядковый номер записи в исходном датасете с помощью row_number()
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
После этого заменяем значения в c_id, на одинаковый для всех записей, где поля груза одинаковы
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
Формируем таблицу с сотрудниками
```sql
select distinct staf_name, staff_age, staff_id, staff_lang into staff from sample_unnulled4
```
Формируем таблицу с клиентами
```sql
select distinct cli_name, cli_email, cli_phone, cli_secret into client from sample_unnulled4
```
Формируем таблицу с грузами
```sql
select distinct с_id, c_token, c_pin, c_gen, c_type into cargo from sample_unnulled4
```
Формируем таблицу с заказами
```sql
select distinct order_pk, order_address, order_country, order_company, order_price, order_dt, order_list into orders
from sample_unnulled4
```

```sql
select distinct order_pk, staff_id, cli_name, c_id into order_components from sample_unnulled4
```
### 5. Вынос значений order_list из массива в отдельную таблицу
