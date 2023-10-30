# Итоговое задание по предмету "Базы данных и SQL"
Репозиторий содержит дамп итоговой БД, скрипты, использовавшиеся для приведение исходного набора данных к итоговому, и описание процесса решения задачи
## Описание решения
### 1. Создание таблицы и импорт данных из файла csv
Сначала создаем таблицу для загрузки набора данных с помощью следующего запроса (размерности целочисленных типов и длины для 
varchar взяты на основе максимальных значений в csv-файле, но с небольшим запасом)
```
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

После этого загружаем csv-файл в созданную таблицу
```
COPY sample
FROM '<data-folder>\sample_0.csv'
DELIMITER ',' CSV HEADER
```
