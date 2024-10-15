ВИТРИНА ДЛЯ РАСЧЕТА С КУРЬЕРАМИ

### ЗАДАЧА
Нам необходима витрина, содержащая информацию о выплатах курьерам.
В ней необходимо рассчитать суммы оплаты каждому курьеру за предыдущий месяцб расчетным числом является 10.

# 1. Список полей, необходимых для витрины:
- `id` — идентификатор записи.
- `courier_id` — ID курьера, которому перечисляем.
- `courier_name` — Ф. И. О. курьера.
- `settlement_year` — год отчёта.
- `settlement_month` — месяц отчёта, где 1 — январь и 12 — декабрь.
- `orders_count` — количество заказов за период (месяц).
- `orders_total_sum` — общая стоимость заказов.
- `rate_avg` — средний рейтинг курьера по оценкам пользователей.
- `order_processing_fee` — сумма, удержанная компанией за обработку заказов, которая высчитывается как 
orders_total_sum * 0.25.
- `courier_order_sum` — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- `courier_tips_sum` — сумма, которую пользователи оставили курьеру в качестве чаевых.
- `courier_reward_sum` — сумма, которую необходимо перечислить курьеру. Вычисляется как 
courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

# Правила расчета процента выплаты курьеру в зависимости от рейтинга:
- r < 4 — 5% от заказа, но не менее 100 р.;
- 4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
- 4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
- 4.9 <= r — 10% от заказа, но не менее 200 р.

! Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки.

# 2. Список таблиц в слое DDS, из которых будут взяты поля для витрины:
Существуюшие таблицы:
- dds.fct_product_sales (orders_count, orders_total_sum)
- dds.dm_timestamps (settlement_year, settlement_month). Дополняем данными по ts доставок.
- dds.dm_orders. Добавляем delivery_id.
Таблицы, которые необходимо создать:
- dds.dm_couriers (courier_id, courier_name) 
- dds.dm_deliveries (rate, tip_sum)

# 3. В результате у нас получится следующий DDL для заполнения из API:

--CDM
--создаем витрину для заказов
drop table if exists cdm.dm_courier_ledger;
create table if not exists cdm.dm_courier_ledger
(
id serial not null,
courier_id varchar not null,
courier_name varchar not null,
settlement_year smallint not null,
settlement_month smallint not null,
orders_count integer not null,
orders_total_sum numeric(14, 2) not null,
rate_avg numeric (3,2)  not null,
order_processing_fee numeric(14, 2) not null,
courier_order_sum numeric(14, 2) not null,
courier_tips_sum numeric(14, 2) not null,
courier_reward_sum numeric(14, 2) not null
)

--добавляем ограничения к витрине
alter table cdm.dm_courier_ledger
add constraint id_pk primary key(id);

alter table cdm.dm_courier_ledger
add constraint dm_settlement_report_settlement_year_check 
check (settlement_year >= 2022);

alter table cdm.dm_courier_ledger 
alter column orders_count set default 0,
alter column orders_total_sum set default 0,
alter column rate_avg set default 0,
alter column order_processing_fee set default 0,
alter column courier_order_sum set default 0,
alter column courier_tips_sum set default 0,
alter column courier_reward_sum set default 0;

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT orders_count_check CHECK (orders_count >= 0),
ADD CONSTRAINT orders_total_sum_check CHECK (orders_total_sum >= 0),
ADD CONSTRAINT rate_avg_check CHECK (rate_avg >= 0),
ADD CONSTRAINT order_processing_fee_check CHECK (order_processing_fee >= 0),
ADD CONSTRAINT courier_order_sum_check CHECK (courier_order_sum >= 0),
ADD CONSTRAINT courier_tips_sum_check CHECK (courier_tips_sum >= 0),
ADD CONSTRAINT courier_reward_sum_check CHECK (courier_reward_sum >= 0),
ADD CONSTRAINT settlement_month_check CHECK (settlement_month >= 1 and settlement_month <= 12);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT unique_courier_settlement 
UNIQUE (courier_id, settlement_year, settlement_month);

--DDS
--dm_couriers
drop table if exists dds.dm_couriers;
create table if not exists dds.dm_couriers
(
id serial primary key not null,
courier_id varchar not null,
courier_name varchar not null
);

ALTER TABLE stg.couriers
ADD CONSTRAINT couriers_object_id_unique UNIQUE (object_id);

drop table if exists dds.dm_deliveries;
create table if not exists dds.dm_deliveries
(
id serial primary key not null,
delivery_id varchar not null,
courier_id int not null,
address varchar not null,
rate int not null,
tip_sum numeric not null,
timestamp_id integer not null
);

ALTER TABLE dds.dm_deliveries
ADD CONSTRAINT dm_deliveries_courier_id_fkey 
foreign key (courier_id)
references dds.dm_couriers(id);

ALTER TABLE dds.dm_deliveries
ADD CONSTRAINT dm_deliveries_timestamp_id_fkey 
foreign key (timestamp_id)
references dds.dm_timestamps(id);

--Дополняем существующие таблицы новыми колонками:
alter table dds.dm_orders
add column delivery_id int;

ALTER TABLE dds.dm_orders
ADD CONSTRAINT dm_orders_delivery_id_fkey 
foreign key (delivery_id)
references dds.dm_deliveries(id);

--STG
drop table if exists stg.couriers;
create table if not exists stg.couriers
(
id serial not null,
object_id varchar not null,
object_value text not null
);

drop table if exists stg.deliveries;
create table if not exists stg.deliveries
(
id serial not null,
objectc_id varchar not null,
object_value text not null
);









