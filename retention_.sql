--в таблице даны транзакционные данные в разрезе:
--device_id - условный номер устройства
--invoice_date - дата оплаты
--service - наименование подписки

--Первая часть. Найдя для каждого устройства самую первую дату оплаты в разрезе сервиса, определяем когорту, к которой он относится.
--Далее определяем размер когорты. 
--Во второй части скрипта определяем для каждой когорты число оставшихся платящих устройств в каждом последующем месяце.
--Соединяем первую и вторую части через left join, получаем таблицу  с наименованием сервиса, когорты, размером когорты, месяцем оплаты, количеством устройств в этом месяце,
--отношением кол-ва в месяцк к исходному кол-ву в когорте,

--Визуализация таблицы через Metabase прилагается

create table db.Retention_new_total as ( 
select *,  to_char(cohort,'Mon YY'), numb -1 as number from ( 
select a.service, a.cohort, month, cohort_count , count_monthly , round(count_monthly /cohort_count, 2) as retention, 
  row_number() over (partition by a.service, a.cohort order by month) as numb from
(select service, cohort, count(distinct device_id) cohort_count from (
select *, date_trunc('month',c) as cohort from ( 
select *, min(invoice_date) over (partition by service, device_id order by invoice_date) as c
from db.transactions dpf 
where  service in ( 'Подписка1', 'Подписка2' )  and  
date_trunc('month', invoice_date)<date_trunc('month', current_date)
and  device_id is not null)
where cohort>='2021-01-01')
group by  service, cohort)a
left join
(select service, date_trunc('month', invoice_date) as month, cohort, count(distinct device_id) count_monthly from ( 
select *, date_trunc('month',c) as cohort from ( 
select *, min(invoice_date) over (partition by service, device_id order by invoice_date) as c
from db.transactions
where  service in ('Подписка1', 'Подписка2'  ) and device_id is not null
and date_trunc('month', invoice_date)<date_trunc('month', current_date))
where cohort>='2021-01-01')
group by service, date_trunc('month', invoice_date), cohort)b  on a.cohort=b.cohort and a.service=b.service 
)
)
