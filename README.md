# Проект 1
Опишите здесь поэтапно ход решения задачи. Вы можете ориентироваться на тот план выполнения проекта, который мы предлагаем в инструкции на платформе.

```
-- shipping_country_rates
drop table if exists public.shipping_country_rates cascade;
create table public.shipping_country_rates(
	shipping_country_id		   serial, 
	shipping_country 		   varchar(100), 
	shipping_country_base_rate numeric(14, 2),
	primary key (shipping_country_id)
);
insert into public.shipping_country_rates
	(shipping_country, shipping_country_base_rate)
(select distinct shipping_country, 
	   shipping_country_base_rate  
from public.shipping
);


-- shipping_agreement 
drop table if exists public.shipping_agreement cascade;
create table public.shipping_agreement(
	agreementid 			smallint, 
	agreement_number		varchar(20), 
	agreement_rate			numeric(14, 2),
	agreement_commission		numeric(14, 2),
	primary key (agreementid)
);
insert into public.shipping_agreement
select	distinct vad[1]::smallint as agreementid, 
	vad[2]::varchar(20) as agreement_number, 
	vad[3]::numeric(14, 2) as agreement_rate,
	vad[4]::numeric(14, 2) as agreement_commission
from (select regexp_split_to_array(vendor_agreement_description, ':+') as vad
      from public.shipping) foo;


-- shipping_transfer 
drop table if exists public.shipping_transfer cascade;
create table public.shipping_transfer (
	transfer_type_id 		serial,
	transfer_type 			varchar(20), 
	transfer_model			varchar(20), 
	shipping_transfer_rate		numeric(14, 3),
	primary key (transfer_type_id)
);
insert into public.shipping_transfer 
(transfer_type, transfer_model, shipping_transfer_rate)
select 	distinct std[1]::varchar(20) as transfer_type, 
	std[2]::varchar(20) as transfer_model, 
	shipping_transfer_rate
from (select regexp_split_to_array(shipping_transfer_description , ':+') as std,
	     shipping_transfer_rate
      from public.shipping) foo;

	
-- shipping_info 	
drop table if exists public.shipping_info cascade;
create table public.shipping_info (
	shippingid			bigint,
	vendorid			bigint,
	payment_amount			real,
	shipping_plan_datetime		timestamp,
	transfer_type_id		bigint,
	shipping_country_id		bigint,
	agreementid			bigint,
	primary key (shippingid),
	FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE cascade,
	FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(shipping_country_id) ON UPDATE cascade,
	FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE cascade
);
insert into public.shipping_info
with cte_shipping as (
	select	distinct shippingid, 
		vendorid, 
		payment as payment_amount,
		shipping_plan_datetime::timestamp,
		shipping_country,
		regexp_split_to_array(shipping_transfer_description, ':+') as std,
		regexp_split_to_array(vendor_agreement_description, ':+') as vad
	from public.shipping
)
select	shippingid, 
	vendorid, 
	payment_amount,
	shipping_plan_datetime,
	transfer_type_id,
	shipping_country_id,
	agreementid
from	cte_shipping c join shipping_transfer st 
	on c.std[1] = st.transfer_type and c.std[2] = st.transfer_model
	join shipping_country_rates scr 
	on c.shipping_country = scr.shipping_country
	join shipping_agreement sg 
	on c.vad[1]::smallint = sg.agreementid;


-- shipping_status			
drop table if exists public.shipping_status cascade;
create table public.shipping_status (
	shippingid			serial,
	status				varchar(20),
	state				varchar(20),
	shipping_start_fact_datetime	timestamp,
	shipping_end_fact_datetime	timestamp
);
insert into public.shipping_status
with cte as (
	select *
	from(select shippingid, status, state, state_datetime,
		    row_number() over(partition by shippingid order by state_datetime desc) num			
	     from shipping) foo
	where num = 1
)
select  distinct s.shippingid, cte.status, cte.state,
	min(s.state_datetime) FILTER (WHERE s.state = 'booked') over(partition by s.shippingid)::timestamp as shipping_start_fact_datetime,
	min(s.state_datetime) FILTER (WHERE s.state = 'recieved') over(partition by s.shippingid)::timestamp as shipping_end_fact_datetime
from shipping s join cte on s.shippingid = cte.shippingid;


-- Создадим представление
create or replace view public.shipping_datamart as (
with cte as (
	select  *, 
		date_part('day', age(shipping_end_fact_datetime, shipping_start_fact_datetime)) as full_day_at_shipping,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end as is_delay,
		case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish,
		case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
					then date_part('day', age(shipping_end_fact_datetime, si.shipping_plan_datetime)) 
								else 0 end as delay_day_at_shipping
	from	shipping_info si join shipping_status ss using(shippingid)
)
select	shippingid,
	vendorid,
	transfer_type,
	full_day_at_shipping,
	is_delay,
	is_shipping_finish,
	delay_day_at_shipping,
	payment_amount,
	payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) as vat,
	payment_amount * agreement_commission as profit
from cte join shipping_transfer st using (transfer_type_id)
	 join shipping_country_rates using (shipping_country_id)
	 join shipping_agreement using (agreementid)
);

```
