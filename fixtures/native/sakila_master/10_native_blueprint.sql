/*
  Curated Firebird-native blueprint for sakila_master.
*/

create sequence seq_actor_id;
create sequence seq_address_id;
create sequence seq_city_id;
create sequence seq_country_id;
create sequence seq_category_id;
create sequence seq_language_id;
create sequence seq_film_id;
create sequence seq_store_id;
create sequence seq_staff_member_id;
create sequence seq_customer_id;
create sequence seq_inventory_item_id;
create sequence seq_rental_id;
create sequence seq_payment_id;

recreate table customer (
  customer_id dm_id_bigint,
  store_id dm_id_bigint,
  first_name dm_name_50,
  last_name dm_name_50,
  email_address dm_email_254,
  address_id dm_id_bigint,
  is_active dm_flag,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_customer primary key (customer_id)
);

recreate table film (
  film_id dm_id_bigint,
  title dm_name_100,
  description dm_description_text,
  release_year smallint,
  language_id dm_id_bigint,
  original_language_id dm_id_bigint,
  rental_duration smallint,
  rental_rate dm_money,
  length_minutes integer,
  replacement_cost dm_money,
  rating_code dm_code_16,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_film primary key (film_id)
);

set term !!;
create trigger bi_customer_id for customer
active before insert position 0
as
begin
  if (new.customer_id is null) then
    new.customer_id = next value for seq_customer_id;
end!!

create procedure sp_post_payment (
  a_customer_id bigint,
  a_staff_member_id bigint,
  a_rental_id bigint,
  a_amount numeric(18,4),
  a_paid_at timestamp
)
as
begin
  /* Insert curated payment row and validate business rules here. */
end!!
set term ;!!
