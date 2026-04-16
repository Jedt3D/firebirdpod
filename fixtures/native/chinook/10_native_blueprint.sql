/*
  Curated Firebird-native blueprint for chinook.
*/

create sequence seq_artist_id;
create sequence seq_album_id;
create sequence seq_track_id;
create sequence seq_customer_id;
create sequence seq_employee_id;
create sequence seq_invoice_id;
create sequence seq_invoice_item_id;

recreate table customer (
  customer_id dm_id_bigint,
  first_name dm_name_50,
  last_name dm_name_50,
  company_name dm_name_100,
  address_line dm_address_120,
  city_name dm_city_60,
  state_name dm_name_50,
  country_name dm_country_60,
  postal_code dm_postal_code_16,
  phone dm_phone_32,
  fax dm_phone_32,
  email_address dm_email_254,
  support_rep_id dm_id_bigint,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_customer primary key (customer_id)
);

recreate table invoice_item (
  invoice_item_id dm_id_bigint,
  invoice_id dm_id_bigint,
  track_id dm_id_bigint,
  unit_price dm_money,
  quantity dm_quantity,
  constraint pk_invoice_item primary key (invoice_item_id)
);

set term !!;
create trigger bi_customer_id for customer
active before insert position 0
as
begin
  if (new.customer_id is null) then
    new.customer_id = next value for seq_customer_id;
end!!

create procedure sp_add_invoice_line (
  a_invoice_id bigint,
  a_track_id bigint,
  a_unit_price numeric(18,4),
  a_quantity integer
)
as
begin
  /* Validate track and insert curated invoice line here. */
end!!
set term ;!!
