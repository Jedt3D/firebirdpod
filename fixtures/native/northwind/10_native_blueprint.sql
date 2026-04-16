/*
  Curated Firebird-native blueprint for northwind.
*/

create sequence seq_customer_id;
create sequence seq_customer_demographic_type_id;
create sequence seq_employee_id;
create sequence seq_territory_id;
create sequence seq_region_id;
create sequence seq_shipper_id;
create sequence seq_supplier_id;
create sequence seq_category_id;
create sequence seq_product_id;
create sequence seq_sales_order_id;
create sequence seq_sales_order_line_id;

recreate table customer (
  customer_id dm_id_bigint,
  customer_code dm_code_16,
  company_name dm_name_100,
  contact_name dm_name_100,
  contact_title dm_title_120,
  address_line dm_address_120,
  city_name dm_city_60,
  region_name dm_name_50,
  postal_code dm_postal_code_16,
  country_name dm_country_60,
  phone dm_phone_32,
  fax dm_phone_32,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_customer primary key (customer_id),
  constraint uq_customer_code unique (customer_code)
);

recreate table sales_order (
  sales_order_id dm_id_bigint,
  customer_id dm_id_bigint,
  employee_id dm_id_bigint,
  order_date timestamp,
  required_date timestamp,
  shipped_date timestamp,
  freight_amount dm_money,
  ship_name dm_name_100,
  ship_address dm_address_120,
  ship_city dm_city_60,
  ship_region dm_name_50,
  ship_postal_code dm_postal_code_16,
  ship_country dm_country_60,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_sales_order primary key (sales_order_id)
);

recreate table sales_order_line (
  sales_order_line_id dm_id_bigint,
  sales_order_id dm_id_bigint,
  product_id dm_id_bigint,
  unit_price dm_money,
  quantity dm_quantity,
  discount_rate dm_percent,
  constraint pk_sales_order_line primary key (sales_order_line_id)
);

set term !!;
create trigger bi_customer_id for customer
active before insert position 0
as
begin
  if (new.customer_id is null) then
    new.customer_id = next value for seq_customer_id;
end!!

create trigger bi_sales_order_id for sales_order
active before insert position 0
as
begin
  if (new.sales_order_id is null) then
    new.sales_order_id = next value for seq_sales_order_id;
end!!

create procedure sp_ship_order (
  a_sales_order_id bigint,
  a_shipped_at timestamp,
  a_freight_amount numeric(18,4)
)
as
begin
  /* Apply shipping transition and audit checks here. */
end!!
set term ;!!
