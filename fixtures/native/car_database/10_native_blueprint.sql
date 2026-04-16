/*
  Curated Firebird-native blueprint for car_database.
  This is a design-start file for the native fixture, not a complete migration script yet.
*/

create sequence seq_brand_id;
create sequence seq_model_id;
create sequence seq_manufacturing_plant_id;
create sequence seq_dealer_id;
create sequence seq_customer_id;
create sequence seq_car_part_id;
create sequence seq_vehicle_option_set_id;
create sequence seq_vehicle_id;
create sequence seq_vehicle_ownership_id;

recreate table brand (
  brand_id dm_id_bigint,
  brand_name dm_name_50,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_brand primary key (brand_id),
  constraint uq_brand_name unique (brand_name)
);

recreate table vehicle (
  vehicle_id dm_id_bigint,
  vin_code dm_vin_code,
  model_id dm_id_bigint,
  vehicle_option_set_id dm_id_bigint,
  manufactured_date date,
  manufacturing_plant_id dm_id_bigint,
  created_at dm_created_at,
  updated_at dm_updated_at,
  constraint pk_vehicle primary key (vehicle_id),
  constraint uq_vehicle_vin_code unique (vin_code)
);

set term !!;
create trigger bi_brand_id for brand
active before insert position 0
as
begin
  if (new.brand_id is null) then
    new.brand_id = next value for seq_brand_id;
end!!

create trigger bi_vehicle_id for vehicle
active before insert position 0
as
begin
  if (new.vehicle_id is null) then
    new.vehicle_id = next value for seq_vehicle_id;
end!!

create procedure sp_register_vehicle_sale (
  a_vehicle_id bigint,
  a_customer_id bigint,
  a_dealer_id bigint,
  a_purchase_date date,
  a_purchase_price numeric(18,4)
)
as
begin
  /* Insert curated ownership row and apply business checks here. */
end!!
set term ;!!
