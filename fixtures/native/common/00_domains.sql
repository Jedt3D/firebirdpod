/*
  Shared Firebird-native domains for curated fixture databases.
  Apply these to curated databases, not to the raw converted fixtures.
*/

create domain dm_id_bigint as bigint;
create domain dm_code_4 as varchar(4) character set utf8;
create domain dm_code_16 as varchar(16) character set utf8;
create domain dm_name_25 as varchar(25) character set utf8;
create domain dm_name_50 as varchar(50) character set utf8;
create domain dm_name_100 as varchar(100) character set utf8;
create domain dm_title_120 as varchar(120) character set utf8;
create domain dm_description_text as blob sub_type text character set utf8;
create domain dm_email_254 as varchar(254) character set utf8;
create domain dm_phone_32 as varchar(32) character set utf8;
create domain dm_postal_code_16 as varchar(16) character set utf8;
create domain dm_city_60 as varchar(60) character set utf8;
create domain dm_country_60 as varchar(60) character set utf8;
create domain dm_address_120 as varchar(120) character set utf8;
create domain dm_money as numeric(18,4) default 0 check (value is null or value >= 0);
create domain dm_quantity as integer default 0 check (value is null or value >= 0);
create domain dm_percent as numeric(9,4) default 0 check (value is null or value between 0 and 1);
create domain dm_flag as boolean default true;
create domain dm_created_at as timestamp default current_timestamp;
create domain dm_updated_at as timestamp;
create domain dm_vin_code as char(17) character set utf8 check (value is null or char_length(value) = 17);
