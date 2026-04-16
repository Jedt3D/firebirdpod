# car_database Native Charter

## Why This Database Matters

`car_database` is the fastest mutation-oriented fixture in the matrix. Its curated variant should become the simplest Firebird-native business schema in the project.

## Main SQLite-to-Firebird Problems

- integer phone numbers
- integer prices with no money domain
- integer `vin` instead of a real vehicle code
- misspelled or classroom-style field naming
- free-text flags and weak business checks

## Table-by-Table Refactoring Direction

| Raw table | Curated table | Main changes |
| --- | --- | --- |
| `Brands` | `brand` | sequence-backed key, unique brand name, audit columns |
| `Models` | `model` | money domain for base price, FK to `brand`, audit columns |
| `Manufacture_Plant` | `manufacturing_plant` | boolean `company_owned`, normalized plant type, better naming |
| `Dealers` | `dealer` | normalized address fields, text phone/email support in future |
| `Customers` | `customer` | phone as text, email domain, `gender` normalized or moved to lookup |
| `Car_Parts` | `car_part` | boolean `part_recall`, date rules, FK cleanup |
| `Car_Options` | `vehicle_option_set` | clearer naming, money domain, stronger FK naming |
| `Car_Vins` | `vehicle` | surrogate `vehicle_id`, business `vin_code`, manufacturing metadata |
| `Customer_Ownership` | `vehicle_ownership` | spelling fix, date rules, money domain, ownership workflow procedures |
| `Dealer_Brand` | `dealer_brand` | naming cleanup and alternate unique enforcement |

## Firebird-Native Features To Add

- `dm_vin_code`
- `dm_money`
- `BOOLEAN` flags
- `sp_register_vehicle_sale`
- `sp_transfer_vehicle_ownership`
- `sp_register_part_recall`

## Testing Role

- smoke CRUD
- FK integrity
- generated-key behavior
- transaction rollback verification
