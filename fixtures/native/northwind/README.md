# northwind Native Charter

## Why This Database Matters

`northwind` is the best proving ground for Firebird-native redesign because it has large row counts, reporting-style views, and several raw-table compromises inherited from older sample-schema habits.

## Main SQLite-to-Firebird Problems

- text business codes used as physical primary keys
- table names with spaces and quoted identifiers
- nullable dates used as implicit workflow state
- address and contact columns with no shared domains
- reporting logic trapped in imported view definitions

## Table-by-Table Refactoring Direction

| Raw table | Curated table | Main changes |
| --- | --- | --- |
| `Customers` | `customer` | surrogate key, unique `customer_code`, domains for contact fields |
| `CustomerDemographics` | `customer_demographic_type` | surrogate key plus unique business code |
| `CustomerCustomerDemo` | `customer_demographic_link` | numeric FKs instead of text PKs |
| `Employees` | `employee` | domains for address/phone, audit columns |
| `EmployeeTerritories` | `employee_territory` | numeric FKs and naming cleanup |
| `Territories` | `territory` | surrogate key, unique business code, FK cleanup |
| `Regions` | `region` | naming cleanup and trimmed description |
| `Shippers` | `shipper` | naming cleanup and phone domain |
| `Suppliers` | `supplier` | contact/address domains |
| `Categories` | `category` | naming cleanup |
| `Products` | `product` | money domain, structured inventory and supplier/category references |
| `Orders` | `sales_order` | explicit order status, audit columns, address snapshot domains |
| `Order Details` | `sales_order_line` | snake case, money/quantity/discount domains |

## Firebird-Native Features To Add

- numeric surrogate keys backed by sequences
- unique alternate business codes
- `dm_percent` for discounts
- explicit order status domain or lookup table
- Firebird views for high-value reports
- `sp_create_sales_order`
- `sp_add_sales_order_line`
- `sp_ship_order`

## Testing Role

- bulk-read and performance validation
- timeout and cancel behavior
- reporting-view backlog
- Firebird-native redesign proving ground
