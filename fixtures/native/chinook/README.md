# chinook Native Charter

## Why This Database Matters

`chinook` is the main integration database for runtime and query behavior. Its curated variant should stay close to the original business model, but become cleaner and more Firebird-native.

## Main SQLite-to-Firebird Problems

- weak typing on imported text and timestamps
- no shared domains for contact and billing fields
- no explicit audit strategy
- no procedure boundary for invoice writes

## Table-by-Table Refactoring Direction

| Raw table | Curated table | Main changes |
| --- | --- | --- |
| `artists` | `artist` | sequence-backed key, normalized naming |
| `albums` | `album` | sequence-backed key, FK cleanup |
| `tracks` | `track` | explicit checks for duration, bytes, and price |
| `genres` | `genre` | naming cleanup |
| `media_types` | `media_type` | naming cleanup |
| `playlists` | `playlist` | naming cleanup |
| `playlist_track` | `playlist_track` | same logical table, clearer PK/FK naming |
| `employees` | `employee` | shared domains for contact fields, audit columns |
| `customers` | `customer` | domains for phone/email/address, stronger null and format expectations |
| `invoices` | `invoice` | billing snapshot preserved, audit columns added |
| `invoice_items` | `invoice_item` | money domain and quantity domain, invoice write procedures |

## Firebird-Native Features To Add

- contact domains
- money and quantity domains
- sequence-backed keys
- `sp_create_invoice`
- `sp_add_invoice_line`
- `vw_customer_purchase_history`

## Testing Role

- default runtime integration
- joins and pagination
- relation loading
- schema round-trip and drift checks
