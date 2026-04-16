# sakila_master Native Charter

## Why This Database Matters

`sakila_master` is already close to a real operational schema, which makes it the best database for Firebird-native refinement of money types, boolean flags, auditing, and workflow procedures.

## Main SQLite-to-Firebird Problems

- imported small-int and char flags instead of clean booleans
- some business columns with weak typing, such as `release_year`
- raw trigger inventory not yet mapped to curated Firebird triggers
- helper and reporting structures not yet re-expressed as Firebird-native views or procedures

## Table-by-Table Refactoring Direction

| Raw table | Curated table | Main changes |
| --- | --- | --- |
| `actor` | `actor` | sequence-backed key, audit trigger cleanup |
| `address` | `address` | domains for postal code and phone |
| `city` | `city` | naming and audit cleanup |
| `country` | `country` | naming and audit cleanup |
| `category` | `category` | naming and audit cleanup |
| `language` | `language_ref` | clearer naming for lookup semantics |
| `film` | `film` | `release_year` to `smallint`, money domains, curated checks |
| `film_text` | `film_search_cache` | explicit cache-table semantics or replacement strategy |
| `store` | `store` | audit cleanup |
| `staff` | `staff_member` | boolean `active`, audit cleanup |
| `customer` | `customer` | boolean `active`, domains for email and contact data |
| `inventory` | `inventory_item` | naming cleanup |
| `rental` | `rental` | workflow procedures for checkout and return |
| `payment` | `payment` | money domain, payment procedures |
| `film_actor` | `film_actor_link` | naming cleanup only |
| `film_category` | `film_category_link` | naming cleanup only |

## Firebird-Native Features To Add

- `BOOLEAN` for active flags
- shared money domains
- audit triggers replacing imported trigger style
- `sp_checkout_rental`
- `sp_return_rental`
- `sp_post_payment`
- curated reporting views

## Testing Role

- migration complexity
- schema introspection
- trigger and procedure coverage
- Firebird-native business workflow validation
