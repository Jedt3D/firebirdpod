# Firebird Native Design Rules

## Naming

- Use lowercase snake case for curated schemas
- Avoid identifiers with spaces
- Avoid quoted mixed-case names unless there is a very strong reason

## Key Strategy

- Primary keys use sequences and `BEFORE INSERT` triggers
- Natural business codes become unique alternate keys, not physical primary keys
- Sequence names follow `seq_<table>_id`
- Insert triggers follow `bi_<table>_id`

## Data Typing

- Use `UTF8` consistently for text
- Use domains for repeated text, money, quantity, timestamp, and flag definitions
- Do not store phone numbers or postal codes as integers
- Convert imported `0/1` or `CHAR(1)` flags to `BOOLEAN` or checked domains

## Auditing

- Add `created_at` and `updated_at` where the table represents business state
- Use `BEFORE UPDATE` triggers to maintain `updated_at`

## Reuse

- Prefer views for stable reporting projections
- Prefer procedures for write workflows and business operations
- Reserve temporary translation layers for raw fixture compatibility only
