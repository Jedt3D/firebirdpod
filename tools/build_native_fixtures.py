#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Callable

from firebird.driver import connect, create_database


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parent
SOURCE_ROOT = WORKSPACE_ROOT / "databases" / "firebird"
TARGET_ROOT = WORKSPACE_ROOT / "databases" / "firebird_native"
DOMAINS_SQL = REPO_ROOT / "fixtures" / "native" / "common" / "00_domains.sql"
REPORT_ROOT = WORKSPACE_ROOT / "docs" / "serverpod-firebird" / "refactoring" / "native-build-reports"

USER = "sysdba"
PASSWORD = "masterkey"
CHARSET = "UTF8"
HOST = "localhost"
DATABASE_NAMES = ("car_database", "chinook", "northwind", "sakila_master")


def snake_case(value: str) -> str:
    value = value.replace("/", "_").replace("-", "_").replace(" ", "_")
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"__+", "_", value)
    return value.strip("_").lower()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sanitize_name(prefix: str, payload: str) -> str:
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
    base = re.sub(r"[^A-Za-z0-9_]+", "_", prefix).strip("_").lower()
    base = base[:48] if base else "obj"
    return f"{base}_{digest}"[:63]


def split_sql_script(script_text: str) -> list[str]:
    return [part.strip() for part in script_text.split(";") if part.strip()]


def dsn_for(path: Path) -> str:
    return f"{HOST}:{path}"


@dataclass
class SourceColumn:
    name: str
    field_type: int
    field_sub_type: int | None
    field_length: int | None
    char_length: int | None
    precision: int | None
    scale: int | None
    not_null: bool


@dataclass
class SourceForeignKey:
    name: str
    local_columns: list[str]
    ref_table: str
    ref_columns: list[str]


@dataclass
class SourceTable:
    name: str
    columns: list[SourceColumn]
    primary_key: list[str]
    foreign_keys: list[SourceForeignKey]
    row_count: int


@dataclass
class TargetColumn:
    source_name: str | None
    target_name: str
    type_sql: str
    not_null: bool = False
    transform: Callable[[dict[str, object]], object] | None = None
    remap_from_table: str | None = None


@dataclass
class TablePlan:
    source_name: str
    target_name: str
    columns: list[TargetColumn]
    source_pk: list[str]
    target_pk: list[str]
    surrogate_pk: bool
    unique_sets: list[list[str]] = field(default_factory=list)
    foreign_keys: list[tuple[list[str], str, list[str]]] = field(default_factory=list)
    sequence_name: str | None = None
    trigger_name: str | None = None
    source_count: int = 0


TABLE_NAME_MAPS: dict[str, dict[str, str]] = {
    "car_database": {
        "Brands": "brand",
        "Car_Options": "vehicle_option_set",
        "Car_Parts": "car_part",
        "Car_Vins": "vehicle",
        "Customer_Ownership": "vehicle_ownership",
        "Customers": "customer",
        "Dealer_Brand": "dealer_brand",
        "Dealers": "dealer",
        "Manufacture_Plant": "manufacturing_plant",
        "Models": "model",
    },
    "chinook": {
        "albums": "album",
        "artists": "artist",
        "customers": "customer",
        "employees": "employee",
        "genres": "genre",
        "invoice_items": "invoice_item",
        "invoices": "invoice",
        "media_types": "media_type",
        "playlist_track": "playlist_track",
        "playlists": "playlist",
        "tracks": "track",
    },
    "northwind": {
        "Categories": "category",
        "CustomerCustomerDemo": "customer_demographic_link",
        "CustomerDemographics": "customer_demographic_type",
        "Customers": "customer",
        "EmployeeTerritories": "employee_territory",
        "Employees": "employee",
        "Order Details": "sales_order_line",
        "Orders": "sales_order",
        "Products": "product",
        "Regions": "region",
        "Shippers": "shipper",
        "Suppliers": "supplier",
        "Territories": "territory",
    },
    "sakila_master": {
        "actor": "actor",
        "address": "address",
        "category": "category",
        "city": "city",
        "country": "country",
        "customer": "customer",
        "film": "film",
        "film_actor": "film_actor_link",
        "film_category": "film_category_link",
        "film_text": "film_search_cache",
        "inventory": "inventory_item",
        "language": "language_ref",
        "payment": "payment",
        "rental": "rental",
        "staff": "staff_member",
        "store": "store",
    },
}


COLUMN_NAME_MAPS: dict[str, dict[tuple[str, str], str]] = {
    "car_database": {
        ("Car_Options", "option_set_id"): "vehicle_option_set_id",
        ("Car_Vins", "option_set_id"): "vehicle_option_set_id",
        ("Car_Vins", "vin"): "vehicle_id",
        ("Customer_Ownership", "vin"): "vehicle_id",
        ("Manufacture_Plant", "manufacture_plant_id"): "manufacturing_plant_id",
        ("Car_Parts", "manufacture_plant_id"): "manufacturing_plant_id",
        ("Car_Vins", "manufactured_plant_id"): "manufacturing_plant_id",
        ("Customers", "phone_number"): "phone",
        ("Customer_Ownership", "warantee_expire_date"): "warranty_expire_date",
        ("Manufacture_Plant", "company_owned"): "is_company_owned",
        ("Car_Parts", "part_recall"): "is_recalled",
    },
    "northwind": {
        ("Customers", "CustomerID"): "customer_code",
        ("Orders", "CustomerID"): "customer_id",
        ("CustomerCustomerDemo", "CustomerID"): "customer_id",
        ("CustomerDemographics", "CustomerTypeID"): "customer_demographic_type_code",
        ("CustomerCustomerDemo", "CustomerTypeID"): "customer_demographic_type_id",
        ("Territories", "TerritoryID"): "territory_code",
        ("EmployeeTerritories", "TerritoryID"): "territory_id",
        ("Orders", "OrderID"): "sales_order_id",
        ("Order Details", "OrderID"): "sales_order_id",
        ("Orders", "ShipVia"): "shipper_id",
        ("Employees", "ReportsTo"): "manager_employee_id",
        ("Products", "Discontinued"): "is_discontinued",
    },
    "sakila_master": {
        ("staff", "staff_id"): "staff_member_id",
        ("store", "manager_staff_id"): "manager_staff_member_id",
        ("payment", "staff_id"): "staff_member_id",
        ("rental", "staff_id"): "staff_member_id",
        ("inventory", "inventory_id"): "inventory_item_id",
        ("rental", "inventory_id"): "inventory_item_id",
        ("customer", "active"): "is_active",
        ("staff", "active"): "is_active",
    },
}


SURROGATE_TABLES: dict[str, set[str]] = {
    "northwind": {"Customers", "CustomerDemographics", "Territories"},
}


BOOL_COLUMNS: dict[str, set[tuple[str, str]]] = {
    "car_database": {
        ("Manufacture_Plant", "company_owned"),
        ("Car_Parts", "part_recall"),
    },
    "northwind": {
        ("Products", "Discontinued"),
    },
    "sakila_master": {
        ("customer", "active"),
        ("staff", "active"),
    },
}


def vehicle_vin_code(row: dict[str, object]) -> str:
    raw = int(row["vin"])
    return f"FDVIN{raw:012d}"


EXTRA_COLUMNS: dict[str, dict[str, list[TargetColumn]]] = {
    "car_database": {
        "Car_Vins": [
            TargetColumn(
                source_name=None,
                target_name="vin_code",
                type_sql="dm_vin_code",
                not_null=True,
                transform=vehicle_vin_code,
            )
        ]
    }
}


PROCEDURES: dict[str, list[str]] = {
    "car_database": [
        """
        create or alter procedure sp_register_vehicle_sale (
          a_customer_id bigint,
          a_vehicle_id bigint,
          a_dealer_id bigint,
          a_purchase_date date,
          a_purchase_price numeric(18,4)
        )
        as
        begin
          insert into "vehicle_ownership" (
            "customer_id",
            "vehicle_id",
            "purchase_date",
            "purchase_price",
            "dealer_id"
          )
          values (
            :a_customer_id,
            :a_vehicle_id,
            :a_purchase_date,
            :a_purchase_price,
            :a_dealer_id
          );
        end
        """,
    ],
    "chinook": [
        """
        create or alter procedure sp_add_invoice_line (
          a_invoice_id bigint,
          a_track_id bigint,
          a_unit_price numeric(18,4),
          a_quantity integer
        )
        as
        begin
          insert into "invoice_item" (
            "invoice_line_id",
            "invoice_id",
            "track_id",
            "unit_price",
            "quantity"
          )
          values (
            next value for "seq_invoice_item",
            :a_invoice_id,
            :a_track_id,
            :a_unit_price,
            :a_quantity
          );
        end
        """,
    ],
    "northwind": [
        """
        create or alter procedure sp_ship_order (
          a_sales_order_id bigint,
          a_shipped_at timestamp,
          a_freight_amount numeric(18,4)
        )
        as
        begin
          update "sales_order"
             set "shipped_date" = :a_shipped_at,
                 "freight" = :a_freight_amount
           where "sales_order_id" = :a_sales_order_id;
        end
        """,
    ],
    "sakila_master": [
        """
        create or alter procedure sp_post_payment (
          a_customer_id bigint,
          a_staff_member_id bigint,
          a_rental_id bigint,
          a_amount numeric(18,4),
          a_paid_at timestamp
        )
        as
        begin
          insert into "payment" (
            "payment_id",
            "customer_id",
            "staff_member_id",
            "rental_id",
            "amount",
            "payment_date",
            "updated_at"
          )
          values (
            next value for "seq_payment",
            :a_customer_id,
            :a_staff_member_id,
            :a_rental_id,
            :a_amount,
            :a_paid_at,
            current_timestamp
          );
        end
        """,
    ],
}


VIEWS: dict[str, list[str]] = {
    "chinook": [
        """
        create or alter view "vw_customer_purchase_history" as
        select
          c."customer_id",
          c."first_name",
          c."last_name",
          count(ii."invoice_line_id") as "line_count",
          sum(ii."unit_price" * ii."quantity") as "total_amount"
        from "customer" c
        join "invoice" i on i."customer_id" = c."customer_id"
        join "invoice_item" ii on ii."invoice_id" = i."invoice_id"
        group by c."customer_id", c."first_name", c."last_name"
        """,
    ],
    "northwind": [
        """
        create or alter view "vw_sales_by_category" as
        select
          c."category_id",
          c."category_name",
          sum(sol."unit_price" * sol."quantity" * (1 - coalesce(sol."discount", 0))) as "net_sales"
        from "category" c
        join "product" p on p."category_id" = c."category_id"
        join "sales_order_line" sol on sol."product_id" = p."product_id"
        group by c."category_id", c."category_name"
        """,
    ],
    "sakila_master": [
        """
        create or alter view "vw_customer_balance" as
        select
          c."customer_id",
          c."first_name",
          c."last_name",
          sum(p."amount") as "total_paid"
        from "customer" c
        left join "payment" p on p."customer_id" = c."customer_id"
        group by c."customer_id", c."first_name", c."last_name"
        """,
    ],
}


def source_type_sql(column: SourceColumn) -> str:
    if column.field_type == 7:
        return "SMALLINT"
    if column.field_type == 8:
        return "INTEGER"
    if column.field_type == 10:
        return "FLOAT"
    if column.field_type == 12:
        return "DATE"
    if column.field_type == 13:
        return "TIME"
    if column.field_type == 14:
        return f"CHAR({column.char_length or column.field_length or 1}) CHARACTER SET UTF8"
    if column.field_type == 16:
        if column.scale and column.scale < 0:
            precision = column.precision or 18
            return f"NUMERIC({precision},{abs(column.scale)})"
        return "BIGINT"
    if column.field_type == 23:
        return "BOOLEAN"
    if column.field_type == 27:
        return "DOUBLE PRECISION"
    if column.field_type == 35:
        return "TIMESTAMP"
    if column.field_type == 37:
        return f"VARCHAR({column.char_length or column.field_length or 1}) CHARACTER SET UTF8"
    if column.field_type == 261:
        if column.field_sub_type == 1:
            return "BLOB SUB_TYPE TEXT CHARACTER SET UTF8"
        return "BLOB SUB_TYPE BINARY"
    raise ValueError(f"Unsupported Firebird field type code: {column.field_type}")


def not_null_type(column: TargetColumn) -> str:
    if column.not_null:
        return f"{column.type_sql} NOT NULL"
    return column.type_sql


def read_source_metadata(connection, table_name: str) -> SourceTable:
    cur = connection.cursor()
    cur.execute(
        """
        select
          trim(rf.rdb$field_name),
          f.rdb$field_type,
          f.rdb$field_sub_type,
          f.rdb$field_length,
          f.rdb$character_length,
          f.rdb$field_precision,
          f.rdb$field_scale,
          coalesce(rf.rdb$null_flag, 0)
        from rdb$relation_fields rf
        join rdb$fields f on rf.rdb$field_source = f.rdb$field_name
        where rf.rdb$relation_name = ?
        order by rf.rdb$field_position
        """,
        (table_name,),
    )
    columns = [
        SourceColumn(
            name=row[0],
            field_type=row[1],
            field_sub_type=row[2],
            field_length=row[3],
            char_length=row[4],
            precision=row[5],
            scale=row[6],
            not_null=bool(row[7]),
        )
        for row in cur
    ]

    cur.execute(
        """
        select trim(seg.rdb$field_name)
        from rdb$relation_constraints rc
        join rdb$index_segments seg on rc.rdb$index_name = seg.rdb$index_name
        where rc.rdb$relation_name = ?
          and rc.rdb$constraint_type = 'PRIMARY KEY'
        order by seg.rdb$field_position
        """,
        (table_name,),
    )
    primary_key = [row[0] for row in cur]

    cur.execute(
        """
        select
          trim(rc.rdb$constraint_name),
          trim(seg.rdb$field_name),
          trim(refrc.rdb$relation_name),
          trim(refseg.rdb$field_name)
        from rdb$relation_constraints rc
        join rdb$ref_constraints refc on rc.rdb$constraint_name = refc.rdb$constraint_name
        join rdb$relation_constraints refrc on refc.rdb$const_name_uq = refrc.rdb$constraint_name
        join rdb$index_segments seg on rc.rdb$index_name = seg.rdb$index_name
        join rdb$index_segments refseg
          on refrc.rdb$index_name = refseg.rdb$index_name
         and seg.rdb$field_position = refseg.rdb$field_position
        where rc.rdb$relation_name = ?
          and rc.rdb$constraint_type = 'FOREIGN KEY'
        order by rc.rdb$constraint_name, seg.rdb$field_position
        """,
        (table_name,),
    )
    fk_groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for name, local_field, ref_table, ref_field in cur:
        fk_groups[name].append((local_field, ref_table, ref_field))
    foreign_keys = [
        SourceForeignKey(
            name=name,
            local_columns=[item[0] for item in items],
            ref_table=items[0][1],
            ref_columns=[item[2] for item in items],
        )
        for name, items in fk_groups.items()
    ]

    cur.execute(f"select count(*) from {quote_ident(table_name)}")
    row_count = cur.fetchone()[0]
    return SourceTable(
        name=table_name,
        columns=columns,
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        row_count=row_count,
    )


def load_source_schema(source_path: Path) -> dict[str, SourceTable]:
    con = connect(dsn_for(source_path), user=USER, password=PASSWORD, charset=CHARSET)
    cur = con.cursor()
    cur.execute(
        """
        select trim(rdb$relation_name)
        from rdb$relations
        where coalesce(rdb$system_flag, 0) = 0
          and rdb$view_blr is null
        order by 1
        """
    )
    tables = {name: read_source_metadata(con, name) for (name,) in cur}
    con.close()
    return tables


def table_target_name(database_name: str, source_table: str) -> str:
    return TABLE_NAME_MAPS.get(database_name, {}).get(source_table, snake_case(source_table))


def surrogate_id_name(target_table: str) -> str:
    return f"{target_table}_id"


def find_fk(table: SourceTable, column_name: str) -> SourceForeignKey | None:
    for fk in table.foreign_keys:
        if len(fk.local_columns) == 1 and fk.local_columns[0] == column_name:
            return fk
    return None


def target_column_name(
    database_name: str,
    source_table: str,
    source_column: str,
    source_tables: dict[str, SourceTable],
) -> str:
    override = COLUMN_NAME_MAPS.get(database_name, {}).get((source_table, source_column))
    if override:
        return override

    table = source_tables[source_table]
    fk = find_fk(table, source_column)
    if fk and fk.ref_table in SURROGATE_TABLES.get(database_name, set()):
        return surrogate_id_name(table_target_name(database_name, fk.ref_table))

    lowered = source_column.lower()
    if lowered == "last_update":
        return "updated_at"
    if lowered == "create_date":
        return "created_at"
    return snake_case(source_column)


def bool_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "t", "true", "y", "yes"}:
        return True
    if text in {"0", "f", "false", "n", "no"}:
        return False
    return None


def release_year_value(value: object) -> object:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def normalize_value_for_target(target_column: TargetColumn, value: object) -> object:
    if value is None:
        return None
    if isinstance(value, Decimal):
        integer_like_types = {"dm_id_bigint", "BIGINT", "INTEGER", "SMALLINT"}
        if target_column.type_sql in integer_like_types and value == value.to_integral_value():
            return int(value)
    return value


def target_column_type(
    database_name: str,
    source_table: SourceTable,
    source_column: SourceColumn,
    target_name: str,
    fk: SourceForeignKey | None,
) -> tuple[str, Callable[[dict[str, object]], object] | None]:
    lower = target_name.lower()
    base = source_type_sql(source_column)

    if fk and fk.ref_table in SURROGATE_TABLES.get(database_name, set()):
        return "dm_id_bigint", None

    if lower.endswith("_id"):
        return "dm_id_bigint", None

    if (source_table.name, source_column.name) in BOOL_COLUMNS.get(database_name, set()):
        return "dm_flag", lambda row: bool_value(row[source_column.name])

    if lower == "release_year":
        return "SMALLINT", lambda row: release_year_value(row[source_column.name])

    if lower == "vin_code":
        return "dm_vin_code", None

    if any(token in lower for token in ("price", "cost", "amount", "freight", "rate", "income")):
        return "dm_money", None
    if "discount" in lower or "percent" in lower:
        return "dm_percent", None
    if lower == "quantity":
        return "dm_quantity", None
    if "email" in lower:
        return "dm_email_254", None
    if "phone" in lower or "fax" in lower:
        return "dm_phone_32", None
    if "postal" in lower or "zip" in lower:
        return "dm_postal_code_16", None
    if lower.endswith("city") or lower.endswith("city_name"):
        return "dm_city_60", None
    if lower.endswith("country") or lower.endswith("country_name"):
        return "dm_country_60", None
    if "address" in lower:
        return "dm_address_120", None
    if "title" in lower and "subtitle" not in lower:
        return "dm_title_120", None
    if lower.endswith("_code"):
        if source_column.char_length and source_column.char_length <= 4:
            return "dm_code_4", None
        return "dm_code_16", None
    if "name" in lower and base.startswith("VARCHAR"):
        if (source_column.char_length or 0) <= 25:
            return "dm_name_25", None
        if (source_column.char_length or 0) <= 50:
            return "dm_name_50", None
        if (source_column.char_length or 0) <= 100:
            return "dm_name_100", None
    if "description" in lower and "BLOB SUB_TYPE TEXT" in base:
        return "dm_description_text", None
    return base, None


def build_table_plans(database_name: str, source_tables: dict[str, SourceTable]) -> dict[str, TablePlan]:
    plans: dict[str, TablePlan] = {}
    surrogated = SURROGATE_TABLES.get(database_name, set())
    extra = EXTRA_COLUMNS.get(database_name, {})

    for source_name, source_table in source_tables.items():
        target_name = table_target_name(database_name, source_name)
        surrogate = source_name in surrogated
        pk_columns = [target_column_name(database_name, source_name, col, source_tables) for col in source_table.primary_key]
        columns: list[TargetColumn] = []

        if surrogate:
            columns.append(
                TargetColumn(
                    source_name=None,
                    target_name=surrogate_id_name(target_name),
                    type_sql="dm_id_bigint",
                    not_null=True,
                )
            )

        for column in source_table.columns:
            target_col_name = target_column_name(database_name, source_name, column.name, source_tables)
            fk = find_fk(source_table, column.name)
            type_sql, transform = target_column_type(database_name, source_table, column, target_col_name, fk)
            not_null = bool(column.not_null or column.name in source_table.primary_key)
            if surrogate and column.name in source_table.primary_key:
                not_null = True

            if surrogate and column.name in source_table.primary_key and target_col_name.endswith("_id"):
                target_col_name = target_col_name.replace("_id", "_code")

            columns.append(
                TargetColumn(
                    source_name=column.name,
                    target_name=target_col_name,
                    type_sql=type_sql,
                    not_null=not_null,
                    transform=transform,
                    remap_from_table=fk.ref_table if fk and fk.ref_table in surrogated else None,
                )
            )

        columns.extend(extra.get(source_name, []))

        if surrogate:
            target_pk = [surrogate_id_name(target_name)]
            unique_sets = [[col.target_name for col in columns if col.source_name in source_table.primary_key]]
        else:
            target_pk = pk_columns
            unique_sets = []

        sequence_name = None
        trigger_name = None
        if len(target_pk) == 1:
            pk_name = target_pk[0]
            pk_column = next(col for col in columns if col.target_name == pk_name)
            if pk_column.type_sql == "dm_id_bigint":
                sequence_name = f"seq_{target_name}"
                trigger_name = f"bi_{target_name}"

        plans[source_name] = TablePlan(
            source_name=source_name,
            target_name=target_name,
            columns=columns,
            source_pk=source_table.primary_key,
            target_pk=target_pk,
            surrogate_pk=surrogate,
            unique_sets=unique_sets,
            source_count=source_table.row_count,
            sequence_name=sequence_name,
            trigger_name=trigger_name,
        )

    for source_name, source_table in source_tables.items():
        plan = plans[source_name]
        for fk in source_table.foreign_keys:
            local_columns = [
                next(
                    col.target_name
                    for col in plan.columns
                    if col.source_name == local_name
                )
                for local_name in fk.local_columns
            ]
            ref_plan = plans[fk.ref_table]
            if fk.ref_table in surrogated:
                ref_columns = [surrogate_id_name(ref_plan.target_name)]
            else:
                ref_columns = [
                    next(
                        col.target_name
                        for col in ref_plan.columns
                        if col.source_name == ref_name
                    )
                    for ref_name in fk.ref_columns
                ]
            plan.foreign_keys.append((local_columns, ref_plan.target_name, ref_columns))
    return plans


def topological_order(source_tables: dict[str, SourceTable]) -> list[str]:
    edges: dict[str, set[str]] = defaultdict(set)
    inbound: dict[str, int] = {name: 0 for name in source_tables}
    for name, table in source_tables.items():
        for fk in table.foreign_keys:
            if fk.ref_table == name:
                continue
            if fk.ref_table not in edges[name]:
                edges[fk.ref_table].add(name)
                inbound[name] += 1
    queue = deque(sorted(name for name, degree in inbound.items() if degree == 0))
    order: list[str] = []
    while queue:
        current = queue.popleft()
        order.append(current)
        for neighbor in sorted(edges[current]):
            inbound[neighbor] -= 1
            if inbound[neighbor] == 0:
                queue.append(neighbor)
    if len(order) != len(source_tables):
        remaining = sorted(set(source_tables) - set(order))
        order.extend(remaining)
    return order


def execute_statements(connection, statements: list[str]) -> None:
    cur = connection.cursor()
    executed = False
    for statement in statements:
        cur.execute(statement)
        executed = True
    if executed and connection.main_transaction.is_active():
        connection.commit()


def create_table_statement(plan: TablePlan) -> str:
    lines = []
    for column in plan.columns:
        lines.append(f"  {quote_ident(column.target_name)} {not_null_type(column)}")
    lines.append(
        f"  constraint {quote_ident(sanitize_name('pk_' + plan.target_name, ','.join(plan.target_pk)))} "
        f"primary key ({', '.join(quote_ident(col) for col in plan.target_pk)})"
    )
    for unique_set in plan.unique_sets:
        payload = ",".join(unique_set)
        lines.append(
            f"  constraint {quote_ident(sanitize_name('uq_' + plan.target_name, payload))} "
            f"unique ({', '.join(quote_ident(col) for col in unique_set)})"
        )
    return f"create table {quote_ident(plan.target_name)} (\n" + ",\n".join(lines) + "\n)"


def sequence_restart_value(target_connection, plan: TablePlan) -> int:
    if not plan.target_pk:
        return 1
    cur = target_connection.cursor()
    cur.execute(
        f"select coalesce(max({quote_ident(plan.target_pk[0])}), 0) from {quote_ident(plan.target_name)}"
    )
    return int(cur.fetchone()[0]) + 1


def create_trigger_statement(plan: TablePlan) -> str:
    pk = plan.target_pk[0]
    return f"""
    create trigger {quote_ident(plan.trigger_name)} for {quote_ident(plan.target_name)}
    active before insert position 0
    as
    begin
      if (new.{quote_ident(pk)} is null) then
        new.{quote_ident(pk)} = next value for {quote_ident(plan.sequence_name)};
    end
    """.strip()


def load_rows(
    source_connection,
    target_connection,
    database_name: str,
    source_name: str,
    plan: TablePlan,
    key_maps: dict[str, dict[tuple[object, ...], int]],
) -> int:
    source_table = source_connection.cursor()
    order_by = ""
    if plan.source_pk:
        order_by = " order by " + ", ".join(quote_ident(col) for col in plan.source_pk)
    source_table.execute(f"select * from {quote_ident(source_name)}{order_by}")

    column_names = [col.target_name for col in plan.columns]
    placeholders = ", ".join("?" for _ in column_names)
    insert_sql = (
        f"insert into {quote_ident(plan.target_name)} "
        f"({', '.join(quote_ident(name) for name in column_names)}) "
        f"values ({placeholders})"
    )
    target_cursor = target_connection.cursor()

    batch: list[tuple[object, ...]] = []
    inserted = 0
    wrote_rows = False
    next_surrogate = 1
    index_by_source = {col.source_name: col for col in plan.columns if col.source_name is not None}

    for source_row in source_table:
        row_dict = {desc[0]: source_row[idx] for idx, desc in enumerate(source_table.description)}
        target_row: dict[str, object] = {}

        if plan.surrogate_pk:
            old_key = tuple(row_dict[name] for name in plan.source_pk)
            key_maps[source_name][old_key] = next_surrogate
            target_row[plan.target_pk[0]] = next_surrogate
            next_surrogate += 1

        for source_column, target_column in index_by_source.items():
            value = row_dict[source_column]
            if target_column.remap_from_table:
                if value is None:
                    target_row[target_column.target_name] = None
                else:
                    target_row[target_column.target_name] = key_maps[target_column.remap_from_table][(value,)]
            elif target_column.transform:
                target_row[target_column.target_name] = normalize_value_for_target(
                    target_column,
                    target_column.transform(row_dict),
                )
            else:
                target_row[target_column.target_name] = normalize_value_for_target(target_column, value)

        for extra_column in [col for col in plan.columns if col.source_name is None and col.target_name not in target_row]:
            if extra_column.transform:
                target_row[extra_column.target_name] = normalize_value_for_target(
                    extra_column,
                    extra_column.transform(row_dict),
                )

        batch.append(tuple(target_row.get(name) for name in column_names))
        inserted += 1
        if len(batch) >= 1000:
            target_cursor.executemany(insert_sql, batch)
            wrote_rows = True
            batch.clear()

    if batch:
        target_cursor.executemany(insert_sql, batch)
        wrote_rows = True
    if wrote_rows and target_connection.main_transaction.is_active():
        target_connection.commit()
    return inserted


def create_foreign_keys(target_connection, plan: TablePlan) -> None:
    cur = target_connection.cursor()
    executed = False
    for local_columns, ref_table, ref_columns in plan.foreign_keys:
        statement = (
            f"alter table {quote_ident(plan.target_name)} "
            f"add constraint {quote_ident(sanitize_name('fk_' + plan.target_name, ','.join(local_columns) + ref_table))} "
            f"foreign key ({', '.join(quote_ident(col) for col in local_columns)}) "
            f"references {quote_ident(ref_table)} ({', '.join(quote_ident(col) for col in ref_columns)})"
        )
        cur.execute(statement)
        executed = True
        if local_columns != plan.target_pk:
            index_name = sanitize_name("ix_" + plan.target_name, ",".join(local_columns))
            cur.execute(
                f"create index {quote_ident(index_name)} on {quote_ident(plan.target_name)} "
                f"({', '.join(quote_ident(col) for col in local_columns)})"
            )
            executed = True
    if executed and target_connection.main_transaction.is_active():
        target_connection.commit()


def apply_native_extras(target_connection, database_name: str) -> None:
    cur = target_connection.cursor()
    executed = False
    for plan_sql in PROCEDURES.get(database_name, []):
        cur.execute(plan_sql)
        executed = True
    for view_sql in VIEWS.get(database_name, []):
        cur.execute(view_sql)
        executed = True
    if executed and target_connection.main_transaction.is_active():
        target_connection.commit()


def build_database(database_name: str, overwrite: bool) -> dict[str, object]:
    source_path = SOURCE_ROOT / f"{database_name}.fdb"
    target_path = TARGET_ROOT / f"{database_name}_native.fdb"
    TARGET_ROOT.mkdir(parents=True, exist_ok=True)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    if overwrite and target_path.exists():
        target_path.unlink()

    source_tables = load_source_schema(source_path)
    plans = build_table_plans(database_name, source_tables)
    order = topological_order(source_tables)

    target_connection = create_database(dsn_for(target_path), user=USER, password=PASSWORD, charset=CHARSET)
    execute_statements(target_connection, split_sql_script(DOMAINS_SQL.read_text(encoding="utf-8")))
    execute_statements(target_connection, [create_table_statement(plans[name]) for name in order])

    source_connection = connect(dsn_for(source_path), user=USER, password=PASSWORD, charset=CHARSET)
    key_maps: dict[str, dict[tuple[object, ...], int]] = defaultdict(dict)
    load_summary = []
    for name in order:
        print(
            f"[loading] {database_name}.{plans[name].target_name} rows={plans[name].source_count}",
            flush=True,
        )
        inserted = load_rows(source_connection, target_connection, database_name, name, plans[name], key_maps)
        load_summary.append(
            {
                "source_table": name,
                "target_table": plans[name].target_name,
                "row_count": inserted,
            }
        )

    for name in order:
        plan = plans[name]
        if plan.sequence_name:
            cur = target_connection.cursor()
            cur.execute(f'create sequence {quote_ident(plan.sequence_name)}')
            cur.execute(f'alter sequence {quote_ident(plan.sequence_name)} restart with {sequence_restart_value(target_connection, plan)}')
            cur.execute(create_trigger_statement(plan))
            target_connection.commit()

    for name in order:
        create_foreign_keys(target_connection, plans[name])

    apply_native_extras(target_connection, database_name)

    target_connection.close()
    source_connection.close()

    report = {
        "database": database_name,
        "source": str(source_path),
        "target": str(target_path),
        "tables": load_summary,
    }
    report_path = REPORT_ROOT / f"{database_name}_native_build.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build curated Firebird-native fixture databases.")
    parser.add_argument(
        "--database",
        choices=["all", *DATABASE_NAMES],
        default="all",
        help="Which curated native database to build.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing native databases before building.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    targets = list(DATABASE_NAMES) if args.database == "all" else [args.database]
    for database_name in targets:
        print(f"[building] {database_name}", flush=True)
        report = build_database(database_name, overwrite=args.overwrite)
        print(f"[built] {report['database']} -> {report['target']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
