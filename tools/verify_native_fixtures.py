#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from firebird.driver import connect


def load_builder_module():
    module_name = "build_native_fixtures"
    if module_name in sys.modules:
        return sys.modules[module_name]
    module_path = Path(__file__).with_name("build_native_fixtures.py")
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load builder module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


builder = load_builder_module()

EXPECTED_METADATA = {
    "car_database": {
        "foreign_keys": 15,
        "triggers": 8,
        "sequences": 8,
        "views": [],
        "procedures": ["SP_REGISTER_VEHICLE_SALE"],
    },
    "chinook": {
        "foreign_keys": 11,
        "triggers": 10,
        "sequences": 10,
        "views": ["vw_customer_purchase_history"],
        "procedures": ["SP_ADD_INVOICE_LINE"],
    },
    "northwind": {
        "foreign_keys": 13,
        "triggers": 10,
        "sequences": 10,
        "views": ["vw_sales_by_category"],
        "procedures": ["SP_SHIP_ORDER"],
    },
    "sakila_master": {
        "foreign_keys": 22,
        "triggers": 14,
        "sequences": 14,
        "views": ["vw_customer_balance"],
        "procedures": ["SP_POST_PAYMENT"],
    },
}


@dataclass
class VerificationResult:
    database: str
    rebuilt: bool
    build_seconds: float | None
    source_table_count: int
    target_table_count: int
    source_total_rows: int
    target_total_rows: int
    metadata: dict[str, Any]
    smoke: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "rebuilt": self.rebuilt,
            "build_seconds": self.build_seconds,
            "source_table_count": self.source_table_count,
            "target_table_count": self.target_table_count,
            "source_total_rows": self.source_total_rows,
            "target_total_rows": self.target_total_rows,
            "metadata": self.metadata,
            "smoke": self.smoke,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify curated Firebird-native fixture databases.")
    parser.add_argument(
        "--database",
        choices=["all", *builder.DATABASE_NAMES],
        default="all",
        help="Which curated native database to verify.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild the curated native database before verification.",
    )
    parser.add_argument(
        "--skip-smoke",
        action="store_true",
        help="Skip procedure/view smoke tests.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit verification results as JSON.",
    )
    return parser.parse_args()


def target_path(database_name: str) -> Path:
    return builder.TARGET_ROOT / f"{database_name}_native.fdb"


def report_path(database_name: str) -> Path:
    return builder.REPORT_ROOT / f"{database_name}_native_build.json"


def open_connection(path: Path):
    return connect(builder.dsn_for(path), user=builder.USER, password=builder.PASSWORD, charset=builder.CHARSET)


def fetch_scalar(connection, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    cur = connection.cursor()
    try:
        cur.execute(sql, parameters)
        row = cur.fetchone()
        if row is None:
            raise AssertionError(f"No row returned for scalar query: {sql}")
        return row[0]
    finally:
        cur.close()


def fetch_names(connection, sql: str, parameters: tuple[Any, ...] = ()) -> list[str]:
    cur = connection.cursor()
    try:
        cur.execute(sql, parameters)
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def user_tables(connection) -> list[str]:
    return fetch_names(
        connection,
        """
        select trim(rdb$relation_name)
        from rdb$relations
        where rdb$system_flag = 0 and rdb$view_blr is null
        order by 1
        """,
    )


def count_rows(connection, table_name: str) -> int:
    return int(fetch_scalar(connection, f"select count(*) from {builder.quote_ident(table_name)}"))


def verify_row_parity(database_name: str) -> tuple[int, int, int, int]:
    source_connection = open_connection(builder.SOURCE_ROOT / f"{database_name}.fdb")
    target_connection = open_connection(target_path(database_name))
    try:
        report = json.loads(report_path(database_name).read_text(encoding="utf-8"))
        source_tables = user_tables(source_connection)
        target_tables = user_tables(target_connection)
        source_total_rows = 0
        target_total_rows = 0
        for entry in report["tables"]:
            source_count = count_rows(source_connection, entry["source_table"])
            target_count = count_rows(target_connection, entry["target_table"])
            expected = int(entry["row_count"])
            if source_count != expected or target_count != expected:
                raise AssertionError(
                    f"{database_name}: row parity mismatch for "
                    f"{entry['source_table']}->{entry['target_table']} "
                    f"(source={source_count}, report={expected}, target={target_count})"
                )
            source_total_rows += source_count
            target_total_rows += target_count
        return len(source_tables), len(target_tables), source_total_rows, target_total_rows
    finally:
        source_connection.close()
        target_connection.close()


def verify_id_domains(connection, database_name: str) -> None:
    cur = connection.cursor()
    try:
        cur.execute(
            """
            select trim(rel.rdb$relation_name), trim(rf.rdb$field_name), trim(f.rdb$field_name)
            from rdb$relation_fields rf
            join rdb$fields f on rf.rdb$field_source = f.rdb$field_name
            join rdb$relations rel on rel.rdb$relation_name = rf.rdb$relation_name
            where rel.rdb$system_flag = 0
              and rel.rdb$view_blr is null
              and rf.rdb$field_name containing '_id'
              and trim(f.rdb$field_name) <> 'DM_ID_BIGINT'
            order by 1, 2
            """
        )
        offenders = cur.fetchall()
        if offenders:
            raise AssertionError(f"{database_name}: identifier domain drift detected: {offenders}")
    finally:
        cur.close()


def verify_fk_type_compatibility(connection, database_name: str) -> None:
    cur = connection.cursor()
    try:
        cur.execute(
            """
            select trim(rc.rdb$relation_name),
                   trim(sg.rdb$field_name),
                   trim(i2.rdb$relation_name),
                   trim(sg2.rdb$field_name)
            from rdb$relation_constraints rc
            join rdb$indices i on i.rdb$index_name = rc.rdb$index_name
            join rdb$index_segments sg on sg.rdb$index_name = i.rdb$index_name
            join rdb$ref_constraints ref on ref.rdb$constraint_name = rc.rdb$constraint_name
            join rdb$relation_constraints rc2 on rc2.rdb$constraint_name = ref.rdb$const_name_uq
            join rdb$indices i2 on i2.rdb$index_name = rc2.rdb$index_name
            join rdb$index_segments sg2
              on sg2.rdb$index_name = i2.rdb$index_name
             and sg2.rdb$field_position = sg.rdb$field_position
            where rc.rdb$constraint_type = 'FOREIGN KEY'
            order by 1, sg.rdb$field_position
            """
        )
        mismatches: list[tuple[Any, ...]] = []
        for relation_name, field_name, ref_relation_name, ref_field_name in cur.fetchall():
            field_meta = field_metadata(connection, relation_name, field_name)
            ref_meta = field_metadata(connection, ref_relation_name, ref_field_name)
            if field_meta[1:] != ref_meta[1:]:
                mismatches.append((relation_name, field_name, field_meta, ref_relation_name, ref_field_name, ref_meta))
        if mismatches:
            raise AssertionError(f"{database_name}: foreign-key type mismatch detected: {mismatches}")
    finally:
        cur.close()


def field_metadata(connection, relation_name: str, field_name: str) -> tuple[Any, ...]:
    cur = connection.cursor()
    try:
        cur.execute(
            """
            select trim(f.rdb$field_name),
                   f.rdb$field_type,
                   coalesce(f.rdb$field_sub_type, -1),
                   coalesce(f.rdb$field_precision, -1),
                   coalesce(f.rdb$field_scale, 0),
                   coalesce(f.rdb$character_length, -1)
            from rdb$relation_fields rf
            join rdb$fields f on rf.rdb$field_source = f.rdb$field_name
            where rf.rdb$relation_name = ? and rf.rdb$field_name = ?
            """,
            (relation_name, field_name),
        )
        row = cur.fetchone()
        if row is None:
            raise AssertionError(f"Unable to read field metadata for {relation_name}.{field_name}")
        return row
    finally:
        cur.close()


def metadata_summary(connection, database_name: str) -> dict[str, Any]:
    expected = EXPECTED_METADATA[database_name]
    procedures = fetch_names(
        connection,
        "select trim(rdb$procedure_name) from rdb$procedures where rdb$system_flag = 0 order by 1",
    )
    views = fetch_names(
        connection,
        """
        select trim(rdb$relation_name)
        from rdb$relations
        where rdb$system_flag = 0 and rdb$view_blr is not null
        order by 1
        """,
    )
    summary = {
        "foreign_keys": int(
            fetch_scalar(
                connection,
                "select count(*) from rdb$relation_constraints where rdb$constraint_type = 'FOREIGN KEY'",
            )
        ),
        "triggers": int(
            fetch_scalar(connection, "select count(*) from rdb$triggers where rdb$system_flag = 0")
        ),
        "sequences": int(
            fetch_scalar(connection, "select count(*) from rdb$generators where rdb$system_flag = 0")
        ),
        "views": views,
        "procedures": procedures,
    }
    if summary["foreign_keys"] != expected["foreign_keys"]:
        raise AssertionError(
            f"{database_name}: expected {expected['foreign_keys']} foreign keys, got {summary['foreign_keys']}"
        )
    if summary["triggers"] != expected["triggers"]:
        raise AssertionError(
            f"{database_name}: expected {expected['triggers']} triggers, got {summary['triggers']}"
        )
    if summary["sequences"] != expected["sequences"]:
        raise AssertionError(
            f"{database_name}: expected {expected['sequences']} sequences, got {summary['sequences']}"
        )
    if summary["views"] != expected["views"]:
        raise AssertionError(f"{database_name}: expected views {expected['views']}, got {summary['views']}")
    if summary["procedures"] != expected["procedures"]:
        raise AssertionError(
            f"{database_name}: expected procedures {expected['procedures']}, got {summary['procedures']}"
        )
    return summary


def smoke_test(database_name: str) -> dict[str, Any]:
    connection = open_connection(target_path(database_name))
    cur = None
    try:
        cur = connection.cursor()
        if database_name == "car_database":
            before = count_rows(connection, "vehicle_ownership")
            cur.execute("execute procedure sp_register_vehicle_sale(1, 1, 1, '2026-01-01', 12345.67)")
            after = count_rows(connection, "vehicle_ownership")
            connection.rollback()
            if after != before + 1:
                raise AssertionError(f"{database_name}: procedure smoke test did not insert a row")
            return {"procedure_delta": after - before}
        if database_name == "chinook":
            before = count_rows(connection, "invoice_item")
            cur.execute("execute procedure sp_add_invoice_line(1, 1, 0.99, 1)")
            after = count_rows(connection, "invoice_item")
            view_rows = count_rows(connection, "vw_customer_purchase_history")
            connection.rollback()
            if after != before + 1 or view_rows <= 0:
                raise AssertionError(f"{database_name}: smoke test failed")
            return {"procedure_delta": after - before, "view_rows": view_rows}
        if database_name == "northwind":
            cur.execute('select first 1 "sales_order_id" from "sales_order" order by "sales_order_id"')
            sales_order_id = int(cur.fetchone()[0])
            cur.execute("execute procedure sp_ship_order(?, current_timestamp, 22.50)", (sales_order_id,))
            cur.execute('select "shipped_date", "freight" from "sales_order" where "sales_order_id" = ?', (sales_order_id,))
            shipped_date, freight = cur.fetchone()
            view_rows = count_rows(connection, "vw_sales_by_category")
            connection.rollback()
            if shipped_date is None or Decimal(freight) != Decimal("22.5000") or view_rows <= 0:
                raise AssertionError(f"{database_name}: smoke test failed")
            return {"sales_order_id": sales_order_id, "view_rows": view_rows}
        if database_name == "sakila_master":
            before = count_rows(connection, "payment")
            cur.execute("execute procedure sp_post_payment(1, 1, 1, 4.99, current_timestamp)")
            after = count_rows(connection, "payment")
            view_rows = count_rows(connection, "vw_customer_balance")
            connection.rollback()
            if after != before + 1 or view_rows <= 0:
                raise AssertionError(f"{database_name}: smoke test failed")
            return {"procedure_delta": after - before, "view_rows": view_rows}
        raise AssertionError(f"Unsupported smoke test database: {database_name}")
    finally:
        if cur is not None:
            cur.close()
        connection.close()


def verify_database(database_name: str, rebuild: bool, include_smoke: bool) -> VerificationResult:
    build_seconds: float | None = None
    if rebuild:
        started = time.monotonic()
        builder.build_database(database_name, overwrite=True)
        build_seconds = round(time.monotonic() - started, 3)

    target = target_path(database_name)
    report = report_path(database_name)
    if not target.exists():
        raise AssertionError(f"{database_name}: missing curated database {target}")
    if not report.exists():
        raise AssertionError(f"{database_name}: missing build report {report}")

    source_table_count, target_table_count, source_total_rows, target_total_rows = verify_row_parity(database_name)
    connection = open_connection(target)
    try:
        metadata = metadata_summary(connection, database_name)
        verify_id_domains(connection, database_name)
        verify_fk_type_compatibility(connection, database_name)
    finally:
        connection.close()

    smoke = smoke_test(database_name) if include_smoke else None
    return VerificationResult(
        database=database_name,
        rebuilt=rebuild,
        build_seconds=build_seconds,
        source_table_count=source_table_count,
        target_table_count=target_table_count,
        source_total_rows=source_total_rows,
        target_total_rows=target_total_rows,
        metadata=metadata,
        smoke=smoke,
    )


def verify_many(database_names: list[str], rebuild: bool, include_smoke: bool) -> list[VerificationResult]:
    return [verify_database(database_name, rebuild=rebuild, include_smoke=include_smoke) for database_name in database_names]


def main() -> int:
    args = parse_args()
    targets = list(builder.DATABASE_NAMES) if args.database == "all" else [args.database]
    results = verify_many(targets, rebuild=args.rebuild, include_smoke=not args.skip_smoke)
    if args.json:
        print(json.dumps([result.as_dict() for result in results], indent=2))
    else:
        for result in results:
            build_part = (
                f"build_seconds={result.build_seconds} "
                if result.build_seconds is not None
                else ""
            )
            print(
                f"[verified] {result.database} "
                f"rebuilt={result.rebuilt} "
                f"{build_part}"
                f"source_rows={result.source_total_rows} "
                f"target_rows={result.target_total_rows} "
                f"fks={result.metadata['foreign_keys']} "
                f"triggers={result.metadata['triggers']} "
                f"sequences={result.metadata['sequences']}"
            )
            if result.smoke is not None:
                print(f"  smoke={result.smoke}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
