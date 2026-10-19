from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


LANDING_DIR = Path("/opt/airflow/data/landing")
RAW_DIR = Path("/opt/airflow/datalake/raw/sales")
STAGED_DIR = Path("/opt/airflow/datalake/staged/sales")
DWH_CONN_ID = "dwh_postgres"

STG_COLUMNS = [
    "order_id", "order_date", "store_id", "store_name",
    "barangay", "city", "product_id", "product_name",
    "category", "quantity", "unit_price", "line_total", "batch_id"
]


@dag(
    dag_id="sarisari_sales_etl",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["sarisari", "etl"],
)
def sarisari_sales_etl():

    @task
    def extract():
        run_stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        ingest_date = datetime.now().strftime("%Y%m%d")
        batch_dir = RAW_DIR / f"ingest_date={ingest_date}" / f"batch={run_stamp}"
        batch_dir.mkdir(parents=True, exist_ok=True)

        csv_files = sorted(LANDING_DIR.glob("sales_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No sales CSV files found in {LANDING_DIR}")

        copied_files = []
        for csv_file in csv_files:
            destination = batch_dir / csv_file.name
            shutil.copy2(csv_file, destination)
            copied_files.append(str(destination))

        print(f"Copied {len(copied_files)} CSV file(s) into {batch_dir}")
        return {
            "batch_id": run_stamp,
            "raw_files": copied_files,
        }

    @task
    def transform(extract_result: dict):
        batch_id = extract_result["batch_id"]
        frames = []

        for raw_file in extract_result["raw_files"]:
            frame = pd.read_csv(raw_file, dtype=str, keep_default_na=False)
            frame["source_file"] = Path(raw_file).name
            frames.append(frame)

        df = pd.concat(frames, ignore_index=True)
        rows_in = len(df)

        for column in df.columns:
            if column != "source_file":
                df[column] = df[column].astype(str).str.strip()

        df["category"] = df["category"].str.title()
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")

        reasons = pd.Series("", index=df.index, dtype=str)

        def add_reason(mask, message):
            selected = mask & reasons.eq("")
            reasons.loc[selected] = message

        add_reason(df["order_id"].eq(""), "missing order_id")
        add_reason(df["order_date"].isna(), "missing or invalid order_date")
        add_reason(df["store_id"].eq(""), "missing store_id")
        add_reason(df["product_id"].eq(""), "missing product_id")
        add_reason(df["quantity_num"].isna(), "missing or invalid quantity")
        add_reason(df["unit_price_num"].isna(), "missing or invalid unit_price")
        add_reason(df["quantity_num"].le(0), "quantity must be positive")
        add_reason(df["unit_price_num"].lt(0), "unit_price cannot be negative")

        rejected = df[reasons.ne("")].copy()
        rejected["rejection_reason"] = reasons[reasons.ne("")]

        if not rejected.empty:
            print(f"Rejected {len(rejected)} invalid row(s):")
            for _, row in rejected.iterrows():
                print(
                    f"REJECTED source={row['source_file']} "
                    f"order_id={row['order_id']} "
                    f"product_id={row['product_id']} "
                    f"reason={row['rejection_reason']}"
                )

        clean = df[reasons.eq("")].copy()

        duplicate_mask = clean.duplicated(
            subset=["order_id", "product_id"],
            keep="first",
        )
        duplicates = clean[duplicate_mask].copy()

        if not duplicates.empty:
            print(f"Rejected {len(duplicates)} duplicate row(s):")
            for _, row in duplicates.iterrows():
                print(
                    f"REJECTED source={row['source_file']} "
                    f"order_id={row['order_id']} "
                    f"product_id={row['product_id']} "
                    f"reason=duplicate order_id and product_id"
                )

        clean = clean[~duplicate_mask].copy()
        total_rejected = len(rejected) + len(duplicates)

        clean["order_date"] = clean["order_date"].dt.date
        clean["quantity"] = clean["quantity_num"].astype(int)
        clean["unit_price"] = clean["unit_price_num"].round(2)
        clean["line_total"] = (
            clean["quantity"] * clean["unit_price"]
        ).round(2)
        clean["batch_id"] = batch_id

        clean = clean[STG_COLUMNS]

        STAGED_DIR.mkdir(parents=True, exist_ok=True)
        parquet_path = STAGED_DIR / f"sales_{batch_id}.parquet"
        clean.to_parquet(parquet_path, index=False)

        print(f"Rows in: {rows_in}")
        print(f"Rows rejected (invalid or duplicate): {total_rejected}")
        print(f"Rows kept: {len(clean)}")
        print(f"Rows rejected for invalid values: {len(rejected)}")
        print(f"Duplicate rows removed: {len(duplicates)}")
        print(f"Staged Parquet: {parquet_path}")

        return {
            "batch_id": batch_id,
            "parquet_path": str(parquet_path),
            "rows_kept": len(clean),
        }

    @task
    def load(transform_result: dict):
        batch_id = transform_result["batch_id"]
        df = pd.read_parquet(transform_result["parquet_path"])

        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE staging.stg_sales")

                records = [
                    tuple(None if pd.isna(value) else value for value in row)
                    for row in df.itertuples(index=False, name=None)
                ]

                insert_sql = """
                    INSERT INTO staging.stg_sales (
                        order_id, order_date, store_id, store_name,
                        barangay, city, product_id, product_name,
                        category, quantity, unit_price, line_total, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, records)

                cursor.execute("""
                    INSERT INTO dw.dim_date (
                        date_key, full_date, day_of_week, is_weekend,
                        month, month_name, quarter, year
                    )
                    SELECT DISTINCT
                        TO_CHAR(order_date, 'YYYYMMDD')::INTEGER,
                        order_date,
                        TO_CHAR(order_date, 'FMDay'),
                        EXTRACT(ISODOW FROM order_date) IN (6, 7),
                        EXTRACT(MONTH FROM order_date)::INTEGER,
                        TO_CHAR(order_date, 'FMMonth'),
                        EXTRACT(QUARTER FROM order_date)::INTEGER,
                        EXTRACT(YEAR FROM order_date)::INTEGER
                    FROM staging.stg_sales
                    ON CONFLICT (date_key) DO NOTHING
                """)

                cursor.execute("""
                    INSERT INTO dw.dim_store (
                        store_id, store_name, barangay, city
                    )
                    SELECT DISTINCT store_id, store_name, barangay, city
                    FROM staging.stg_sales
                    ON CONFLICT (store_id) DO UPDATE SET
                        store_name = EXCLUDED.store_name,
                        barangay = EXCLUDED.barangay,
                        city = EXCLUDED.city
                """)

                cursor.execute("""
                    INSERT INTO dw.dim_product (
                        product_id, product_name, category
                    )
                    SELECT DISTINCT product_id, product_name, category
                    FROM staging.stg_sales
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        category = EXCLUDED.category
                """)

                cursor.execute("""
                    INSERT INTO dw.fact_sales (
                        order_id, date_key, store_key, product_key,
                        quantity, unit_price, line_total, batch_id
                    )
                    SELECT
                        s.order_id,
                        TO_CHAR(s.order_date, 'YYYYMMDD')::INTEGER,
                        st.store_key,
                        p.product_key,
                        s.quantity,
                        s.unit_price,
                        s.line_total,
                        s.batch_id
                    FROM staging.stg_sales s
                    JOIN dw.dim_store st ON st.store_id = s.store_id
                    JOIN dw.dim_product p ON p.product_id = s.product_id
                    ON CONFLICT (order_id, product_key) DO UPDATE SET
                        date_key = EXCLUDED.date_key,
                        store_key = EXCLUDED.store_key,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        line_total = EXCLUDED.line_total,
                        batch_id = EXCLUDED.batch_id
                """)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"Loaded batch {batch_id} with {len(df)} staged row(s)")
        return {"batch_id": batch_id, "rows_kept": len(df)}

    @task
    def quality_check(load_result: dict):
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        checks = [
            (
                "fact_sales is not empty",
                "SELECT COUNT(*) FROM dw.fact_sales",
                lambda count: count > 0,
            ),
            (
                "current batch row count matches fact table",
                """
                SELECT
                    (SELECT COUNT(*) FROM staging.stg_sales WHERE batch_id = %s),
                    (SELECT COUNT(*) FROM dw.fact_sales WHERE batch_id = %s)
                """,
                lambda counts: counts[0] == counts[1],
            ),
            (
                "no non-positive quantities",
                "SELECT COUNT(*) FROM dw.fact_sales WHERE quantity <= 0",
                lambda count: count == 0,
            ),
            (
                "line totals are correct",
                """
                SELECT COUNT(*) FROM dw.fact_sales
                WHERE line_total <> quantity * unit_price
                """,
                lambda count: count == 0,
            ),
            (
                "no future sale dates",
                "SELECT COUNT(*) FROM dw.fact_sales f JOIN dw.dim_date d ON f.date_key = d.date_key WHERE d.full_date > CURRENT_DATE",
                lambda count: count == 0,
            ),
        ]

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for label, sql, test in checks:
                    if "current batch row count" in label:
                        cursor.execute(
                            sql,
                            (load_result["batch_id"], load_result["batch_id"]),
                        )
                    else:
                        cursor.execute(sql)

                    result = cursor.fetchone()
                    value = result[0] if len(result) == 1 else result
                    if not test(value):
                        raise ValueError(f"Quality check failed: {label}; result={value}")
                    print(f"PASS: {label} -> {value}")

        print("All five quality checks passed.")

    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)
    quality_check(loaded)


sarisari_sales_etl()
