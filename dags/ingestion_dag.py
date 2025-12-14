from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


CAT_API_BASE = "https://api.thecatapi.com/v1"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # TheCatAPI иногда отдает ISO
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def fetch_json(url: str, headers: Dict[str, str], params: Dict[str, Any] | None = None, timeout: int = 30) -> Any:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="cats_ingestion_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cats", "ingestion"],
) as dag:

    @task
    def ingest_breeds() -> int:
        api_key = os.environ.get("CAT_API_KEY", "").strip()
        if not api_key:
            raise ValueError("CAT_API_KEY is empty. Put it into .env")

        headers = {"x-api-key": api_key}
        data = fetch_json(f"{CAT_API_BASE}/breeds", headers=headers)

        rows = []
        now = utcnow()

        for b in data:
            rows.append(
                (
                    str(b.get("id")),
                    b.get("name"),
                    b.get("origin"),
                    b.get("temperament"),
                    b.get("life_span"),
                    json.dumps(b, ensure_ascii=False),
                    now,
                )
            )

        if not rows:
            return 0

        hook = PostgresHook(postgres_conn_id="postgres_dw")
        sql = """
        INSERT INTO raw.breeds (id, name, origin, temperament, life_span, raw_json, loaded_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name,
          origin = EXCLUDED.origin,
          temperament = EXCLUDED.temperament,
          life_span = EXCLUDED.life_span,
          raw_json = EXCLUDED.raw_json,
          loaded_at = EXCLUDED.loaded_at;
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=500)
            conn.commit()

        return len(rows)

    @task
    def ingest_images(max_pages: int = 10, limit_per_page: int = 100) -> int:
        """
        Забираем изображения с породами.
        TheCatAPI images/search возвращает пачками; используем page + order=ASC.
        max_pages ограничивает объем (не бесконечный).
        """
        api_key = os.environ.get("CAT_API_KEY", "").strip()
        if not api_key:
            raise ValueError("CAT_API_KEY is empty. Put it into .env")

        headers = {"x-api-key": api_key}
        hook = PostgresHook(postgres_conn_id="postgres_dw")

        total = 0
        now = utcnow()

        insert_rows = []

        for page in range(max_pages):
            params = {
                "limit": limit_per_page,
                "page": page,
                "order": "ASC",
                "has_breeds": 1,
            }
            batch = fetch_json(f"{CAT_API_BASE}/images/search", headers=headers, params=params)

            if not batch:
                break

            for img in batch:
                img_id = str(img.get("id"))
                url = img.get("url")
                width = img.get("width")
                height = img.get("height")

                # breed_id: в images есть breeds: [{id, name, ...}]
                breeds = img.get("breeds") or []
                breed_id = None
                if breeds and isinstance(breeds, list):
                    breed_id = breeds[0].get("id")

                created_at = parse_ts(img.get("created_at"))  # может быть None

                insert_rows.append(
                    (
                        img_id,
                        breed_id,
                        url,
                        int(width) if width is not None else None,
                        int(height) if height is not None else None,
                        created_at,
                        json.dumps(img, ensure_ascii=False),
                        now,
                    )
                )

            total += len(batch)

        if not insert_rows:
            return 0

        sql = """
        INSERT INTO raw.images (id, breed_id, url, width, height, created_at, raw_json, loaded_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
          breed_id = EXCLUDED.breed_id,
          url = EXCLUDED.url,
          width = EXCLUDED.width,
          height = EXCLUDED.height,
          created_at = EXCLUDED.created_at,
          raw_json = EXCLUDED.raw_json,
          loaded_at = EXCLUDED.loaded_at;
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, insert_rows, page_size=500)
            conn.commit()

        return total

    b = ingest_breeds()
    i = ingest_images()
    b >> i
