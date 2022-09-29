from datetime import datetime

from google.cloud import bigquery

DATASET = "BingAds"


def add_batched_at(rows: list[dict], schema: list[dict]):
    return (
        [
            {**row, "_batched_at": datetime.utcnow().isoformat(timespec="seconds")}
            for row in rows
        ],
        [*schema, {"name": "_batched_at", "type": "TIMESTAMP"}],
    )


def load(table: str, schema: list[dict]):
    def _load(rows: list[dict]) -> int:
        if not rows:
            return 0

        client = bigquery.Client()

        _rows, _schema = add_batched_at(rows, schema)

        output_rows = (
            client.load_table_from_json(
                _rows,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    schema=_schema,
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                ),
            )
            .result()
            .output_rows
        )

        return output_rows

    return _load
