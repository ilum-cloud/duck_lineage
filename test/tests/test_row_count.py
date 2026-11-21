"""
Tests for outputStatistics.rowCount on COMPLETE events.

Without RETURNING, DML statements return a single row holding the number of rows they wrote, so rowCount must
come from that value rather than from the number of result rows (which used to make it 1 for every statement).

The events are captured with the file transport, so these tests do not need Marquez.
"""

import pytest


def _row_counts(event):
    return [o["outputFacets"]["outputStatistics"]["rowCount"] for o in event.get("outputs", [])]


# (setup statements, statement under test, expected rowCount). {dir} is replaced by a temporary directory.
_CASES = {
    "ctas": ([], "CREATE TABLE rc AS SELECT range AS id FROM range(100)", 100),
    "ctas_empty": (
        [],
        "CREATE TABLE rc AS SELECT range AS id FROM range(100) LIMIT 0",
        0,
    ),
    "insert": (
        ["CREATE TABLE rc (id BIGINT)"],
        "INSERT INTO rc SELECT * FROM range(7)",
        7,
    ),
    "insert_returning": (
        ["CREATE TABLE rc (id BIGINT)"],
        "INSERT INTO rc SELECT * FROM range(3) RETURNING id",
        3,
    ),
    "update": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "UPDATE rc SET id = id + 1000 WHERE id < 20",
        20,
    ),
    "update_returning": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "UPDATE rc SET id = id + 1000 WHERE id < 4 RETURNING id",
        4,
    ),
    "delete": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "DELETE FROM rc WHERE id >= 90",
        10,
    ),
    "delete_nothing": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "DELETE FROM rc WHERE id < 0",
        0,
    ),
    "copy_to": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "COPY rc TO '{dir}/rc.parquet' (FORMAT parquet)",
        100,
    ),
    "copy_to_return_files": (
        ["CREATE TABLE rc AS SELECT range AS id FROM range(100)"],
        "COPY rc TO '{dir}/rc.parquet' (FORMAT parquet, RETURN_FILES true)",
        100,
    ),
    "copy_to_partitioned_return_stats": (
        ["CREATE TABLE rc AS SELECT range AS id, range % 4 AS p FROM range(100)"],
        "COPY rc TO '{dir}/rc_part' (FORMAT parquet, PARTITION_BY (p), RETURN_STATS true)",
        100,
    ),
    "merge": (
        [
            "CREATE TABLE rc (id INT PRIMARY KEY, v INT)",
            "INSERT INTO rc SELECT range, 0 FROM range(10)",
            "CREATE TABLE rc_src AS SELECT range::INT AS id FROM range(5, 15)",
        ],
        "MERGE INTO rc USING rc_src ON rc.id = rc_src.id "
        "WHEN MATCHED THEN UPDATE SET v = 1 WHEN NOT MATCHED THEN INSERT VALUES (rc_src.id, 2)",
        10,
    ),
    "insert_or_replace": (
        ["CREATE TABLE rc (id INT PRIMARY KEY, v INT)", "INSERT INTO rc SELECT range, 0 FROM range(10)"],
        "INSERT OR REPLACE INTO rc SELECT range, 1 FROM range(12)",
        12,
    ),
    "insert_on_conflict_do_nothing": (
        ["CREATE TABLE rc (id INT PRIMARY KEY, v INT)", "INSERT INTO rc SELECT range, 0 FROM range(10)"],
        "INSERT INTO rc SELECT range, 1 FROM range(15) ON CONFLICT DO NOTHING",
        5,
    ),
    "copy_from": (
        [
            "CREATE TABLE rc_src AS SELECT range AS id FROM range(100)",
            "COPY rc_src TO '{dir}/rc.csv'",
            "CREATE TABLE rc (id BIGINT)",
        ],
        "COPY rc FROM '{dir}/rc.csv'",
        100,
    ),
}


@pytest.mark.integration
@pytest.mark.parametrize("case", list(_CASES))
def test_row_count_is_rows_written(file_lineage, case):
    setup, statement, expected = _CASES[case]
    for sql in setup:
        file_lineage.conn.execute(sql.format(dir=file_lineage.dir))
    statement = statement.format(dir=file_lineage.dir)
    file_lineage.conn.execute(statement).fetchall()

    row_counts = _row_counts(file_lineage.event(statement))
    assert row_counts, "COMPLETE event has no output datasets"
    assert row_counts == [expected] * len(row_counts)


@pytest.mark.integration
def test_select_has_no_output_statistics(file_lineage):
    """A plain SELECT writes nothing, so it has no outputs (and no rowCount) at all."""
    file_lineage.conn.execute("CREATE TABLE rc AS SELECT range AS id FROM range(100)")
    file_lineage.conn.execute("SELECT * FROM rc").fetchall()
    assert file_lineage.event("SELECT * FROM rc").get("outputs", []) == []
