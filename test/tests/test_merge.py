"""
Tests for lineage of MERGE INTO, and of INSERT ... ON CONFLICT / INSERT OR REPLACE, which DuckDB plans as a merge.

The target table must be an output dataset. These statements used to produce events without any outputs.
The events are captured with the file transport, so these tests do not need Marquez.
"""

import pytest

_SETUP = [
    "CREATE TABLE mt (id INT PRIMARY KEY, v INT)",
    "INSERT INTO mt SELECT range, 0 FROM range(10)",
    "CREATE TABLE ms AS SELECT range::INT AS id, 7 AS v FROM range(5, 15)",
]

_STATEMENTS = {
    "merge": "MERGE INTO mt USING ms ON mt.id = ms.id "
    "WHEN MATCHED THEN UPDATE SET v = ms.v WHEN NOT MATCHED THEN INSERT VALUES (ms.id, ms.v)",
    "merge_delete_returning": "MERGE INTO mt USING ms ON mt.id = ms.id WHEN MATCHED THEN DELETE RETURNING *",
    "insert_or_replace": "INSERT OR REPLACE INTO mt SELECT range, 3 FROM range(12)",
    "insert_on_conflict_do_nothing": "INSERT INTO mt SELECT range, 4 FROM range(20) ON CONFLICT DO NOTHING",
    "insert_on_conflict_do_update": "INSERT INTO mt SELECT range, 5 FROM range(3) "
    "ON CONFLICT (id) DO UPDATE SET v = excluded.v",
}


@pytest.mark.integration
@pytest.mark.parametrize("case", list(_STATEMENTS))
def test_merge_target_is_output(file_lineage, case):
    for sql in _SETUP:
        file_lineage.conn.execute(sql)
    statement = _STATEMENTS[case]
    file_lineage.conn.execute(statement).fetchall()

    start = file_lineage.event(statement, "START")
    outputs = start.get("outputs", [])
    assert [o["name"] for o in outputs] == ["memory.main.mt"]

    facets = outputs[0]["facets"]
    assert facets["lifecycleStateChange"]["lifecycleStateChange"] == "OVERWRITE"
    assert [f["name"] for f in facets["schema"]["fields"]] == ["id", "v"]
    # The merge's input does not map to the target's columns by position, so no column lineage is emitted
    # rather than a wrong one
    assert "columnLineage" not in facets

    if case.startswith("merge"):
        assert "memory.main.ms" in [i["name"] for i in start.get("inputs", [])]


@pytest.mark.integration
def test_merge_into_excluded_table_has_no_output(file_lineage):
    """duck_lineage_exclude_dataset_prefixes also applies to merge targets."""
    file_lineage.conn.execute("SET duck_lineage_exclude_dataset_prefixes = 'memory.main.mt'")
    try:
        for sql in _SETUP:
            file_lineage.conn.execute(sql)
        statement = _STATEMENTS["merge"]
        file_lineage.conn.execute(statement)

        start = file_lineage.event(statement, "START")
        assert "memory.main.mt" not in [o["name"] for o in start.get("outputs", [])]
    finally:
        file_lineage.conn.execute("RESET duck_lineage_exclude_dataset_prefixes")
