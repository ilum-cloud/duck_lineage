"""
Performance benchmarks for duck_lineage extension overhead.

Measures the wall-clock time overhead added by the lineage extension
to DuckDB query execution. No Marquez dependency — tests run in-memory.

Run:
    cd test && uv run pytest tests/test_performance.py -v -s

Marked as @pytest.mark.slow so they don't run in default CI.
"""

import time
import duckdb
import pytest

# Number of timed iterations per query (after warm-up)
ITERATIONS = 20
WARMUP = 3

# Overhead thresholds (ms)
SIMPLE_THRESHOLD_MS = 20
DEFAULT_THRESHOLD_MS = 100


@pytest.fixture
def plain_conn():
    """Plain DuckDB connection WITHOUT the lineage extension."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def lineage_conn(extension_path):
    """DuckDB connection WITH the lineage extension loaded (no Marquez)."""
    conn = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{extension_path}'")
    # RFC 5737 TEST-NET — guaranteed non-routable, avoids HTTP side effects
    conn.execute("SET duck_lineage_url = 'http://192.0.2.1:1/api/v1/lineage'")
    conn.execute("SET duck_lineage_namespace = 'perf_test'")
    yield conn
    conn.close()


def _setup_wide_table(conn, name="wide_table", ncols=50, nrows=1000):
    """Create a table with many columns and rows."""
    cols = ", ".join(f"col{i} INTEGER" for i in range(ncols))
    conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({cols})")
    vals = ", ".join(str(i % 100) for i in range(ncols))
    for _ in range(nrows // 100):
        batch = ", ".join(f"({vals})" for _ in range(100))
        conn.execute(f"INSERT INTO {name} VALUES {batch}")


def _setup_join_tables(conn, ntables=5, ncols=10, nrows=500):
    """Create multiple tables for JOIN benchmarks."""
    for t in range(ntables):
        cols = ["id INTEGER"] + [f"t{t}_col{i} INTEGER" for i in range(ncols)]
        conn.execute(f"CREATE TABLE IF NOT EXISTS join_t{t} ({', '.join(cols)})")
        vals_list = []
        for row in range(nrows):
            vals = [str(row)] + [str((row + i) % 100) for i in range(ncols)]
            vals_list.append(f"({', '.join(vals)})")
        conn.execute(f"INSERT INTO join_t{t} VALUES {', '.join(vals_list)}")


def _measure(conn, query, iterations=ITERATIONS, warmup=WARMUP):
    """Run query multiple times, return median execution time in ms."""
    for _ in range(warmup):
        conn.execute(query).fetchall()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        conn.execute(query).fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)

    times.sort()
    return times[len(times) // 2]


def _report(name, plain_ms, lineage_ms):
    """Print benchmark results and return overhead."""
    overhead = lineage_ms - plain_ms
    ratio = lineage_ms / plain_ms if plain_ms > 0 else float("inf")
    print(
        f"\n  {name}:"
        f"\n    plain:    {plain_ms:7.2f} ms"
        f"\n    lineage:  {lineage_ms:7.2f} ms"
        f"\n    overhead: {overhead:+7.2f} ms  ({ratio:.2f}x)"
    )
    return overhead


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_baseline_select_1(plain_conn, lineage_conn):
    """Minimum overhead: SELECT 1."""
    q = "SELECT 1"
    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("SELECT 1", plain_ms, lineage_ms)
    ratio = lineage_ms / plain_ms if plain_ms > 0 else float("inf")
    assert overhead < SIMPLE_THRESHOLD_MS, f"Baseline overhead {overhead:.1f}ms exceeds {SIMPLE_THRESHOLD_MS}ms"
    assert ratio < 1.8, f"Baseline ratio {ratio:.2f}x exceeds 1.8x"


@pytest.mark.slow
def test_simple_select_star(plain_conn, lineage_conn):
    """Star expansion on a 50-column table."""
    _setup_wide_table(plain_conn)
    _setup_wide_table(lineage_conn)

    q = "SELECT * FROM wide_table"
    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("SELECT * (50 cols)", plain_ms, lineage_ms)
    assert overhead < SIMPLE_THRESHOLD_MS, f"Simple SELECT overhead {overhead:.1f}ms exceeds {SIMPLE_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_expression_heavy(plain_conn, lineage_conn):
    """20 computed expressions stressing ResolveExpression."""
    _setup_wide_table(plain_conn, ncols=25)
    _setup_wide_table(lineage_conn, ncols=25)

    exprs = []
    for i in range(0, 20):
        exprs.append(f"col{i} + col{i+1} AS sum_{i}")
    exprs.extend(
        [
            "UPPER(CAST(col0 AS VARCHAR)) AS upper_col0",
            "col1 * col2 + col3 AS combo",
            "CASE WHEN col4 > 50 THEN col5 ELSE col6 END AS case_expr",
        ]
    )
    q = f"SELECT {', '.join(exprs)} FROM wide_table"

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("Expression-heavy (23 exprs)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"Expression overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_multi_join(plain_conn, lineage_conn):
    """5-table JOIN selecting 20 columns."""
    _setup_join_tables(plain_conn)
    _setup_join_tables(lineage_conn)

    select_cols = ", ".join(f"join_t{t}.t{t}_col{c}" for t in range(5) for c in range(4))
    joins = " ".join(f"JOIN join_t{t} ON join_t0.id = join_t{t}.id" for t in range(1, 5))
    q = f"SELECT {select_cols} FROM join_t0 {joins}"

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("5-table JOIN (20 cols)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"Multi-JOIN overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_aggregation(plain_conn, lineage_conn):
    """GROUP BY with 10 aggregate functions."""
    _setup_wide_table(plain_conn, ncols=20)
    _setup_wide_table(lineage_conn, ncols=20)

    aggs = [
        "SUM(col5) AS s1",
        "AVG(col6) AS a1",
        "MIN(col7) AS mn1",
        "MAX(col8) AS mx1",
        "COUNT(col9) AS c1",
        "SUM(col10) AS s2",
        "AVG(col11) AS a2",
        "MIN(col12) AS mn2",
        "MAX(col13) AS mx2",
        "COUNT(col14) AS c2",
    ]
    q = f"SELECT col0, col1, col2, col3, col4, {', '.join(aggs)} FROM wide_table GROUP BY col0, col1, col2, col3, col4"

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("Aggregation (5 GB, 10 aggs)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"Aggregation overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_window_functions(plain_conn, lineage_conn):
    """5 window expressions with PARTITION BY + ORDER BY."""
    _setup_wide_table(plain_conn, ncols=15)
    _setup_wide_table(lineage_conn, ncols=15)

    windows = [
        "ROW_NUMBER() OVER (PARTITION BY col0 ORDER BY col1) AS rn",
        "RANK() OVER (PARTITION BY col0, col2 ORDER BY col3 DESC) AS rnk",
        "SUM(col4) OVER (PARTITION BY col0 ORDER BY col1 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum",
        "AVG(col5) OVER (PARTITION BY col0 ORDER BY col1) AS running_avg",
        "LAG(col6, 1) OVER (PARTITION BY col0 ORDER BY col1) AS prev_val",
    ]
    q = f"SELECT col0, col1, {', '.join(windows)} FROM wide_table"

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("Window functions (5 windows)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"Window overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_nested_cte(plain_conn, lineage_conn):
    """3-level CTE chain."""
    _setup_wide_table(plain_conn, ncols=10)
    _setup_wide_table(lineage_conn, ncols=10)

    q = """
    WITH cte1 AS (
        SELECT col0, col1, col2, col0 + col1 AS derived1
        FROM wide_table
        WHERE col0 > 10
    ),
    cte2 AS (
        SELECT col0, derived1, col2 * 2 AS derived2
        FROM cte1
        WHERE derived1 > 20
    ),
    cte3 AS (
        SELECT col0, derived1 + derived2 AS final_val
        FROM cte2
    )
    SELECT * FROM cte3
    """

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("Nested CTE (3 levels)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"CTE overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_union_chain(plain_conn, lineage_conn):
    """UNION ALL of 4 branches, 10 cols each."""
    _setup_wide_table(plain_conn, ncols=10)
    _setup_wide_table(lineage_conn, ncols=10)

    cols = ", ".join(f"col{i}" for i in range(10))
    branches = [f"SELECT {cols} FROM wide_table WHERE col0 > {v}" for v in [10, 20, 30, 40]]
    q = " UNION ALL ".join(branches)

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("UNION ALL (4 branches)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"UNION overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"


@pytest.mark.slow
def test_complex_combined(plain_conn, lineage_conn):
    """Realistic analytics query: CTE + JOIN + aggregation + window."""
    _setup_wide_table(plain_conn, name="orders", ncols=10)
    _setup_wide_table(lineage_conn, name="orders", ncols=10)
    _setup_wide_table(plain_conn, name="customers", ncols=10)
    _setup_wide_table(lineage_conn, name="customers", ncols=10)

    q = """
    WITH base AS (
        SELECT o.col0 AS order_id,
               o.col1 AS customer_id,
               o.col2 AS amount,
               c.col3 AS region
        FROM orders o
        JOIN customers c ON o.col1 = c.col0
    ),
    agg AS (
        SELECT region,
               customer_id,
               SUM(amount) AS total_amount,
               COUNT(*) AS order_count,
               AVG(amount) AS avg_amount
        FROM base
        GROUP BY region, customer_id
    )
    SELECT region,
           customer_id,
           total_amount,
           order_count,
           avg_amount,
           RANK() OVER (PARTITION BY region ORDER BY total_amount DESC) AS rank_in_region,
           total_amount * 1.0 / SUM(total_amount) OVER (PARTITION BY region) AS pct_of_region
    FROM agg
    """

    plain_ms = _measure(plain_conn, q)
    lineage_ms = _measure(lineage_conn, q)
    overhead = _report("Complex combined (CTE+JOIN+AGG+WINDOW)", plain_ms, lineage_ms)
    assert overhead < DEFAULT_THRESHOLD_MS, f"Complex query overhead {overhead:.1f}ms exceeds {DEFAULT_THRESHOLD_MS}ms"
