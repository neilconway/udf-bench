"""UDF benchmark definitions with per-system SQL expressions.

Queries are written so that every system must actually evaluate the function
on every row:

- Scalar functions are reduced with ``SUM(<cheap function of the result>)``,
  not ``COUNT(f(x))``. Engines rewrite ``COUNT(f(x))`` to ``COUNT(*)`` when they
  can prove ``f(x)`` is never NULL (ClickHouse for non-Nullable result types
  such as arrays and booleans; DuckDB via Parquet statistics), and then answer
  it from file metadata without calling ``f`` at all.
- Window functions are reduced with ``SUM`` over the window output, not
  ``COUNT(*)``; otherwise the planner drops the unused window expression.
- Ungrouped MIN/MAX/COUNT(*) disable the metadata shortcuts that would answer
  them from Parquet footer statistics.

Each scalar benchmark also has a *baseline* query that applies the same
reduction to the raw input columns, so the cost of the function itself
(``net = query - baseline``) can be separated from scan/decode cost.
"""

import re
from dataclasses import dataclass, field

SYSTEMS = ("datafusion", "duckdb", "clickhouse")


@dataclass
class UDFBenchmark:
    name: str
    category: str
    description: str
    # Full SQL query templates. Use {table} as placeholder for table reference.
    # None means the function is not supported on that system.
    query_datafusion: str | None
    query_duckdb: str | None
    query_clickhouse: str | None
    # Same reduction over the raw input columns (scalar functions only).
    baselines: dict[str, str] = field(default_factory=dict)
    # False for functions whose results legitimately differ across systems
    # (e.g. approximate aggregates), to skip the cross-system result check.
    check_result: bool = True

    def query_for(self, system: str) -> str | None:
        return getattr(self, f"query_{system}", None)

    def baseline_for(self, system: str) -> str | None:
        return self.baselines.get(system)


# ---------------------------------------------------------------------------
# Scalar function reductions
# ---------------------------------------------------------------------------

# Result kinds of scalar functions; each maps to a cheap, per-row reduction
# that depends on the function's output value. Dates/timestamps are reduced to
# epoch seconds (the cheapest unit all systems can produce from any temporal
# type) and summed as DOUBLE; summing epoch micro/nanoseconds overflows Int64.
STR, NUM, BOOL, ARR, TS = "str", "num", "bool", "arr", "ts"

_REDUCE = {
    "datafusion": {
        STR: "octet_length({})",
        NUM: "CAST({} AS DOUBLE)",
        BOOL: "CAST({} AS INT)",
        ARR: "cardinality({})",
        TS: "CAST(CAST(CAST({} AS TIMESTAMP) AS BIGINT) AS DOUBLE) / 1e9",
    },
    "duckdb": {
        STR: "strlen({})",
        NUM: "CAST({} AS DOUBLE)",
        BOOL: "CAST({} AS INTEGER)",
        ARR: "len({})",
        TS: "epoch({})",
    },
    "clickhouse": {
        STR: "length({})",
        NUM: "toFloat64({})",
        BOOL: "toInt32({})",
        ARR: "length({})",
        TS: "toFloat64(toUnixTimestamp({}, 'UTC'))",
    },
}

# Input columns of the generated data set (see generate_data.py), by kind.
_COLUMN_KINDS = {
    **{c: STR for c in ("str_short", "str_medium", "str_long", "str_nullable",
                        "str_pattern", "str_second", "ustr_short", "ustr_medium",
                        "ustr_long", "ustr_nullable", "ustr_second")},
    **{c: NUM for c in ("id", "int_small", "int_large", "int_nullable", "int_second",
                        "float_pos", "float_angle", "float_signed", "float_nullable",
                        "search_int")},
    **{c: TS for c in ("ts", "ts_second")},
    **{c: ARR for c in ("arr_int", "arr_int_second", "arr_str")},
    "bool_col": BOOL,
}
_COLUMN_RE = re.compile(r"\b(" + "|".join(sorted(_COLUMN_KINDS, key=len, reverse=True)) + r")\b")


# Raw timestamp(us) input columns can be read as integers directly, which is
# much cheaper than the general temporal reduction above (in ClickHouse in
# particular); baselines use this so they don't overstate the scan cost.
_RAW_TS = {
    "datafusion": "CAST(CAST({} AS BIGINT) AS DOUBLE)",
    "duckdb": "CAST(epoch_us({}) AS DOUBLE)",
    "clickhouse": "toFloat64(toUnixTimestamp64Micro({}))",
}


def _reduce(system, kind, expr):
    return _REDUCE[system][kind].format(expr)


def _baseline(system, expr):
    """Reduction over the raw input columns referenced by ``expr``.

    This approximates the cost of reading the inputs plus the reduction, so
    ``query - baseline`` approximates the cost of the function itself.
    """
    cols = list(dict.fromkeys(_COLUMN_RE.findall(expr)))
    if not cols:
        return None
    terms = " + ".join(
        _RAW_TS[system].format(c) if _COLUMN_KINDS[c] == TS
        else _reduce(system, _COLUMN_KINDS[c], c)
        for c in cols
    )
    return f"SELECT SUM({terms}) FROM {{table}}"


def _scalar(kind, expr_df, expr_dk, expr_ch):
    """Helper: build scalar UDF queries and their baselines. None = unsupported.

    ``kind`` is the function's result kind (STR, NUM, BOOL, ARR or TS).
    """
    exprs = dict(zip(SYSTEMS, (expr_df, expr_dk, expr_ch)))
    queries = tuple(
        f"SELECT SUM({_reduce(s, kind, e)}) FROM {{table}}" if e else None
        for s, e in exprs.items()
    )
    baselines = {s: b for s, e in exprs.items() if e and (b := _baseline(s, e))}
    return (*queries, {"baselines": baselines})


def _agg_ungrouped(expr_df, expr_dk, expr_ch):
    """Helper: build ungrouped aggregate queries. None = unsupported."""
    return (
        f"SELECT {expr_df} FROM {{table}}" if expr_df else None,
        f"SELECT {expr_dk} FROM {{table}}" if expr_dk else None,
        f"SELECT {expr_ch} FROM {{table}}" if expr_ch else None,
    )


def _agg_grouped(expr_df, expr_dk, expr_ch):
    """Helper: build grouped aggregate queries. None = unsupported."""
    return (
        f"SELECT id % 1000 AS g, {expr_df} FROM {{table}} GROUP BY g" if expr_df else None,
        f"SELECT id % 1000 AS g, {expr_dk} FROM {{table}} GROUP BY g" if expr_dk else None,
        f"SELECT id % 1000 AS g, {expr_ch} FROM {{table}} GROUP BY g" if expr_ch else None,
    )


def _no_stats(q_df, q_dk, q_ch):
    """Disable answering an ungrouped aggregate from Parquet metadata.

    DataFusion and DuckDB answer MIN/MAX/COUNT(*) from footer statistics, and
    ClickHouse answers COUNT(*) from file metadata, without scanning any data.
    """
    return (
        f"SET datafusion.execution.collect_statistics = false; {q_df}" if q_df else None,
        f"SET disabled_optimizers = 'statistics_propagation'; {q_dk}" if q_dk else None,
        f"{q_ch} SETTINGS optimize_count_from_files = 0, optimize_trivial_count_query = 0"
        if q_ch else None,
    )


def _udf(name, category, description, q_df, q_dk, q_ch, opts=None, **kwargs):
    return UDFBenchmark(
        name=name,
        category=category,
        description=description,
        query_datafusion=q_df,
        query_duckdb=q_dk,
        query_clickhouse=q_ch,
        **(opts or {}),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Helper for window functions
# ---------------------------------------------------------------------------


def _window(expr_df, expr_dk, expr_ch):
    """Helper: build window function queries (summed so the window is evaluated)."""
    return tuple(
        f"SELECT SUM({_reduce(s, NUM, 'x')}) FROM (SELECT {e} AS x FROM {{table}})"
        if e else None
        for s, e in zip(SYSTEMS, (expr_df, expr_dk, expr_ch))
    )


# ---------------------------------------------------------------------------
# String functions (27)
# ---------------------------------------------------------------------------

_STRING = "string"

UDFS: list[UDFBenchmark] = [
    _udf("upper", _STRING, "upper(column)", *_scalar(
        STR, "upper(str_medium)", "upper(str_medium)", "upper(str_medium)")),
    _udf("lower", _STRING, "lower(column)", *_scalar(
        STR, "lower(str_medium)", "lower(str_medium)", "lower(str_medium)")),
    _udf("initcap", _STRING, "initcap(column)", *_scalar(
        STR, "initcap(str_medium)", None, "initCap(str_medium)")),
    _udf("character_length", _STRING, "character_length(column)", *_scalar(
        NUM, "character_length(str_medium)", "length(str_medium)", "lengthUTF8(str_medium)")),
    _udf("octet_length", _STRING, "octet_length(column)", *_scalar(
        NUM, "octet_length(str_medium)", "strlen(str_medium)", "length(str_medium)")),
    _udf("ascii", _STRING, "ascii(column)", *_scalar(
        NUM, "ascii(str_short)", "ascii(str_short)", "ascii(str_short)")),
    _udf("chr", _STRING, "chr(int)", *_scalar(
        STR, "chr(int_small % 95 + 32)",
        "chr(CAST(int_small % 95 + 32 AS INTEGER))",
        "char(int_small % 95 + 32)")),
    _udf("concat", _STRING, "concat(col1, col2)", *_scalar(
        STR, "concat(str_short, str_second)", "concat(str_short, str_second)",
        "concat(str_short, str_second)")),
    _udf("concat_ws", _STRING, "concat_ws(sep, col1, col2)", *_scalar(
        STR, "concat_ws('-', str_short, str_second)", "concat_ws('-', str_short, str_second)",
        "concat_ws('-', str_short, str_second)")),
    _udf("trim", _STRING, "btrim(column)", *_scalar(
        STR, "btrim(str_medium)", "trim(str_medium)", "trimBoth(str_medium)")),
    _udf("ltrim", _STRING, "ltrim(column)", *_scalar(
        STR, "ltrim(str_medium)", "ltrim(str_medium)", "trimLeft(str_medium)")),
    _udf("rtrim", _STRING, "rtrim(column)", *_scalar(
        STR, "rtrim(str_medium)", "rtrim(str_medium)", "trimRight(str_medium)")),
    _udf("lpad", _STRING, "lpad(column, 30, '0')", *_scalar(
        STR, "lpad(str_short, 30, '0')", "lpad(str_short, 30, '0')", "leftPad(str_short, 30, '0')")),
    _udf("rpad", _STRING, "rpad(column, 30, '0')", *_scalar(
        STR, "rpad(str_short, 30, '0')", "rpad(str_short, 30, '0')", "rightPad(str_short, 30, '0')")),
    _udf("left", _STRING, "left(column, 5)", *_scalar(
        STR, "left(str_medium, 5)", "left(str_medium, 5)", "left(str_medium, 5)")),
    _udf("right", _STRING, "right(column, 5)", *_scalar(
        STR, "right(str_medium, 5)", "right(str_medium, 5)", "right(str_medium, 5)")),
    _udf("repeat", _STRING, "repeat(column, 3)", *_scalar(
        STR, "repeat(str_short, 3)", "repeat(str_short, 3)", "repeat(str_short, 3)")),
    _udf("reverse", _STRING, "reverse(column)", *_scalar(
        STR, "reverse(str_medium)", "reverse(str_medium)", "reverse(str_medium)")),
    _udf("replace", _STRING, "replace(column, 'a', 'z')", *_scalar(
        STR, "replace(str_medium, 'a', 'z')", "replace(str_medium, 'a', 'z')",
        "replaceAll(str_medium, 'a', 'z')")),
    _udf("translate", _STRING, "translate(column, 'abc', 'xyz')", *_scalar(
        STR, "translate(str_medium, 'abc', 'xyz')", "translate(str_medium, 'abc', 'xyz')",
        "translate(str_medium, 'abc', 'xyz')")),
    _udf("starts_with", _STRING, "starts_with(column, 'alpha')", *_scalar(
        BOOL, "starts_with(str_pattern, 'alpha')", "starts_with(str_pattern, 'alpha')",
        "startsWith(str_pattern, 'alpha')")),
    _udf("ends_with", _STRING, "ends_with(column, 'one')", *_scalar(
        BOOL, "ends_with(str_pattern, 'one')", "ends_with(str_pattern, 'one')",
        "endsWith(str_pattern, 'one')")),
    _udf("position", _STRING, "strpos(column, 'alpha')", *_scalar(
        NUM, "strpos(str_pattern, 'alpha')", "strpos(str_pattern, 'alpha')",
        "position(str_pattern, 'alpha')")),
    _udf("substr", _STRING, "substr(column, 1, 5)", *_scalar(
        STR, "substr(str_medium, 1, 5)", "substr(str_medium, 1, 5)",
        "substring(str_medium, 1, 5)")),
    _udf("split_part", _STRING, "split_part(column, '-', 2)", *_scalar(
        STR, "split_part(str_pattern, '-', 2)", "split_part(str_pattern, '-', 2)",
        "splitByChar('-', assumeNotNull(str_pattern))[2]")),
    _udf("levenshtein", _STRING, "levenshtein(col1, col2)", *_scalar(
        NUM, "levenshtein(str_short, str_second)", "levenshtein(str_short, str_second)",
        "editDistance(str_short, str_second)")),
    _udf("overlay", _STRING, "overlay(col placing 'XX' from 3 for 2)", *_scalar(
        STR, "overlay(str_medium placing 'XX' from 3 for 2)",
        None,
        None)),
    _udf("concat_op", _STRING, "col1 || col2 (operator)", *_scalar(
        STR, "str_short || str_second", "str_short || str_second",
        "str_short || str_second")),
    _udf("like_prefix", _STRING, "col LIKE 'alpha%'", *_scalar(
        BOOL, "str_pattern LIKE 'alpha%'", "str_pattern LIKE 'alpha%'",
        "str_pattern LIKE 'alpha%'")),
    _udf("like_contains", _STRING, "col LIKE '%alpha%'", *_scalar(
        BOOL, "str_medium LIKE '%abc%'", "str_medium LIKE '%abc%'",
        "str_medium LIKE '%abc%'")),
    _udf("ilike_prefix", _STRING, "col ILIKE 'ALPHA%'", *_scalar(
        BOOL, "str_pattern ILIKE 'ALPHA%'", "str_pattern ILIKE 'ALPHA%'",
        "str_pattern ILIKE 'ALPHA%'")),
    _udf("ilike_contains", _STRING, "col ILIKE '%ALPHA%'", *_scalar(
        BOOL, "str_medium ILIKE '%ABC%'", "str_medium ILIKE '%ABC%'",
        "str_medium ILIKE '%ABC%'")),
]

# ---------------------------------------------------------------------------
# Hash functions (3)
# ---------------------------------------------------------------------------

_HASH = "hash"

UDFS += [
    _udf("md5", _HASH, "md5(column)", *_scalar(
        STR, "md5(str_short)", "md5(str_short)", "hex(MD5(str_short))")),
    _udf("sha256", _HASH, "sha256(column)", *_scalar(
        STR, "encode(sha256(str_short), 'hex')", "sha256(str_short)",
        "hex(SHA256(str_short))")),
    # ClickHouse's hex() zero-pads to whole bytes, so result lengths differ.
    _udf("to_hex", _HASH, "to_hex(int)", *_scalar(
        STR, "to_hex(int_large)", "to_hex(int_large)", "hex(int_large)"),
        check_result=False),
]

# ---------------------------------------------------------------------------
# Regex functions (3)
# ---------------------------------------------------------------------------

_REGEX = "regex"

UDFS += [
    _udf("regexp_replace", _REGEX, "regexp_replace(column, pattern, repl)", *_scalar(
        STR, r"regexp_replace(str_pattern, '\d+', 'XXXX')",
        r"regexp_replace(str_pattern, '\d+', 'XXXX')",
        r"replaceRegexpOne(str_pattern, '\d+', 'XXXX')")),
    _udf("regexp_like", _REGEX, "regexp_like(column, pattern)", *_scalar(
        BOOL, "regexp_like(str_pattern, '^alpha')",
        "regexp_matches(str_pattern, '^alpha')",
        "match(str_pattern, '^alpha')")),
    _udf("regexp_count", _REGEX, "regexp_count(column, pattern)", *_scalar(
        NUM, r"regexp_count(str_pattern, '\d+')",
        None,
        None)),
]

# ---------------------------------------------------------------------------
# Math functions (18)
# ---------------------------------------------------------------------------

_MATH = "math"

UDFS += [
    _udf("abs", _MATH, "abs(column)", *_scalar(
        NUM, "abs(float_signed)", "abs(float_signed)", "abs(float_signed)")),
    _udf("ceil", _MATH, "ceil(column)", *_scalar(
        NUM, "ceil(float_signed)", "ceil(float_signed)", "ceil(float_signed)")),
    _udf("floor", _MATH, "floor(column)", *_scalar(
        NUM, "floor(float_signed)", "floor(float_signed)", "floor(float_signed)")),
    _udf("round", _MATH, "round(column, 2)", *_scalar(
        NUM, "round(float_signed, 2)", "round(float_signed, 2)", "round(float_signed, 2)")),
    _udf("trunc", _MATH, "trunc(column)", *_scalar(
        NUM, "trunc(float_signed)", "trunc(float_signed)", "trunc(float_signed)")),
    _udf("power", _MATH, "power(column, 2)", *_scalar(
        NUM, "power(float_pos, 2)", "power(float_pos, 2)", "power(float_pos, 2)")),
    _udf("sqrt", _MATH, "sqrt(column)", *_scalar(
        NUM, "sqrt(float_pos)", "sqrt(float_pos)", "sqrt(float_pos)")),
    _udf("cbrt", _MATH, "cbrt(column)", *_scalar(
        NUM, "cbrt(float_pos)", "cbrt(float_pos)", "cbrt(float_pos)")),
    _udf("exp", _MATH, "exp(column)", *_scalar(
        NUM, "exp(float_angle)", "exp(float_angle)", "exp(float_angle)")),
    _udf("ln", _MATH, "ln(column)", *_scalar(
        NUM, "ln(float_pos)", "ln(float_pos)", "log(float_pos)")),
    _udf("log2", _MATH, "log2(column)", *_scalar(
        NUM, "log2(float_pos)", "log2(float_pos)", "log2(float_pos)")),
    _udf("log10", _MATH, "log10(column)", *_scalar(
        NUM, "log10(float_pos)", "log10(float_pos)", "log10(float_pos)")),
    _udf("sign", _MATH, "sign(column)", *_scalar(
        NUM, "signum(float_signed)", "sign(float_signed)", "sign(float_signed)")),
    _udf("factorial", _MATH, "factorial(column % 20)", *_scalar(
        NUM, "factorial(int_small % 20)",
        "factorial(CAST(int_small % 20 AS INTEGER))",
        "factorial(int_small % 20)")),
    _udf("gcd", _MATH, "gcd(col1, col2)", *_scalar(
        NUM, "gcd(int_small, int_second)", "gcd(int_small, int_second)",
        "gcd(int_small, int_second)")),
    _udf("lcm", _MATH, "lcm(col1, col2)", *_scalar(
        NUM, "lcm(int_small, int_second)", "lcm(int_small, int_second)",
        "lcm(int_small, int_second)")),
    _udf("degrees", _MATH, "degrees(column)", *_scalar(
        NUM, "degrees(float_angle)", "degrees(float_angle)", "degrees(float_angle)")),
    _udf("radians", _MATH, "radians(column)", *_scalar(
        NUM, "radians(float_angle)", "radians(float_angle)", "radians(float_angle)")),
]

# ---------------------------------------------------------------------------
# Trig functions (8)
# ---------------------------------------------------------------------------

_TRIG = "trig"

UDFS += [
    _udf("sin", _TRIG, "sin(column)", *_scalar(
        NUM, "sin(float_angle)", "sin(float_angle)", "sin(float_angle)")),
    _udf("cos", _TRIG, "cos(column)", *_scalar(
        NUM, "cos(float_angle)", "cos(float_angle)", "cos(float_angle)")),
    _udf("tan", _TRIG, "tan(column)", *_scalar(
        NUM, "tan(float_angle)", "tan(float_angle)", "tan(float_angle)")),
    _udf("asin", _TRIG, "asin(column / pi())", *_scalar(
        NUM, "asin(float_angle / pi())", "asin(float_angle / pi())",
        "asin(float_angle / pi())")),
    _udf("acos", _TRIG, "acos(column / pi())", *_scalar(
        NUM, "acos(float_angle / pi())", "acos(float_angle / pi())",
        "acos(float_angle / pi())")),
    _udf("atan", _TRIG, "atan(column)", *_scalar(
        NUM, "atan(float_signed)", "atan(float_signed)", "atan(float_signed)")),
    _udf("atan2", _TRIG, "atan2(col1, col2)", *_scalar(
        NUM, "atan2(float_signed, float_pos)", "atan2(float_signed, float_pos)",
        "atan2(float_signed, float_pos)")),
    _udf("cot", _TRIG, "cot(column)", *_scalar(
        NUM, "cot(float_angle)", "cot(float_angle)", "1/tan(float_angle)")),
]

# ---------------------------------------------------------------------------
# DateTime functions (8)
# ---------------------------------------------------------------------------

_DT = "datetime"

UDFS += [
    _udf("date_trunc", _DT, "date_trunc('month', column)", *_scalar(
        TS, "date_trunc('month', ts)", "date_trunc('month', ts)",
        "date_trunc('month', ts)")),
    _udf("date_part_year", _DT, "date_part('year', column)", *_scalar(
        NUM, "date_part('year', ts)", "date_part('year', ts)",
        "toYear(ts)")),
    _udf("date_part_month", _DT, "date_part('month', column)", *_scalar(
        NUM, "date_part('month', ts)", "date_part('month', ts)",
        "toMonth(ts)")),
    _udf("date_part_dow", _DT, "date_part('dow', column)", *_scalar(
        NUM, "date_part('dow', ts)", "date_part('dow', ts)",
        "toDayOfWeek(ts, 2)")),
    _udf("to_unixtime", _DT, "to_unixtime(column)", *_scalar(
        NUM, "to_unixtime(ts)", "epoch(ts)", "toUnixTimestamp(ts)")),
    _udf("make_date", _DT, "make_date(2024, month, day)", *_scalar(
        TS, "make_date(2024, int_small % 12 + 1, int_small % 28 + 1)",
        "make_date(2024, int_small % 12 + 1, int_small % 28 + 1)",
        "makeDate(2024, int_small % 12 + 1, int_small % 28 + 1)")),
    _udf("to_char", _DT, "to_char(ts, format)", *_scalar(
        STR, "to_char(ts, '%Y-%m-%d %H:%M:%S')",
        "strftime(ts, '%Y-%m-%d %H:%M:%S')",
        "formatDateTime(ts, '%Y-%m-%d %H:%i:%S')")),  # %M is the month name in ClickHouse
    _udf("date_bin", _DT, "date_bin(interval, ts, origin)", *_scalar(
        TS, "date_bin(interval '1 hour', ts, timestamp '2020-01-01T00:00:00')",
        "time_bucket(interval '1 hour', ts)",
        "toStartOfInterval(ts, interval 1 hour)")),
]

# ---------------------------------------------------------------------------
# Conditional functions (4)
# ---------------------------------------------------------------------------

_COND = "conditional"

UDFS += [
    _udf("coalesce", _COND, "coalesce(nullable, fallback)", *_scalar(
        STR, "coalesce(str_nullable, str_second)", "coalesce(str_nullable, str_second)",
        "coalesce(str_nullable, str_second)")),
    _udf("nullif", _COND, "nullif(col1, col2)", *_scalar(
        NUM, "nullif(int_small, int_second)", "nullif(int_small, int_second)",
        "nullIf(int_small, int_second)")),
    _udf("greatest", _COND, "greatest(col1, col2)", *_scalar(
        NUM, "greatest(int_small, int_second)", "greatest(int_small, int_second)",
        "greatest(int_small, int_second)")),
    _udf("least", _COND, "least(col1, col2)", *_scalar(
        NUM, "least(int_small, int_second)", "least(int_small, int_second)",
        "least(int_small, int_second)")),
]

# ---------------------------------------------------------------------------
# Array functions (16)
# ---------------------------------------------------------------------------

_ARRAY = "array"

UDFS += [
    _udf("array_has", _ARRAY, "array_has(arr, 42)", *_scalar(
        BOOL, "array_has(arr_int, 42)", "list_contains(arr_int, 42)", "has(arr_int, 42)")),
    _udf("array_has_col", _ARRAY, "array_has(arr, search_col)", *_scalar(
        BOOL, "array_has(arr_int, search_int)", "list_contains(arr_int, search_int)",
        "has(arr_int, search_int)")),
    _udf("array_has_any", _ARRAY, "array_has_any(arr1, arr2)", *_scalar(
        BOOL, "array_has_any(arr_int, arr_int_second)",
        "list_has_any(arr_int, arr_int_second)",
        "hasAny(arr_int, arr_int_second)")),
    _udf("array_has_all", _ARRAY, "array_has_all(arr1, arr2)", *_scalar(
        BOOL, "array_has_all(arr_int, arr_int_second)",
        "list_has_all(arr_int, arr_int_second)",
        "hasAll(arr_int, arr_int_second)")),
    _udf("array_length", _ARRAY, "array_length(arr)", *_scalar(
        NUM, "array_length(arr_int)", "len(arr_int)", "length(arr_int)")),
    _udf("array_position", _ARRAY, "array_position(arr, val)", *_scalar(
        NUM, "array_position(arr_int, search_int)",
        "list_position(arr_int, search_int)",
        "indexOf(arr_int, search_int)")),
    _udf("array_element", _ARRAY, "array_element(arr, 3)", *_scalar(
        NUM, "array_element(arr_int, 3)", "list_extract(arr_int, 3)",
        "arrayElement(arr_int, 3)")),
    _udf("array_append", _ARRAY, "array_append(arr, val)", *_scalar(
        ARR, "array_append(arr_int, search_int)", "list_append(arr_int, search_int)",
        "arrayPushBack(arr_int, search_int)")),
    _udf("array_prepend", _ARRAY, "array_prepend(val, arr)", *_scalar(
        ARR, "array_prepend(search_int, arr_int)", "list_prepend(search_int, arr_int)",
        "arrayPushFront(arr_int, search_int)")),
    _udf("array_concat", _ARRAY, "array_concat(arr1, arr2)", *_scalar(
        ARR, "array_concat(arr_int, arr_int_second)",
        "list_concat(arr_int, arr_int_second)",
        "arrayConcat(arr_int, arr_int_second)")),
    _udf("array_sort", _ARRAY, "array_sort(arr)", *_scalar(
        ARR, "array_sort(arr_int)", "list_sort(arr_int)", "arraySort(arr_int)")),
    _udf("array_reverse", _ARRAY, "array_reverse(arr)", *_scalar(
        ARR, "array_reverse(arr_int)", "list_reverse(arr_int)", "arrayReverse(arr_int)")),
    _udf("array_distinct", _ARRAY, "array_distinct(arr)", *_scalar(
        ARR, "array_distinct(arr_int)", "list_distinct(arr_int)", "arrayDistinct(arr_int)")),
    _udf("array_intersect", _ARRAY, "array_intersect(arr1, arr2)", *_scalar(
        ARR, "array_intersect(arr_int, arr_int_second)",
        "list_intersect(arr_int, arr_int_second)",
        "arrayIntersect(arr_int, arr_int_second)")),
    _udf("array_to_string", _ARRAY, "array_to_string(arr, ',')", *_scalar(
        STR, "array_to_string(arr_int, ',')", "array_to_string(arr_int, ',')",
        "arrayStringConcat(arr_int, ',')")),
    _udf("array_slice", _ARRAY, "array_slice(arr, 2, 5)", *_scalar(
        ARR, "array_slice(arr_int, 2, 5)", "list_slice(arr_int, 2, 5)",
        "arraySlice(arr_int, 2, 4)")),
    _udf("array_min", _ARRAY, "array_min(arr)", *_scalar(
        NUM, "array_min(arr_int)", "list_min(arr_int)", "arrayMin(arr_int)")),
    _udf("array_positions", _ARRAY, "array_positions(arr, val)", *_scalar(
        ARR, "array_positions(arr_int, search_int)", None, None)),
    _udf("array_union", _ARRAY, "array_union(arr1, arr2)", *_scalar(
        ARR, "array_union(arr_int, arr_int_second)",
        "list_distinct(list_concat(arr_int, arr_int_second))",
        None)),
    _udf("array_except", _ARRAY, "array_except(arr1, arr2)", *_scalar(
        ARR, "array_except(arr_int, arr_int_second)", None, None)),
]

# ---------------------------------------------------------------------------
# Aggregate functions — ungrouped (14)
# ---------------------------------------------------------------------------

_AGG = "agg_ungrouped"

UDFS += [
    _udf("sum", _AGG, "sum(column)", *_agg_ungrouped(
        "sum(float_signed)", "sum(float_signed)", "sum(float_signed)")),
    _udf("avg", _AGG, "avg(column)", *_agg_ungrouped(
        "avg(float_signed)", "avg(float_signed)", "avg(float_signed)")),
    _udf("min_agg", _AGG, "min(column)", *_no_stats(*_agg_ungrouped(
        "min(float_signed)", "min(float_signed)", "min(float_signed)"))),
    _udf("max_agg", _AGG, "max(column)", *_no_stats(*_agg_ungrouped(
        "max(float_signed)", "max(float_signed)", "max(float_signed)"))),
    _udf("count_distinct", _AGG, "count(distinct column)", *_agg_ungrouped(
        "count(distinct int_small)", "count(distinct int_small)",
        "count(distinct int_small)")),
    _udf("approx_distinct", _AGG, "approx_distinct(column)", *_agg_ungrouped(
        "approx_distinct(int_large)", "approx_count_distinct(int_large)",
        "uniq(int_large)"), check_result=False),
    _udf("approx_distinct_str", _AGG, "approx_distinct(string column)", *_agg_ungrouped(
        "approx_distinct(str_medium)", "approx_count_distinct(str_medium)",
        "uniq(str_medium)"), check_result=False),
    _udf("stddev", _AGG, "stddev(column)", *_agg_ungrouped(
        "stddev(float_signed)", "stddev(float_signed)", "stddevSamp(float_signed)")),
    _udf("variance", _AGG, "var(column)", *_agg_ungrouped(
        "var(float_signed)", "var_samp(float_signed)", "varSamp(float_signed)")),
    _udf("bit_and_agg", _AGG, "bit_and(column)", *_agg_ungrouped(
        "bit_and(int_small)", "bit_and(int_small)", "groupBitAnd(int_small)")),
    _udf("bit_xor_agg", _AGG, "bit_xor(column)", *_agg_ungrouped(
        "bit_xor(int_small)", "bit_xor(int_small)", "groupBitXor(int_small)")),
    _udf("bit_or_agg", _AGG, "bit_or(column)", *_agg_ungrouped(
        "bit_or(int_small)", "bit_or(int_small)", "groupBitOr(int_small)")),
    _udf("count_star", _AGG, "count(*)", *_no_stats(*_agg_ungrouped(
        "count(*)", "count(*)", "count(*)"))),
    _udf("stddev_pop", _AGG, "stddev_pop(column)", *_agg_ungrouped(
        "stddev_pop(float_signed)", "stddev_pop(float_signed)",
        "stddevPop(float_signed)")),
    _udf("var_pop", _AGG, "var_pop(column)", *_agg_ungrouped(
        "var_pop(float_signed)", "var_pop(float_signed)", "varPop(float_signed)")),
]

# ---------------------------------------------------------------------------
# Aggregate functions — grouped (25)
# ---------------------------------------------------------------------------

_AGG_G = "agg_grouped"

UDFS += [
    _udf("sum_grouped", _AGG_G, "sum(column) GROUP BY", *_agg_grouped(
        "sum(float_signed)", "sum(float_signed)", "sum(float_signed)")),
    _udf("avg_grouped", _AGG_G, "avg(column) GROUP BY", *_agg_grouped(
        "avg(float_signed)", "avg(float_signed)", "avg(float_signed)")),
    _udf("min_grouped", _AGG_G, "min(column) GROUP BY", *_agg_grouped(
        "min(float_signed)", "min(float_signed)", "min(float_signed)")),
    _udf("max_grouped", _AGG_G, "max(column) GROUP BY", *_agg_grouped(
        "max(float_signed)", "max(float_signed)", "max(float_signed)")),
    _udf("count_distinct_grouped", _AGG_G, "count(distinct) GROUP BY", *_agg_grouped(
        "count(distinct int_small)", "count(distinct int_small)",
        "count(distinct int_small)")),
    _udf("approx_distinct_grouped", _AGG_G, "approx_distinct GROUP BY", *_agg_grouped(
        "approx_distinct(int_large)", "approx_count_distinct(int_large)",
        "uniq(int_large)")),
    _udf("approx_distinct_str_grouped", _AGG_G, "approx_distinct(string) GROUP BY", *_agg_grouped(
        "approx_distinct(str_medium)", "approx_count_distinct(str_medium)",
        "uniq(str_medium)")),
    _udf("stddev_grouped", _AGG_G, "stddev GROUP BY", *_agg_grouped(
        "stddev(float_signed)", "stddev(float_signed)", "stddevSamp(float_signed)")),
    _udf("variance_grouped", _AGG_G, "var GROUP BY", *_agg_grouped(
        "var(float_signed)", "var_samp(float_signed)", "varSamp(float_signed)")),
    _udf("bit_and_grouped", _AGG_G, "bit_and GROUP BY", *_agg_grouped(
        "bit_and(int_small)", "bit_and(int_small)", "groupBitAnd(int_small)")),
    _udf("bit_xor_grouped", _AGG_G, "bit_xor GROUP BY", *_agg_grouped(
        "bit_xor(int_small)", "bit_xor(int_small)", "groupBitXor(int_small)")),
    _udf("string_agg_grouped", _AGG_G, "string_agg GROUP BY", *_agg_grouped(
        "string_agg(str_short, ',')", "string_agg(str_short, ',')",
        "groupConcat(',')(str_short)")),
    _udf("array_agg_grouped", _AGG_G, "array_agg GROUP BY", *_agg_grouped(
        "array_agg(int_small)", "array_agg(int_small)", "groupArray(int_small)")),
    _udf("bool_and_grouped", _AGG_G, "bool_and GROUP BY", *_agg_grouped(
        "bool_and(bool_col)", "bool_and(bool_col)", "min(bool_col)")),
    _udf("corr_grouped", _AGG_G, "corr GROUP BY", *_agg_grouped(
        "corr(float_signed, float_pos)", "corr(float_signed, float_pos)",
        "corr(float_signed, float_pos)")),
    _udf("covar_samp_grouped", _AGG_G, "covar_samp GROUP BY", *_agg_grouped(
        "covar_samp(float_signed, float_pos)", "covar_samp(float_signed, float_pos)",
        "covarSamp(float_signed, float_pos)")),
    _udf("median_grouped", _AGG_G, "median GROUP BY (exact)", *_agg_grouped(
        "median(float_signed)", "median(float_signed)",
        "quantileExact(0.5)(float_signed)")),
    _udf("approx_median_grouped", _AGG_G, "approx_median GROUP BY", *_agg_grouped(
        "approx_median(float_signed)", "approx_quantile(float_signed, 0.5)",
        "quantile(0.5)(float_signed)")),
    _udf("count_star_grouped", _AGG_G, "count(*) GROUP BY", *_agg_grouped(
        "count(*)", "count(*)", "count(*)")),
    _udf("stddev_pop_grouped", _AGG_G, "stddev_pop GROUP BY", *_agg_grouped(
        "stddev_pop(float_signed)", "stddev_pop(float_signed)",
        "stddevPop(float_signed)")),
    _udf("var_pop_grouped", _AGG_G, "var_pop GROUP BY", *_agg_grouped(
        "var_pop(float_signed)", "var_pop(float_signed)", "varPop(float_signed)")),
    _udf("bit_or_grouped", _AGG_G, "bit_or GROUP BY", *_agg_grouped(
        "bit_or(int_small)", "bit_or(int_small)", "groupBitOr(int_small)")),
    _udf("bool_or_grouped", _AGG_G, "bool_or GROUP BY", *_agg_grouped(
        "bool_or(bool_col)", "bool_or(bool_col)", "max(bool_col)")),
    _udf("covar_pop_grouped", _AGG_G, "covar_pop GROUP BY", *_agg_grouped(
        "covar_pop(float_signed, float_pos)", "covar_pop(float_signed, float_pos)",
        "covarPop(float_signed, float_pos)")),
    _udf("regr_slope_grouped", _AGG_G, "regr_slope GROUP BY", *_agg_grouped(
        "regr_slope(float_signed, float_pos)", "regr_slope(float_signed, float_pos)",
        None)),
    _udf("approx_percentile_grouped", _AGG_G, "approx_percentile(0.95) GROUP BY", *_agg_grouped(
        "approx_percentile_cont(float_signed, 0.95)",
        "approx_quantile(float_signed, 0.95)",
        "quantile(0.95)(float_signed)")),
]

# ---------------------------------------------------------------------------
# Window functions (6)
# ---------------------------------------------------------------------------

_WIN = "window"

UDFS += [
    _udf("row_number", _WIN, "row_number() OVER (ORDER BY id)", *_window(
        "row_number() OVER (ORDER BY id)",
        "row_number() OVER (ORDER BY id)",
        "row_number() OVER (ORDER BY id)")),
    _udf("rank", _WIN, "rank() OVER (ORDER BY int_small)", *_window(
        "rank() OVER (ORDER BY int_small)",
        "rank() OVER (ORDER BY int_small)",
        "rank() OVER (ORDER BY int_small)")),
    _udf("dense_rank", _WIN, "dense_rank() OVER (ORDER BY int_small)", *_window(
        "dense_rank() OVER (ORDER BY int_small)",
        "dense_rank() OVER (ORDER BY int_small)",
        "dense_rank() OVER (ORDER BY int_small)")),
    _udf("lag", _WIN, "lag(col, 1) OVER (ORDER BY id)", *_window(
        "lag(int_small, 1) OVER (ORDER BY id)",
        "lag(int_small, 1) OVER (ORDER BY id)",
        "lag(int_small, 1) OVER (ORDER BY id)")),
    _udf("lead", _WIN, "lead(col, 1) OVER (ORDER BY id)", *_window(
        "lead(int_small, 1) OVER (ORDER BY id)",
        "lead(int_small, 1) OVER (ORDER BY id)",
        "lead(int_small, 1) OVER (ORDER BY id)")),
    _udf("running_sum", _WIN, "sum(col) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)", *_window(
        "sum(int_small) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "sum(int_small) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "sum(int_small) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")),
]
