"""UDF benchmark definitions with per-system SQL expressions."""

from dataclasses import dataclass


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

    def query_for(self, system: str) -> str | None:
        return getattr(self, f"query_{system}")


def _scalar(expr_df, expr_dk, expr_ch):
    """Helper: build scalar UDF queries (wrapped in COUNT). None = unsupported."""
    return (
        f"SELECT COUNT({expr_df}) FROM {{table}}" if expr_df else None,
        f"SELECT COUNT({expr_dk}) FROM {{table}}" if expr_dk else None,
        f"SELECT COUNT({expr_ch}) FROM {{table}}" if expr_ch else None,
    )


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


def _udf(name, category, description, q_df, q_dk, q_ch):
    return UDFBenchmark(
        name=name,
        category=category,
        description=description,
        query_datafusion=q_df,
        query_duckdb=q_dk,
        query_clickhouse=q_ch,
    )


# ---------------------------------------------------------------------------
# String functions (25)
# ---------------------------------------------------------------------------

_STRING = "string"

UDFS: list[UDFBenchmark] = [
    _udf("upper", _STRING, "upper(column)", *_scalar(
        "upper(str_medium)", "upper(str_medium)", "upper(str_medium)")),
    _udf("lower", _STRING, "lower(column)", *_scalar(
        "lower(str_medium)", "lower(str_medium)", "lower(str_medium)")),
    _udf("initcap", _STRING, "initcap(column)", *_scalar(
        "initcap(str_medium)", None, "initCap(str_medium)")),
    _udf("character_length", _STRING, "character_length(column)", *_scalar(
        "character_length(str_medium)", "length(str_medium)", "lengthUTF8(str_medium)")),
    _udf("octet_length", _STRING, "octet_length(column)", *_scalar(
        "octet_length(str_medium)", "strlen(str_medium)", "length(str_medium)")),
    _udf("ascii", _STRING, "ascii(column)", *_scalar(
        "ascii(str_short)", "ascii(str_short)", "ascii(str_short)")),
    _udf("chr", _STRING, "chr(int)", *_scalar(
        "chr(int_small % 95 + 32)",
        "chr(CAST(int_small % 95 + 32 AS INTEGER))",
        "char(int_small % 95 + 32)")),
    _udf("concat", _STRING, "concat(col1, col2)", *_scalar(
        "concat(str_short, str_second)", "concat(str_short, str_second)",
        "concat(str_short, str_second)")),
    _udf("concat_ws", _STRING, "concat_ws(sep, col1, col2)", *_scalar(
        "concat_ws('-', str_short, str_second)", "concat_ws('-', str_short, str_second)",
        "concat_ws('-', str_short, str_second)")),
    _udf("trim", _STRING, "btrim(column)", *_scalar(
        "btrim(str_medium)", "trim(str_medium)", "trimBoth(str_medium)")),
    _udf("ltrim", _STRING, "ltrim(column)", *_scalar(
        "ltrim(str_medium)", "ltrim(str_medium)", "trimLeft(str_medium)")),
    _udf("rtrim", _STRING, "rtrim(column)", *_scalar(
        "rtrim(str_medium)", "rtrim(str_medium)", "trimRight(str_medium)")),
    _udf("lpad", _STRING, "lpad(column, 30, '0')", *_scalar(
        "lpad(str_short, 30, '0')", "lpad(str_short, 30, '0')", "leftPad(str_short, 30, '0')")),
    _udf("rpad", _STRING, "rpad(column, 30, '0')", *_scalar(
        "rpad(str_short, 30, '0')", "rpad(str_short, 30, '0')", "rightPad(str_short, 30, '0')")),
    _udf("left", _STRING, "left(column, 5)", *_scalar(
        "left(str_medium, 5)", "left(str_medium, 5)", "left(str_medium, 5)")),
    _udf("right", _STRING, "right(column, 5)", *_scalar(
        "right(str_medium, 5)", "right(str_medium, 5)", "right(str_medium, 5)")),
    _udf("repeat", _STRING, "repeat(column, 3)", *_scalar(
        "repeat(str_short, 3)", "repeat(str_short, 3)", "repeat(str_short, 3)")),
    _udf("reverse", _STRING, "reverse(column)", *_scalar(
        "reverse(str_medium)", "reverse(str_medium)", "reverse(str_medium)")),
    _udf("replace", _STRING, "replace(column, 'a', 'z')", *_scalar(
        "replace(str_medium, 'a', 'z')", "replace(str_medium, 'a', 'z')",
        "replaceAll(str_medium, 'a', 'z')")),
    _udf("translate", _STRING, "translate(column, 'abc', 'xyz')", *_scalar(
        "translate(str_medium, 'abc', 'xyz')", "translate(str_medium, 'abc', 'xyz')",
        "translate(str_medium, 'abc', 'xyz')")),
    _udf("starts_with", _STRING, "starts_with(column, 'alpha')", *_scalar(
        "starts_with(str_pattern, 'alpha')", "starts_with(str_pattern, 'alpha')",
        "startsWith(str_pattern, 'alpha')")),
    _udf("ends_with", _STRING, "ends_with(column, 'one')", *_scalar(
        "ends_with(str_pattern, 'one')", "ends_with(str_pattern, 'one')",
        "endsWith(str_pattern, 'one')")),
    _udf("position", _STRING, "strpos(column, 'alpha')", *_scalar(
        "strpos(str_pattern, 'alpha')", "strpos(str_pattern, 'alpha')",
        "position(str_pattern, 'alpha')")),
    _udf("substr", _STRING, "substr(column, 1, 5)", *_scalar(
        "substr(str_medium, 1, 5)", "substr(str_medium, 1, 5)",
        "substring(str_medium, 1, 5)")),
    _udf("split_part", _STRING, "split_part(column, '-', 2)", *_scalar(
        "split_part(str_pattern, '-', 2)", "split_part(str_pattern, '-', 2)",
        "splitByChar('-', str_pattern)[2]")),
    _udf("levenshtein", _STRING, "levenshtein(col1, col2)", *_scalar(
        "levenshtein(str_short, str_second)", "levenshtein(str_short, str_second)",
        "editDistance(str_short, str_second)")),
]

# ---------------------------------------------------------------------------
# Hash functions (3)
# ---------------------------------------------------------------------------

_HASH = "hash"

UDFS += [
    _udf("md5", _HASH, "md5(column)", *_scalar(
        "md5(str_short)", "md5(str_short)", "hex(MD5(str_short))")),
    _udf("sha256", _HASH, "sha256(column)", *_scalar(
        "sha256(str_short)", "sha256(str_short)", "hex(SHA256(str_short))")),
    _udf("to_hex", _HASH, "to_hex(int)", *_scalar(
        "to_hex(int_large)", "to_hex(int_large)", "hex(int_large)")),
]

# ---------------------------------------------------------------------------
# Regex functions (2)
# ---------------------------------------------------------------------------

_REGEX = "regex"

UDFS += [
    _udf("regexp_replace", _REGEX, "regexp_replace(column, pattern, repl)", *_scalar(
        r"regexp_replace(str_pattern, '\d+', 'XXXX')",
        r"regexp_replace(str_pattern, '\d+', 'XXXX')",
        r"replaceRegexpOne(str_pattern, '\d+', 'XXXX')")),
    _udf("regexp_like", _REGEX, "regexp_like(column, pattern)", *_scalar(
        "regexp_like(str_pattern, '^alpha')",
        "regexp_matches(str_pattern, '^alpha')",
        "match(str_pattern, '^alpha')")),
]

# ---------------------------------------------------------------------------
# Math functions (16)
# ---------------------------------------------------------------------------

_MATH = "math"

UDFS += [
    _udf("abs", _MATH, "abs(column)", *_scalar(
        "abs(float_signed)", "abs(float_signed)", "abs(float_signed)")),
    _udf("ceil", _MATH, "ceil(column)", *_scalar(
        "ceil(float_signed)", "ceil(float_signed)", "ceil(float_signed)")),
    _udf("floor", _MATH, "floor(column)", *_scalar(
        "floor(float_signed)", "floor(float_signed)", "floor(float_signed)")),
    _udf("round", _MATH, "round(column, 2)", *_scalar(
        "round(float_signed, 2)", "round(float_signed, 2)", "round(float_signed, 2)")),
    _udf("trunc", _MATH, "trunc(column)", *_scalar(
        "trunc(float_signed)", "trunc(float_signed)", "trunc(float_signed)")),
    _udf("power", _MATH, "power(column, 2)", *_scalar(
        "power(float_pos, 2)", "power(float_pos, 2)", "power(float_pos, 2)")),
    _udf("sqrt", _MATH, "sqrt(column)", *_scalar(
        "sqrt(float_pos)", "sqrt(float_pos)", "sqrt(float_pos)")),
    _udf("cbrt", _MATH, "cbrt(column)", *_scalar(
        "cbrt(float_pos)", "cbrt(float_pos)", "cbrt(float_pos)")),
    _udf("exp", _MATH, "exp(column)", *_scalar(
        "exp(float_angle)", "exp(float_angle)", "exp(float_angle)")),
    _udf("ln", _MATH, "ln(column)", *_scalar(
        "ln(float_pos)", "ln(float_pos)", "log(float_pos)")),
    _udf("log2", _MATH, "log2(column)", *_scalar(
        "log2(float_pos)", "log2(float_pos)", "log2(float_pos)")),
    _udf("log10", _MATH, "log10(column)", *_scalar(
        "log10(float_pos)", "log10(float_pos)", "log10(float_pos)")),
    _udf("sign", _MATH, "sign(column)", *_scalar(
        "signum(float_signed)", "sign(float_signed)", "sign(float_signed)")),
    _udf("factorial", _MATH, "factorial(column % 20)", *_scalar(
        "factorial(int_small % 20)",
        "factorial(CAST(int_small % 20 AS INTEGER))",
        "factorial(int_small % 20)")),
    _udf("gcd", _MATH, "gcd(col1, col2)", *_scalar(
        "gcd(int_small, int_second)", "gcd(int_small, int_second)",
        "gcd(int_small, int_second)")),
    _udf("lcm", _MATH, "lcm(col1, col2)", *_scalar(
        "lcm(int_small, int_second)", "lcm(int_small, int_second)",
        "lcm(int_small, int_second)")),
]

# ---------------------------------------------------------------------------
# Trig functions (7)
# ---------------------------------------------------------------------------

_TRIG = "trig"

UDFS += [
    _udf("sin", _TRIG, "sin(column)", *_scalar(
        "sin(float_angle)", "sin(float_angle)", "sin(float_angle)")),
    _udf("cos", _TRIG, "cos(column)", *_scalar(
        "cos(float_angle)", "cos(float_angle)", "cos(float_angle)")),
    _udf("tan", _TRIG, "tan(column)", *_scalar(
        "tan(float_angle)", "tan(float_angle)", "tan(float_angle)")),
    _udf("asin", _TRIG, "asin(column / pi())", *_scalar(
        "asin(float_angle / pi())", "asin(float_angle / pi())",
        "asin(float_angle / pi())")),
    _udf("acos", _TRIG, "acos(column / pi())", *_scalar(
        "acos(float_angle / pi())", "acos(float_angle / pi())",
        "acos(float_angle / pi())")),
    _udf("atan", _TRIG, "atan(column)", *_scalar(
        "atan(float_signed)", "atan(float_signed)", "atan(float_signed)")),
    _udf("atan2", _TRIG, "atan2(col1, col2)", *_scalar(
        "atan2(float_signed, float_pos)", "atan2(float_signed, float_pos)",
        "atan2(float_signed, float_pos)")),
]

# ---------------------------------------------------------------------------
# DateTime functions (6)
# ---------------------------------------------------------------------------

_DT = "datetime"

UDFS += [
    _udf("date_trunc", _DT, "date_trunc('month', column)", *_scalar(
        "date_trunc('month', ts)", "date_trunc('month', ts)",
        "date_trunc('month', ts)")),
    _udf("date_part_year", _DT, "date_part('year', column)", *_scalar(
        "date_part('year', ts)", "date_part('year', ts)",
        "toYear(ts)")),
    _udf("date_part_month", _DT, "date_part('month', column)", *_scalar(
        "date_part('month', ts)", "date_part('month', ts)",
        "toMonth(ts)")),
    _udf("date_part_dow", _DT, "date_part('dow', column)", *_scalar(
        "date_part('dow', ts)", "date_part('dow', ts)",
        "toDayOfWeek(ts)")),
    _udf("to_unixtime", _DT, "to_unixtime(column)", *_scalar(
        "to_unixtime(ts)", "epoch(ts)", "toUnixTimestamp(ts)")),
    _udf("make_date", _DT, "make_date(2024, month, day)", *_scalar(
        "make_date(2024, int_small % 12 + 1, int_small % 28 + 1)",
        "make_date(2024, int_small % 12 + 1, int_small % 28 + 1)",
        "makeDate(2024, int_small % 12 + 1, int_small % 28 + 1)")),
]

# ---------------------------------------------------------------------------
# Conditional functions (4)
# ---------------------------------------------------------------------------

_COND = "conditional"

UDFS += [
    _udf("coalesce", _COND, "coalesce(nullable, fallback)", *_scalar(
        "coalesce(str_nullable, str_second)", "coalesce(str_nullable, str_second)",
        "coalesce(str_nullable, str_second)")),
    _udf("nullif", _COND, "nullif(col1, col2)", *_scalar(
        "nullif(int_small, int_second)", "nullif(int_small, int_second)",
        "nullIf(int_small, int_second)")),
    _udf("greatest", _COND, "greatest(col1, col2)", *_scalar(
        "greatest(int_small, int_second)", "greatest(int_small, int_second)",
        "greatest(int_small, int_second)")),
    _udf("least", _COND, "least(col1, col2)", *_scalar(
        "least(int_small, int_second)", "least(int_small, int_second)",
        "least(int_small, int_second)")),
]

# ---------------------------------------------------------------------------
# Array functions (14)
# ---------------------------------------------------------------------------

_ARRAY = "array"

UDFS += [
    _udf("array_has", _ARRAY, "array_has(arr, 42)", *_scalar(
        "array_has(arr_int, 42)", "list_contains(arr_int, 42)", "has(arr_int, 42)")),
    _udf("array_has_col", _ARRAY, "array_has(arr, search_col)", *_scalar(
        "array_has(arr_int, search_int)", "list_contains(arr_int, search_int)",
        "has(arr_int, search_int)")),
    _udf("array_has_any", _ARRAY, "array_has_any(arr1, arr2)", *_scalar(
        "array_has_any(arr_int, arr_int_second)",
        "list_has_any(arr_int, arr_int_second)",
        "hasAny(arr_int, arr_int_second)")),
    _udf("array_has_all", _ARRAY, "array_has_all(arr1, arr2)", *_scalar(
        "array_has_all(arr_int, arr_int_second)",
        "list_has_all(arr_int, arr_int_second)",
        "hasAll(arr_int, arr_int_second)")),
    _udf("array_length", _ARRAY, "array_length(arr)", *_scalar(
        "array_length(arr_int)", "len(arr_int)", "length(arr_int)")),
    _udf("array_position", _ARRAY, "array_position(arr, val)", *_scalar(
        "array_position(arr_int, search_int)",
        "list_position(arr_int, search_int)",
        "indexOf(arr_int, search_int)")),
    _udf("array_element", _ARRAY, "array_element(arr, 3)", *_scalar(
        "array_element(arr_int, 3)", "list_extract(arr_int, 3)",
        "arrayElement(arr_int, 3)")),
    _udf("array_append", _ARRAY, "array_append(arr, val)", *_scalar(
        "array_append(arr_int, search_int)", "list_append(arr_int, search_int)",
        "arrayPushBack(arr_int, search_int)")),
    _udf("array_prepend", _ARRAY, "array_prepend(val, arr)", *_scalar(
        "array_prepend(search_int, arr_int)", "list_prepend(search_int, arr_int)",
        "arrayPushFront(arr_int, search_int)")),
    _udf("array_concat", _ARRAY, "array_concat(arr1, arr2)", *_scalar(
        "array_concat(arr_int, arr_int_second)",
        "list_concat(arr_int, arr_int_second)",
        "arrayConcat(arr_int, arr_int_second)")),
    _udf("array_sort", _ARRAY, "array_sort(arr)", *_scalar(
        "array_sort(arr_int)", "list_sort(arr_int)", "arraySort(arr_int)")),
    _udf("array_reverse", _ARRAY, "array_reverse(arr)", *_scalar(
        "array_reverse(arr_int)", "list_reverse(arr_int)", "arrayReverse(arr_int)")),
    _udf("array_distinct", _ARRAY, "array_distinct(arr)", *_scalar(
        "array_distinct(arr_int)", "list_distinct(arr_int)", "arrayDistinct(arr_int)")),
    _udf("array_intersect", _ARRAY, "array_intersect(arr1, arr2)", *_scalar(
        "array_intersect(arr_int, arr_int_second)",
        "list_intersect(arr_int, arr_int_second)",
        "arrayIntersect(arr_int, arr_int_second)")),
    _udf("array_to_string", _ARRAY, "array_to_string(arr, ',')", *_scalar(
        "array_to_string(arr_int, ',')", "array_to_string(arr_int, ',')",
        "arrayStringConcat(arr_int, ',')")),
]

# ---------------------------------------------------------------------------
# Aggregate functions — ungrouped (11)
# ---------------------------------------------------------------------------

_AGG = "agg_ungrouped"

UDFS += [
    _udf("sum", _AGG, "sum(column)", *_agg_ungrouped(
        "sum(float_signed)", "sum(float_signed)", "sum(float_signed)")),
    _udf("avg", _AGG, "avg(column)", *_agg_ungrouped(
        "avg(float_signed)", "avg(float_signed)", "avg(float_signed)")),
    _udf("min_agg", _AGG, "min(column)", *_agg_ungrouped(
        "min(float_signed)", "min(float_signed)", "min(float_signed)")),
    _udf("max_agg", _AGG, "max(column)", *_agg_ungrouped(
        "max(float_signed)", "max(float_signed)", "max(float_signed)")),
    _udf("count_distinct", _AGG, "count(distinct column)", *_agg_ungrouped(
        "count(distinct int_small)", "count(distinct int_small)",
        "count(distinct int_small)")),
    _udf("approx_distinct", _AGG, "approx_distinct(column)", *_agg_ungrouped(
        "approx_distinct(int_large)", "approx_count_distinct(int_large)",
        "uniq(int_large)")),
    _udf("stddev", _AGG, "stddev(column)", *_agg_ungrouped(
        "stddev(float_signed)", "stddev(float_signed)", "stddevSamp(float_signed)")),
    _udf("variance", _AGG, "var(column)", *_agg_ungrouped(
        "var(float_signed)", "var_samp(float_signed)", "varSamp(float_signed)")),
    _udf("bit_and_agg", _AGG, "bit_and(column)", *_agg_ungrouped(
        "bit_and(int_small)", "bit_and(int_small)", "groupBitAnd(int_small)")),
    _udf("bit_xor_agg", _AGG, "bit_xor(column)", *_agg_ungrouped(
        "bit_xor(int_small)", "bit_xor(int_small)", "groupBitXor(int_small)")),
]

# ---------------------------------------------------------------------------
# Aggregate functions — grouped (17)
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
]
