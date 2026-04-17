"""
Verify conversion coverage: deterministic checks that a legacy-to-metadata
conversion didn't miss source tables, complexity signals, or operation types.

Usage:
    python verify_conversion.py --source legacy.sql --metadata generated.sql
    python verify_conversion.py --source file1.sql file2.py --metadata out.sql
    python verify_conversion.py --source legacy.sql --metadata out.sql --format json
    cat legacy.sql | python verify_conversion.py --stdin --metadata out.sql

Checks:
    1. Table coverage  — every source table mapped somewhere in metadata
    2. Complexity       — cursors/UDFs/RDD/recursion → custom function exists
    3. Operation types  — presence of each operation category in metadata

Exit codes:
    0  All checks PASS or WARN
    1  Any check FAILs (complexity signals with no custom function)

Pure stdlib Python — no external dependencies.
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Set, Tuple

# =============================================================================
# CONSTANTS
# =============================================================================

# Operation type → (expected accelerator transformation, source regex patterns)
# Each entry: key = internal operation name, value = (transformation_name, [regex_patterns])
# Patterns are applied with re.IGNORECASE and re.MULTILINE
OPERATION_MAP = {
    "join": (
        "join_data",
        [
            r'\bJOIN\b',                          # SQL JOIN (any type)
            r'\.join\s*\(',                        # PySpark/Pandas .join(
            r'\.merge\s*\(',                       # Pandas .merge(
            r'pd\.merge\s*\(',                     # pd.merge(
        ],
    ),
    "aggregate": (
        "aggregate_data",
        [
            r'\bGROUP\s+BY\b',                    # SQL GROUP BY
            r'\.groupBy\s*\(',                     # PySpark .groupBy(
            r'\.groupby\s*\(',                     # Pandas .groupby(
            r'\.agg\s*\(',                         # .agg(
        ],
    ),
    "conditional": (
        "conditional_column",
        [
            r'\bCASE\s+WHEN\b',                   # SQL CASE WHEN
            r'\bIIF\s*\(',                         # SQL IIF()
            r'F\.when\s*\(',                       # PySpark F.when(
            r'functions\.when\s*\(',               # PySpark functions.when(
            r'\.when\s*\(.*\.otherwise\s*\(',      # chained when().otherwise()
        ],
    ),
    "window_function": (
        "add_window_function",
        [
            r'\bROW_NUMBER\s*\(\s*\)',             # SQL ROW_NUMBER()
            r'\bRANK\s*\(\s*\)',                   # SQL RANK()
            r'\bDENSE_RANK\s*\(\s*\)',             # SQL DENSE_RANK()
            r'\bNTILE\s*\(',                       # SQL NTILE()
            r'\bLEAD\s*\(',                        # SQL LEAD()
            r'\bLAG\s*\(',                         # SQL LAG()
            r'\b\w+\s*\(\s*\)\s+OVER\s*\(',       # SUM() OVER(, AVG() OVER(, etc.
            r'Window\.',                           # PySpark Window.
            r'F\.row_number\s*\(',                 # PySpark F.row_number()
            r'F\.rank\s*\(',                       # PySpark F.rank()
            r'F\.dense_rank\s*\(',                 # PySpark F.dense_rank()
            r'F\.lag\s*\(',                        # PySpark F.lag()
            r'F\.lead\s*\(',                       # PySpark F.lead()
        ],
    ),
    "union": (
        "union_data",
        [
            r'\bUNION\b(?:\s+ALL\b)?',            # SQL UNION / UNION ALL
            r'\.union\s*\(',                       # PySpark .union(
            r'\.unionByName\s*\(',                 # PySpark .unionByName(
            r'pd\.concat\s*\(',                    # Pandas pd.concat(
        ],
    ),
    "pivot": (
        "pivot_data",
        [
            r'\bPIVOT\b',                         # SQL PIVOT
            r'\.pivot\s*\(',                       # PySpark/Pandas .pivot(
            r'\.pivot_table\s*\(',                 # Pandas .pivot_table(
        ],
    ),
    "unpivot": (
        "unpivot_data",
        [
            r'\bUNPIVOT\b',                       # SQL UNPIVOT
            r'\.melt\s*\(',                        # Pandas .melt(
            r'\.stack\s*\(',                       # Pandas .stack(
        ],
    ),
    "null_handling": (
        "apply_null_handling",
        [
            r'\bCOALESCE\s*\(',                   # SQL COALESCE()
            r'\bISNULL\s*\(',                      # SQL Server ISNULL()
            r'\bNVL\s*\(',                         # Oracle NVL()
            r'\bIFNULL\s*\(',                      # MySQL IFNULL()
            r'\.fillna\s*\(',                      # PySpark/Pandas .fillna(
            r'\.na\.fill\s*\(',                    # PySpark .na.fill(
        ],
    ),
    "type_cast": (
        "change_data_types",
        [
            r'\bCAST\s*\(',                        # SQL CAST()
            r'\bCONVERT\s*\(',                     # SQL Server CONVERT()
            r'::\s*\w+',                           # PostgreSQL ::type
            r'\.cast\s*\(',                        # PySpark .cast(
            r'\.astype\s*\(',                      # Pandas .astype(
        ],
    ),
    "dedup": (
        "drop_duplicates",
        [
            r'\bSELECT\s+DISTINCT\b',             # SQL SELECT DISTINCT
            r'\.distinct\s*\(',                    # PySpark .distinct()
            r'\.drop_duplicates\s*\(',             # PySpark/Pandas .drop_duplicates(
            r'\.dropDuplicates\s*\(',              # PySpark .dropDuplicates(
        ],
    ),
    "row_hash": (
        "add_row_hash",
        [
            r'\bHASHBYTES\s*\(',                   # SQL Server HASHBYTES()
            r'\bMD5\s*\(',                         # MD5()
            r'\bSHA2\s*\(',                        # SHA2()
            r'F\.md5\s*\(',                        # PySpark F.md5()
            r'F\.sha2\s*\(',                       # PySpark F.sha2()
            r'F\.hash\s*\(',                       # PySpark F.hash()
        ],
    ),
    "explode": (
        "explode_array",
        [
            r'\bSTRING_SPLIT\s*\(',                # SQL STRING_SPLIT()
            r'\bOPENJSON\s*\(',                    # SQL OPENJSON()
            r'F\.explode\s*\(',                    # PySpark F.explode()
            r'F\.posexplode\s*\(',                 # PySpark F.posexplode()
            r'F\.explode_outer\s*\(',              # PySpark F.explode_outer()
            r'\.explode\s*\(',                     # Pandas .explode(
        ],
    ),
    "datetime": (
        "transform_datetime",
        [
            r'\bDATEPART\s*\(',                    # SQL DATEPART()
            r'\bDATEADD\s*\(',                     # SQL DATEADD()
            r'\bDATEDIFF\s*\(',                    # SQL DATEDIFF()
            r'\bYEAR\s*\(',                        # SQL YEAR()
            r'\bMONTH\s*\(',                       # SQL MONTH()
            r'\bDAY\s*\(',                         # SQL DAY()
            r'F\.year\s*\(',                       # PySpark F.year()
            r'F\.month\s*\(',                      # PySpark F.month()
            r'F\.date_add\s*\(',                   # PySpark F.date_add()
            r'F\.datediff\s*\(',                   # PySpark F.datediff()
        ],
    ),
    "filter": (
        "filter_data",
        [
            r'\bWHERE\b',                          # SQL WHERE
            r'\bHAVING\b',                         # SQL HAVING
            r'\.filter\s*\(',                      # PySpark .filter(
            r'\.where\s*\(',                       # PySpark .where(
            r'\.query\s*\(',                       # Pandas .query(
        ],
    ),
    "derived_column": (
        "derived_column",
        [
            r'\.withColumn\s*\(',                  # PySpark .withColumn(
            r'\.withColumnRenamed\s*\(',           # PySpark .withColumnRenamed(
            r'\.assign\s*\(',                      # Pandas .assign(
        ],
        # Note: SQL "expr AS alias" is too broad to regex for here without
        # false positives on simple column aliasing. We detect it only via
        # PySpark/Pandas patterns; SQL derived columns are picked up by the
        # other operation checks (conditional, datetime, etc.).
    ),
    "string_functions": (
        "string_functions",
        [
            r'\bUPPER\s*\(',                       # SQL UPPER()
            r'\bLOWER\s*\(',                       # SQL LOWER()
            r'\bTRIM\s*\(',                        # SQL TRIM()
            r'\bLTRIM\s*\(',                       # SQL LTRIM()
            r'\bRTRIM\s*\(',                       # SQL RTRIM()
            r'\bREPLACE\s*\(',                     # SQL REPLACE()
            r'\bSUBSTRING\s*\(',                   # SQL SUBSTRING()
            r'F\.upper\s*\(',                      # PySpark F.upper()
            r'F\.lower\s*\(',                      # PySpark F.lower()
            r'F\.trim\s*\(',                       # PySpark F.trim()
            r'F\.regexp_replace\s*\(',             # PySpark F.regexp_replace()
        ],
    ),
}

# Complexity signals — patterns that MUST map to custom functions
COMPLEXITY_PATTERNS = [
    # SQL cursors
    ("cursor", r'\bDECLARE\b[^;]*\bCURSOR\b'),
    ("cursor", r'\bOPEN\s+\w+\s*;?\s*FETCH\b'),
    # SQL loops
    ("loop", r'\bWHILE\b.*\bBEGIN\b'),
    ("loop", r'\bLOOP\b'),
    ("goto", r'\bGOTO\b\s+\w+'),
    # Recursive CTE
    ("recursion", r'\bWITH\s+RECURSIVE\b'),
    # Second pattern: CTE that references itself (heuristic)
    ("recursion", r'\bUNION\s+ALL\b[^;]*\bSELECT\b[^;]*\bFROM\b[^;]*\bWHERE\b'),
    # Dynamic SQL
    ("dynamic_sql", r'\bEXEC(?:UTE)?\s*\('),
    ("dynamic_sql", r'\bsp_executesql\b'),
    # PySpark UDFs
    ("udf", r'@udf\b'),
    ("udf", r'\budf\s*\('),
    ("udf", r'spark\.udf\.register\s*\('),
    # RDD operations
    ("rdd", r'\.rdd\.'),
    ("rdd", r'\.rdd\b'),
    ("rdd", r'\.mapPartitions\s*\('),
    ("rdd", r'\.foreach\s*\('),
    ("rdd", r'\.foreachPartition\s*\('),
    # External API / HTTP calls
    ("api_call", r'\brequests\.(get|post|put|delete|patch)\s*\('),
    ("api_call", r'\burllib\b'),
    ("api_call", r'\bhttp\.client\b'),
    ("api_call", r'\bhttpx\b'),
    # ML inference
    ("ml_inference", r'\bmodel\.(predict|transform|score)\s*\('),
    ("ml_inference", r'\bmlflow\b'),
]

# Table name patterns — SQL keywords that precede table references
# Quoted identifier pattern: handles [brackets], "double_quotes", `backticks`, bare identifiers
_IDENT = r'(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|(\b[A-Za-z_]\w*\b))'
_MULTI_PART = (
    r'(?:'
    + _IDENT
    + r'(?:\.'
    + _IDENT
    + r'){0,3})'
)

# System tables to exclude from table coverage check
SYSTEM_TABLE_PREFIXES = (
    'sys.', 'information_schema.', 'pg_catalog.', 'pg_stat',
    'mysql.', 'performance_schema.', 'msdb.', 'master.', 'tempdb.',
)

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TableRef:
    """A table reference found in source code."""
    name: str
    normalized: str
    context: str  # FROM, JOIN, INTO, read.table, etc.
    line: int
    confidence: str  # high, medium, low
    is_cte: bool = False
    is_temp: bool = False


@dataclass
class ComplexitySignal:
    """A complexity pattern found in source code."""
    signal_type: str
    line: int
    snippet: str


@dataclass
class MetadataInfo:
    """Parsed metadata SQL information."""
    source_tables: List[str] = field(default_factory=list)
    target_entities: List[str] = field(default_factory=list)
    join_right_tables: List[str] = field(default_factory=list)
    query_texts: List[str] = field(default_factory=list)
    transformation_types: Set[str] = field(default_factory=set)
    custom_functions: List[str] = field(default_factory=list)
    table_ids: List[int] = field(default_factory=list)


@dataclass
class CheckResult:
    """Result of a single check."""
    name: str
    status: str  # PASS, WARN, FAIL, SKIP
    details: dict = field(default_factory=dict)


# =============================================================================
# COMMENT / STRING LITERAL STRIPPING
# =============================================================================

def _strip_sql_comments(text: str) -> Tuple[str, Dict[int, int]]:
    """Remove SQL comments (-- and /* */) preserving line structure.

    Returns cleaned text and a mapping from cleaned-line-number to
    original-line-number (1-based).
    """
    # Replace block comments with equivalent number of newlines to preserve line numbers
    def _block_replace(m):
        return '\n' * m.group(0).count('\n')

    text = re.sub(r'/\*.*?\*/', _block_replace, text, flags=re.DOTALL)

    lines = text.split('\n')
    cleaned = []
    line_map = {}
    for i, line in enumerate(lines):
        # Remove single-line comment portion
        stripped = re.sub(r'--.*$', '', line)
        cleaned.append(stripped)
        line_map[i + 1] = i + 1  # 1-based

    return '\n'.join(cleaned), line_map


def _strip_python_comments(text: str) -> Tuple[str, Dict[int, int]]:
    """Remove Python comments (# and triple-quoted docstrings) preserving line structure."""
    # Replace triple-quoted strings with equivalent newlines
    def _triple_replace(m):
        return '\n' * m.group(0).count('\n')

    text = re.sub(r'""".*?"""', _triple_replace, text, flags=re.DOTALL)
    text = re.sub(r"'''.*?'''", _triple_replace, text, flags=re.DOTALL)

    lines = text.split('\n')
    cleaned = []
    line_map = {}
    for i, line in enumerate(lines):
        stripped = re.sub(r'#.*$', '', line)
        cleaned.append(stripped)
        line_map[i + 1] = i + 1

    return '\n'.join(cleaned), line_map


def strip_comments(text: str, language: str) -> str:
    """Strip comments based on detected language."""
    if language == 'sql':
        cleaned, _ = _strip_sql_comments(text)
        return cleaned
    elif language == 'python':
        cleaned, _ = _strip_python_comments(text)
        return cleaned
    else:
        # Mixed — strip both
        cleaned, _ = _strip_sql_comments(text)
        cleaned, _ = _strip_python_comments(cleaned)
        return cleaned


# =============================================================================
# LANGUAGE DETECTION
# =============================================================================

def detect_language(text: str, file_path: Optional[str] = None) -> str:
    """Detect whether source is SQL, Python, or mixed."""
    if file_path:
        ext = Path(file_path).suffix.lower()
        if ext in ('.sql', '.ddl', '.dml'):
            return 'sql'
        if ext in ('.py', '.pyw'):
            return 'python'

    python_signals = len(re.findall(
        r'\b(?:def |import |from pyspark|from pandas|spark\.|DataFrame)\b', text
    ))
    sql_signals = len(re.findall(
        r'\b(?:SELECT|INSERT\s+INTO|CREATE\s+TABLE|ALTER\s+TABLE|DELETE\s+FROM|MERGE\s+INTO)\b',
        text, re.IGNORECASE
    ))

    if python_signals > 0 and sql_signals > 0:
        return 'mixed'
    if python_signals > sql_signals:
        return 'python'
    return 'sql'


# =============================================================================
# TABLE NAME NORMALIZATION
# =============================================================================

def normalize_table_name(raw: str) -> str:
    """Strip quoting and lowercase a table reference.

    '[dbo].[Orders]' → 'dbo.orders'
    '"PUBLIC"."ORDERS"' → 'public.orders'
    '`schema`.`table`' → 'schema.table'
    """
    # Remove bracket quoting
    name = re.sub(r'\[([^\]]+)\]', r'\1', raw)
    # Remove double-quote quoting
    name = re.sub(r'"([^"]+)"', r'\1', name)
    # Remove backtick quoting
    name = re.sub(r'`([^`]+)`', r'\1', name)
    return name.lower().strip()


def extract_table_part(normalized: str) -> str:
    """Extract just the table name from a multi-part reference.

    'database.schema.table' → 'table'
    'schema.table' → 'table'
    'table' → 'table'
    """
    parts = normalized.split('.')
    return parts[-1]


def tables_match(source_norm: str, metadata_norm: str) -> bool:
    """Check if two normalized table names refer to the same table.

    Tries exact match first, then table-name-only match.
    """
    if source_norm == metadata_norm:
        return True
    # Compare just the table part (ignoring schema/database differences)
    return extract_table_part(source_norm) == extract_table_part(metadata_norm)


def is_system_table(normalized: str) -> bool:
    """Check if a table reference is a system/catalog table."""
    return any(normalized.startswith(prefix) for prefix in SYSTEM_TABLE_PREFIXES)


# =============================================================================
# SOURCE PARSING — TABLE EXTRACTION
# =============================================================================

def _extract_sql_table_refs(text: str) -> List[Tuple[str, str, int]]:
    """Extract table references from SQL code.

    Returns list of (raw_name, context, line_number).
    """
    results = []
    lines = text.split('\n')

    # Quoted or bare identifier (single part)
    # Allow # prefix for SQL temp tables (#staging, ##global_temp)
    ident = r'(?:\[([^\]]+)\]|"([^"]+)"|`([^`]+)`|(#{0,2}[A-Za-z_]\w*))'
    # Multi-part: 1-4 parts separated by dots
    multi = ident + r'(?:\.' + ident + r'){0,3}'

    # SQL keywords that precede table names
    table_contexts = [
        (r'\bFROM\s+', 'FROM'),
        (r'\b(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\s+', 'JOIN'),
        (r'\bINTO\s+', 'INTO'),
        (r'\bUPDATE\s+', 'UPDATE'),
        (r'\bDELETE\s+FROM\s+', 'DELETE'),
        (r'\bMERGE\s+INTO\s+', 'MERGE'),
        (r'\bCROSS\s+APPLY\s+', 'APPLY'),
        (r'\bOUTER\s+APPLY\s+', 'APPLY'),
    ]

    for line_num, line in enumerate(lines, 1):
        for kw_pattern, context in table_contexts:
            for m in re.finditer(kw_pattern + r'(' + multi + r')', line, re.IGNORECASE):
                raw = m.group(1) if m.lastindex and m.group(1) else ''
                # The full match after keyword is the whole captured group
                # We need to extract the table reference portion
                full_match = m.group(0)
                # Strip the keyword part to get the table reference
                table_part = re.sub(kw_pattern, '', full_match, count=1, flags=re.IGNORECASE).strip()
                if table_part and not table_part.startswith('('):
                    results.append((table_part, context, line_num))

    return results


def _extract_pyspark_table_refs(text: str) -> List[Tuple[str, str, int]]:
    """Extract table references from PySpark code."""
    results = []
    lines = text.split('\n')

    patterns = [
        # spark.read.table("name")
        (r'spark\.read\.table\s*\(\s*["\']([^"\']+)["\']', 'read.table'),
        # spark.table("name")
        (r'spark\.table\s*\(\s*["\']([^"\']+)["\']', 'read.table'),
        # spark.read.format(...).load("path")
        (r'\.load\s*\(\s*["\']([^"\']+)["\']', 'read.load'),
        # spark.read.parquet/csv/json/orc("path")
        (r'spark\.read\.(?:parquet|csv|json|orc)\s*\(\s*["\']([^"\']+)["\']', 'read.file'),
        # DeltaTable.forName(spark, "name")
        (r'DeltaTable\.forName\s*\([^,]+,\s*["\']([^"\']+)["\']', 'DeltaTable.forName'),
        # DeltaTable.forPath(spark, "path")
        (r'DeltaTable\.forPath\s*\([^,]+,\s*["\']([^"\']+)["\']', 'DeltaTable.forPath'),
        # .write.saveAsTable("name")
        (r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']', 'write.saveAsTable'),
        # .write.insertInto("name")
        (r'\.insertInto\s*\(\s*["\']([^"\']+)["\']', 'write.insertInto'),
        # .option("dbtable", "schema.table")
        (r'\.option\s*\(\s*["\']dbtable["\']\s*,\s*["\']([^"\']+)["\']', 'jdbc.dbtable'),
        # createOrReplaceTempView("name")
        (r'\.createOrReplaceTempView\s*\(\s*["\']([^"\']+)["\']', 'createTempView'),
        # spark.catalog.cacheTable("name")
        (r'spark\.catalog\.cacheTable\s*\(\s*["\']([^"\']+)["\']', 'cacheTable'),
    ]

    for line_num, line in enumerate(lines, 1):
        for pat, ctx in patterns:
            for m in re.finditer(pat, line):
                results.append((m.group(1), ctx, line_num))

    # Also find table names inside spark.sql("...") strings
    for line_num, line in enumerate(lines, 1):
        for m in re.finditer(r'spark\.sql\s*\(\s*(?:f?["\']|f?""")(.*?)(?:["\']|""")\s*\)', line, re.DOTALL):
            sql_str = m.group(1)
            for ref_name, ref_ctx, _ in _extract_sql_table_refs(sql_str):
                results.append((ref_name, f'spark.sql.{ref_ctx}', line_num))

    return results


def _extract_pandas_table_refs(text: str) -> List[Tuple[str, str, int]]:
    """Extract table references from Pandas code."""
    results = []
    lines = text.split('\n')

    patterns = [
        (r'pd\.read_csv\s*\(\s*["\']([^"\']+)["\']', 'pd.read_csv'),
        (r'pd\.read_excel\s*\(\s*["\']([^"\']+)["\']', 'pd.read_excel'),
        (r'pd\.read_parquet\s*\(\s*["\']([^"\']+)["\']', 'pd.read_parquet'),
        (r'pd\.read_json\s*\(\s*["\']([^"\']+)["\']', 'pd.read_json'),
    ]

    for line_num, line in enumerate(lines, 1):
        for pat, ctx in patterns:
            for m in re.finditer(pat, line):
                results.append((m.group(1), ctx, line_num))

    # pd.read_sql("SELECT ... FROM table", ...) — extract table from SQL inside
    for line_num, line in enumerate(lines, 1):
        for m in re.finditer(r'pd\.read_sql(?:_query|_table)?\s*\(\s*["\']([^"\']+)["\']', line):
            sql_str = m.group(1)
            for ref_name, ref_ctx, _ in _extract_sql_table_refs(sql_str):
                results.append((ref_name, f'pd.read_sql.{ref_ctx}', line_num))

    return results


def _extract_cte_names(text: str) -> Set[str]:
    """Extract CTE names from SQL: WITH name AS, ;WITH name AS."""
    ctes = set()
    for m in re.finditer(
        r'(?:^|;)\s*WITH\s+(\w+)\s+AS\s*\(',
        text, re.IGNORECASE | re.MULTILINE
    ):
        ctes.add(m.group(1).lower())
    # Chained CTEs: , name AS (
    for m in re.finditer(
        r',\s*(\w+)\s+AS\s*\(', text, re.IGNORECASE
    ):
        ctes.add(m.group(1).lower())
    return ctes


def _extract_temp_names(text: str) -> Set[str]:
    """Extract temp table/view names."""
    temps = set()
    # SQL temp tables: #name, ##name
    for m in re.finditer(r'(#{1,2}\w+)', text):
        temps.add(m.group(1).lower())
    # SQL table variables: DECLARE @name TABLE
    for m in re.finditer(r'DECLARE\s+(@\w+)\s+TABLE\b', text, re.IGNORECASE):
        temps.add(m.group(1).lower())
    # CREATE TEMP TABLE
    for m in re.finditer(r'CREATE\s+TEMP(?:ORARY)?\s+TABLE\s+(\w+)', text, re.IGNORECASE):
        temps.add(m.group(1).lower())
    # PySpark createOrReplaceTempView
    for m in re.finditer(r'\.createOrReplaceTempView\s*\(\s*["\']([^"\']+)["\']', text):
        temps.add(m.group(1).lower())
    return temps


def extract_source_tables(text: str, language: str) -> List[TableRef]:
    """Extract all table references from source code."""
    cte_names = _extract_cte_names(text)
    temp_names = _extract_temp_names(text)

    raw_refs: List[Tuple[str, str, int]] = []

    if language in ('sql', 'mixed'):
        raw_refs.extend(_extract_sql_table_refs(text))
    if language in ('python', 'mixed'):
        raw_refs.extend(_extract_pyspark_table_refs(text))
        raw_refs.extend(_extract_pandas_table_refs(text))

    tables = []
    seen = set()

    for raw_name, context, line in raw_refs:
        normalized = normalize_table_name(raw_name)
        if not normalized or len(normalized) < 2:
            continue

        # Determine confidence
        confidence = 'high'
        if context.startswith('spark.sql.'):
            confidence = 'medium'  # extracted from embedded SQL string
        if context in ('read.load', 'read.file', 'DeltaTable.forPath',
                       'pd.read_csv', 'pd.read_excel', 'pd.read_parquet', 'pd.read_json'):
            confidence = 'medium'  # file path, not table name

        is_cte = extract_table_part(normalized) in cte_names
        is_temp = (
            extract_table_part(normalized) in temp_names
            or normalized.startswith('#')
            or normalized.startswith('@')
            or context == 'createTempView'
        )

        # Dedup by normalized name
        dedup_key = (normalized, is_cte, is_temp)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        tables.append(TableRef(
            name=raw_name,
            normalized=normalized,
            context=context,
            line=line,
            confidence=confidence,
            is_cte=is_cte,
            is_temp=is_temp,
        ))

    return tables


# =============================================================================
# SOURCE PARSING — COMPLEXITY SIGNALS
# =============================================================================

def detect_complexity_signals(text: str) -> List[ComplexitySignal]:
    """Detect patterns that require custom functions."""
    signals = []
    lines = text.split('\n')
    seen_types_at_line = set()

    for sig_type, pattern in COMPLEXITY_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE):
            # Find line number
            line_num = text[:m.start()].count('\n') + 1
            # Avoid duplicate signals of same type on same line
            key = (sig_type, line_num)
            if key in seen_types_at_line:
                continue
            seen_types_at_line.add(key)

            snippet = text[m.start():m.start() + 60].split('\n')[0].strip()
            signals.append(ComplexitySignal(
                signal_type=sig_type,
                line=line_num,
                snippet=snippet,
            ))

    return signals


# =============================================================================
# SOURCE PARSING — OPERATION TYPE DETECTION
# =============================================================================

def detect_operation_types(text: str) -> Dict[str, bool]:
    """Detect which operation categories are present (boolean per type)."""
    result = {}
    for op_name, (_, patterns) in OPERATION_MAP.items():
        found = False
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE | re.MULTILINE):
                found = True
                break
        result[op_name] = found
    return result


# =============================================================================
# METADATA SQL PARSING
# =============================================================================

def parse_metadata_sql(content: str) -> MetadataInfo:
    """Parse generated metadata SQL to extract tables, transformations, custom functions.

    Regex patterns adapted from scripts/summarize_metadata.py.
    """
    info = MetadataInfo()
    sections = _find_insert_sections(content)

    # Parse orchestration
    if 'orchestration' in sections:
        orch_pattern = (
            r"\(\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,"
            r"\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,"
            r"\s*(?:'((?:[^']|'')*)'|NULL)\s*,"
            r"\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*\)"
        )
        for line in sections['orchestration']['lines']:
            for m in re.finditer(orch_pattern, line):
                table_id = int(m.group(3))
                target_entity = m.group(5).replace("''", "'")
                info.table_ids.append(table_id)
                info.target_entities.append(normalize_table_name(target_entity))

    # Parse primary config
    if 'primary_config' in sections:
        pc_pattern = r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'"
        for line in sections['primary_config']['lines']:
            for m in re.finditer(pc_pattern, line):
                category = m.group(2).replace("''", "'")
                name = m.group(3).replace("''", "'")
                value = m.group(4).replace("''", "'")

                if category == 'source_details' and name == 'table_name':
                    info.source_tables.append(normalize_table_name(value))
                elif category == 'source_details' and name == 'datastore_name':
                    info.source_tables.append(normalize_table_name(value))
                elif category == 'source_details' and name == 'query':
                    info.query_texts.append(value)
                elif category == 'source_details' and name in (
                    'custom_transformation_function',
                    'custom_table_ingestion_function',
                    'custom_staging_function',
                    'custom_file_ingestion_function',
                ):
                    info.custom_functions.append(value)

    # Parse advanced config
    if 'advanced_config' in sections:
        ac_pattern = (
            r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,"
            r"\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,"
            r"\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)"
        )
        for line in sections['advanced_config']['lines']:
            for m in re.finditer(ac_pattern, line):
                category = m.group(2).replace("''", "'")
                name = m.group(3).replace("''", "'")
                attr_name = m.group(5).replace("''", "'")
                attr_value = m.group(6).replace("''", "'")

                if category == 'data_transformation_steps':
                    info.transformation_types.add(name)
                    # Capture join right_table_name references
                    if name == 'join_data' and attr_name == 'right_table_name':
                        info.join_right_tables.append(normalize_table_name(attr_value))

    return info


def _find_insert_sections(content: str) -> dict:
    """Find the three INSERT INTO sections in metadata SQL.

    Adapted from scripts/summarize_metadata.py.
    """
    sections = {}
    lines = content.split('\n')
    current_section = None
    section_start = 0

    for i, line in enumerate(lines, 1):
        match = re.search(
            r'INSERT INTO\s+(?:\[?dbo\]?\.)?\[?(Data_Pipeline_\w+|Datastore_Configuration)\]?',
            line, re.IGNORECASE
        )
        if match:
            if current_section:
                sections[current_section] = {
                    'start': section_start,
                    'end': i - 1,
                    'lines': lines[section_start - 1:i - 1],
                }
            table_name = match.group(1).lower()
            if 'orchestration' in table_name:
                current_section = 'orchestration'
            elif 'primary' in table_name:
                current_section = 'primary_config'
            elif 'advanced' in table_name:
                current_section = 'advanced_config'
            else:
                current_section = None
            section_start = i

    if current_section:
        sections[current_section] = {
            'start': section_start,
            'end': len(lines),
            'lines': lines[section_start - 1:],
        }

    return sections


# =============================================================================
# THREE CHECKS
# =============================================================================

def check_table_coverage(
    source_tables: List[TableRef], metadata: MetadataInfo
) -> CheckResult:
    """CHECK 1: Every source table must appear somewhere in metadata."""
    # Build set of all table references in metadata
    meta_tables: Set[str] = set()
    for t in metadata.source_tables:
        meta_tables.add(t)
    for t in metadata.target_entities:
        meta_tables.add(t)
    for t in metadata.join_right_tables:
        meta_tables.add(t)
    # Also search inside query texts
    query_text_blob = ' '.join(metadata.query_texts).lower()

    # Only check non-CTE, non-temp, non-system tables
    checkable = [
        t for t in source_tables
        if not t.is_cte and not t.is_temp and not is_system_table(t.normalized)
    ]

    matched = []
    unmatched = []

    for src in checkable:
        found = False
        # Try exact match against metadata table set
        if src.normalized in meta_tables:
            found = True
        else:
            # Try table-name-only match
            src_table_part = extract_table_part(src.normalized)
            for mt in meta_tables:
                if extract_table_part(mt) == src_table_part:
                    found = True
                    break
        # Also search in query texts
        if not found and src.normalized in query_text_blob:
            found = True
        if not found:
            src_table_part = extract_table_part(src.normalized)
            if src_table_part in query_text_blob:
                found = True

        if found:
            matched.append(src.normalized)
        else:
            unmatched.append({
                'name': src.name,
                'normalized': src.normalized,
                'line': src.line,
                'context': src.context,
                'confidence': src.confidence,
            })

    # Find extra tables in metadata not in source
    source_table_parts = {extract_table_part(t.normalized) for t in checkable}
    extra = [
        t for t in sorted(meta_tables)
        if extract_table_part(t) not in source_table_parts and t
    ]

    status = 'PASS'
    if unmatched:
        status = 'WARN'

    return CheckResult(
        name='table_coverage',
        status=status,
        details={
            'source_tables': len(checkable),
            'matched': len(matched),
            'unmatched': unmatched,
            'extra_in_metadata': extra,
            'excluded_ctes': [t.normalized for t in source_tables if t.is_cte],
            'excluded_temps': [t.normalized for t in source_tables if t.is_temp],
        },
    )


def check_complexity_coverage(
    signals: List[ComplexitySignal], metadata: MetadataInfo
) -> CheckResult:
    """CHECK 2: Complexity signals must have corresponding custom functions."""
    if not signals:
        return CheckResult(
            name='complexity',
            status='SKIP',
            details={'verdict': 'No complexity signals found in source'},
        )

    has_custom = len(metadata.custom_functions) > 0

    if has_custom:
        return CheckResult(
            name='complexity',
            status='PASS',
            details={
                'signals': [
                    {'type': s.signal_type, 'line': s.line, 'snippet': s.snippet}
                    for s in signals
                ],
                'custom_functions_in_metadata': metadata.custom_functions,
                'verdict': 'Complexity signals found and custom functions exist in metadata',
            },
        )
    else:
        return CheckResult(
            name='complexity',
            status='FAIL',
            details={
                'signals': [
                    {'type': s.signal_type, 'line': s.line, 'snippet': s.snippet}
                    for s in signals
                ],
                'custom_functions_in_metadata': [],
                'verdict': (
                    f'Source has {len(signals)} complexity signal(s) '
                    f'({", ".join(sorted(set(s.signal_type for s in signals)))}) '
                    f'but metadata has NO custom functions'
                ),
            },
        )


def check_operation_coverage(
    source_ops: Dict[str, bool], metadata: MetadataInfo
) -> CheckResult:
    """CHECK 3: Each detected operation type should have a corresponding transformation."""
    matched = []
    unmatched = []

    for op_name, present in source_ops.items():
        if not present:
            continue
        expected_transform = OPERATION_MAP[op_name][0]
        if expected_transform in metadata.transformation_types:
            matched.append(expected_transform)
        else:
            unmatched.append({
                'source_type': op_name,
                'expected_transformation': expected_transform,
            })

    status = 'PASS'
    if unmatched:
        status = 'WARN'

    return CheckResult(
        name='operation_coverage',
        status=status,
        details={
            'matched': sorted(set(matched)),
            'unmatched': unmatched,
        },
    )


# =============================================================================
# OUTPUT FORMATTING
# =============================================================================

def format_json(
    source_files: List[str],
    language: str,
    check1: CheckResult,
    check2: CheckResult,
    check3: CheckResult,
) -> dict:
    """Build the JSON output structure."""
    return {
        'source_files': source_files,
        'language': language,
        'check_1_tables': {
            'status': check1.status,
            **check1.details,
        },
        'check_2_complexity': {
            'status': check2.status,
            **check2.details,
        },
        'check_3_operations': {
            'status': check3.status,
            **check3.details,
        },
    }


def format_markdown(result: dict) -> str:
    """Render the JSON result as a human-readable Markdown summary."""
    lines = []
    lines.append('# Conversion Verification Report\n')

    # Table coverage
    tc = result['check_1_tables']
    icon = {'PASS': '✅', 'WARN': '⚠️', 'FAIL': '❌', 'SKIP': '⏭️'}
    lines.append(f"## {icon.get(tc['status'], '?')} CHECK 1: Table Coverage ({tc['status']})\n")
    lines.append(f"- Source tables (checked): **{tc['source_tables']}**")
    lines.append(f"- Matched: **{tc['matched']}**")
    if tc.get('excluded_ctes'):
        lines.append(f"- CTEs excluded: {', '.join(tc['excluded_ctes'])}")
    if tc.get('excluded_temps'):
        lines.append(f"- Temp tables excluded: {', '.join(tc['excluded_temps'])}")
    if tc['unmatched']:
        lines.append(f"\n**Unmatched source tables ({len(tc['unmatched'])}):**\n")
        lines.append('| Table | Context | Line | Confidence |')
        lines.append('|-------|---------|------|------------|')
        for u in tc['unmatched']:
            lines.append(f"| `{u['name']}` | {u['context']} | {u['line']} | {u['confidence']} |")
    if tc.get('extra_in_metadata'):
        lines.append(f"\nExtra tables in metadata (not in source): {', '.join(f'`{t}`' for t in tc['extra_in_metadata'])}")
    lines.append('')

    # Complexity
    cc = result['check_2_complexity']
    lines.append(f"## {icon.get(cc['status'], '?')} CHECK 2: Complexity ({cc['status']})\n")
    lines.append(f"_{cc['verdict']}_\n")
    if cc.get('signals'):
        lines.append('| Type | Line | Snippet |')
        lines.append('|------|------|---------|')
        for s in cc['signals']:
            lines.append(f"| {s['type']} | {s['line']} | `{s['snippet']}` |")
    if cc.get('custom_functions_in_metadata'):
        lines.append(f"\nCustom functions in metadata: {', '.join(f'`{f}`' for f in cc['custom_functions_in_metadata'])}")
    lines.append('')

    # Operations
    oc = result['check_3_operations']
    lines.append(f"## {icon.get(oc['status'], '?')} CHECK 3: Operation Types ({oc['status']})\n")
    if oc['matched']:
        lines.append(f"Matched: {', '.join(f'`{t}`' for t in oc['matched'])}")
    if oc['unmatched']:
        lines.append(f"\n**Unmatched ({len(oc['unmatched'])}):**\n")
        lines.append('| Source Operation | Expected Transformation |')
        lines.append('|-----------------|------------------------|')
        for u in oc['unmatched']:
            lines.append(f"| {u['source_type']} | `{u['expected_transformation']}` |")
    lines.append('')

    return '\n'.join(lines)


# =============================================================================
# MAIN
# =============================================================================

def run(source_texts: List[Tuple[str, Optional[str]]], metadata_text: str,
        output_format: str = 'both') -> Tuple[dict, int]:
    """Run all checks.

    Args:
        source_texts: list of (text_content, file_path_or_None)
        metadata_text: content of the metadata SQL file
        output_format: 'json', 'markdown', or 'both'

    Returns:
        (result_dict, exit_code)
    """
    # Combine source texts
    all_text = ''
    source_files = []
    language = 'sql'

    for text, path in source_texts:
        lang = detect_language(text, path)
        if all_text:
            all_text += '\n'
        all_text += text
        source_files.append(path or '<stdin>')
        # Upgrade language to most specific
        if lang == 'mixed' or (language != lang and language != 'mixed'):
            if lang != language:
                language = 'mixed' if all_text else lang

    # Re-detect on combined text if multiple files
    if len(source_texts) > 1:
        language = detect_language(all_text)
    elif len(source_texts) == 1:
        language = detect_language(source_texts[0][0], source_texts[0][1])

    # Strip comments
    cleaned = strip_comments(all_text, language)

    # Source parsing
    source_tables = extract_source_tables(cleaned, language)
    complexity_signals = detect_complexity_signals(cleaned)
    operation_types = detect_operation_types(cleaned)

    # Metadata parsing
    metadata = parse_metadata_sql(metadata_text)

    # Run checks
    check1 = check_table_coverage(source_tables, metadata)
    check2 = check_complexity_coverage(complexity_signals, metadata)
    check3 = check_operation_coverage(operation_types, metadata)

    result = format_json(source_files, language, check1, check2, check3)

    # Print output
    if output_format in ('json', 'both'):
        print(json.dumps(result, indent=2))
    if output_format in ('markdown', 'both'):
        if output_format == 'both':
            print('\n---\n')
        print(format_markdown(result))

    # Exit code: 1 if any FAIL
    exit_code = 0
    if check1.status == 'FAIL' or check2.status == 'FAIL' or check3.status == 'FAIL':
        exit_code = 1

    return result, exit_code


def main():
    parser = argparse.ArgumentParser(
        description='Verify legacy-to-metadata conversion coverage',
    )
    parser.add_argument(
        '--source', nargs='+',
        help='One or more legacy source files (.sql, .py)',
    )
    parser.add_argument(
        '--metadata', required=True,
        help='Path to the generated metadata SQL file',
    )
    parser.add_argument(
        '--stdin', action='store_true',
        help='Read source from stdin instead of --source files',
    )
    parser.add_argument(
        '--format', choices=['json', 'markdown', 'both'], default='both',
        help='Output format (default: both)',
    )

    args = parser.parse_args()

    if not args.source and not args.stdin:
        parser.error('Either --source or --stdin is required')

    # Read source files
    source_texts = []
    if args.stdin:
        source_texts.append((sys.stdin.read(), None))
    else:
        for path in args.source:
            p = Path(path)
            if not p.exists():
                print(f'Error: source file not found: {path}', file=sys.stderr)
                sys.exit(2)
            source_texts.append((p.read_text(encoding='utf-8'), str(p)))

    # Read metadata file
    meta_path = Path(args.metadata)
    if not meta_path.exists():
        print(f'Error: metadata file not found: {args.metadata}', file=sys.stderr)
        sys.exit(2)
    metadata_text = meta_path.read_text(encoding='utf-8')

    _, exit_code = run(source_texts, metadata_text, args.format)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
