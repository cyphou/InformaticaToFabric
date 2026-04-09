"""
Synthetic Mapping Generator — Sprint 95
Generates N synthetic Informatica XML mappings with configurable complexity.

Usage:
    python benchmarks/generate_mappings.py --count 100
    python benchmarks/generate_mappings.py --count 500 --complexity-mix 0.4,0.35,0.25
"""

import argparse
import os
import random
import textwrap
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
OUTPUT_DIR = WORKSPACE / "benchmarks" / "synthetic_input" / "mappings"

# Transformation types by complexity tier
SIMPLE_TX = ["SQ", "EXP", "FIL", "TGT"]
MEDIUM_TX = ["SQ", "EXP", "FIL", "LKP", "AGG", "JNR", "SRT", "TGT"]
COMPLEX_TX = ["SQ", "EXP", "FIL", "LKP", "AGG", "JNR", "SRT", "RTR", "UPD", "UNI", "NRM", "RNK", "SEQ", "TC", "TGT"]

SOURCE_TABLES = [
    "CUSTOMERS", "ORDERS", "ORDER_ITEMS", "PRODUCTS", "SUPPLIERS",
    "EMPLOYEES", "DEPARTMENTS", "REGIONS", "TRANSACTIONS", "INVENTORY",
    "ACCOUNTS", "CONTACTS", "ADDRESSES", "PAYMENTS", "SHIPMENTS",
    "CATEGORIES", "PRICES", "DISCOUNTS", "RETURNS", "INVOICES",
]

TARGET_TABLES = [
    "DIM_CUSTOMER", "DIM_PRODUCT", "DIM_EMPLOYEE", "DIM_SUPPLIER",
    "DIM_DATE", "DIM_REGION", "DIM_CATEGORY", "DIM_STORE",
    "FACT_SALES", "FACT_INVENTORY", "FACT_ORDERS", "FACT_PAYMENTS",
    "FACT_SHIPMENTS", "FACT_TRANSACTIONS", "FACT_RETURNS",
    "AGG_DAILY_SALES", "AGG_MONTHLY_REVENUE", "AGG_CUSTOMER_LIFETIME",
]

COLUMN_NAMES = [
    "ID", "NAME", "DESCRIPTION", "AMOUNT", "QUANTITY", "PRICE",
    "STATUS", "TYPE", "CODE", "DATE", "TIMESTAMP", "FLAG",
    "CREATED_DATE", "MODIFIED_DATE", "CREATED_BY", "MODIFIED_BY",
    "EMAIL", "PHONE", "ADDRESS", "CITY", "STATE", "ZIP", "COUNTRY",
]

SQL_OVERRIDES = [
    "SELECT /*+ PARALLEL(4) */ * FROM {src} WHERE status = 'ACTIVE'",
    "SELECT NVL(amount, 0) AS amount, TRUNC(SYSDATE) AS load_date FROM {src}",
    "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM {src}",
    "SELECT a.*, b.name FROM {src} a LEFT JOIN LOOKUP_TABLE b ON a.code = b.code",
    "SELECT ROWNUM AS row_id, t.* FROM (SELECT * FROM {src} ORDER BY id) t WHERE ROWNUM <= 10000",
]


def _random_columns(n=None):
    """Generate a random set of column names."""
    if n is None:
        n = random.randint(5, 20)
    return random.sample(COLUMN_NAMES, min(n, len(COLUMN_NAMES)))


def _generate_column_xml(columns):
    """Generate TRANSFORMFIELD XML elements."""
    lines = []
    for col in columns:
        datatype = random.choice(["string", "integer", "decimal", "date/time"])
        precision = random.randint(10, 50)
        lines.append(
            f'            <TRANSFORMFIELD DATATYPE="{datatype}" NAME="{col}" '
            f'PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="{precision}" SCALE="0"/>'
        )
    return "\n".join(lines)


def generate_mapping_xml(mapping_name, complexity="Simple", idx=0):
    """Generate a synthetic Informatica mapping XML string.

    Args:
        mapping_name: Mapping name (e.g. M_BENCH_0001).
        complexity: Simple, Medium, or Complex.
        idx: Index for deterministic variation.

    Returns:
        str: XML content for the mapping.
    """
    random.seed(idx)

    if complexity == "Simple":
        tx_chain = random.sample(SIMPLE_TX, len(SIMPLE_TX))
        source_count = 1
        has_sql_override = False
    elif complexity == "Medium":
        tx_count = random.randint(4, 6)
        tx_chain = random.sample(MEDIUM_TX, min(tx_count, len(MEDIUM_TX)))
        source_count = random.randint(1, 2)
        has_sql_override = random.random() < 0.3
    else:  # Complex
        tx_count = random.randint(6, 12)
        tx_chain = random.sample(COMPLEX_TX, min(tx_count, len(COMPLEX_TX)))
        source_count = random.randint(2, 4)
        has_sql_override = random.random() < 0.6

    sources = random.sample(SOURCE_TABLES, source_count)
    target = random.choice(TARGET_TABLES)
    columns = _random_columns(random.randint(5, 15))

    # Build transformation XML blocks
    tx_blocks = []
    for tx_type in tx_chain:
        tx_name = f"TX_{tx_type}_{mapping_name}"
        cols_xml = _generate_column_xml(columns[:random.randint(3, len(columns))])
        tx_blocks.append(textwrap.dedent(f"""\
        <TRANSFORMATION NAME="{tx_name}" TYPE="{tx_type}">
{cols_xml}
        </TRANSFORMATION>"""))

    # SQL override
    sql_block = ""
    if has_sql_override:
        sql_text = random.choice(SQL_OVERRIDES).format(src=sources[0])
        sql_block = f"""
        <TABLEATTRIBUTE NAME="Sql Query" VALUE="{sql_text}"/>"""

    source_blocks = []
    for src in sources:
        source_blocks.append(textwrap.dedent(f"""\
        <SOURCE DATABASETYPE="Oracle" NAME="SRC_{src}" OWNERNAME="SCHEMAOWNER">
{_generate_column_xml(columns)}
        </SOURCE>"""))

    target_block = textwrap.dedent(f"""\
        <TARGET DATABASETYPE="Oracle" NAME="TGT_{target}" OWNERNAME="DW">
{_generate_column_xml(columns)}
        </TARGET>""")

    xml_content = textwrap.dedent(f"""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE POWERMART>
    <POWERMART>
      <REPOSITORY NAME="BENCH_REPO">
        <FOLDER NAME="BENCH_FOLDER">
          <MAPPING NAME="{mapping_name}" ISVALID="YES">
    {"".join(source_blocks)}
    {target_block}
    {"".join(tx_blocks)}{sql_block}
          </MAPPING>
        </FOLDER>
      </REPOSITORY>
    </POWERMART>
    """)

    return xml_content


def generate_benchmark_mappings(count=100, complexity_mix=None, output_dir=None):
    """Generate N synthetic mappings with given complexity distribution.

    Args:
        count: Number of mappings to generate.
        complexity_mix: Tuple of (simple_pct, medium_pct, complex_pct).
        output_dir: Output directory for mapping XML files.

    Returns:
        dict with generation stats.
    """
    if complexity_mix is None:
        complexity_mix = (0.4, 0.35, 0.25)

    output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    simple_count = int(count * complexity_mix[0])
    medium_count = int(count * complexity_mix[1])
    complex_count = count - simple_count - medium_count

    stats = {"simple": 0, "medium": 0, "complex": 0, "total": 0, "files": []}

    idx = 0
    for i in range(simple_count):
        name = f"M_BENCH_{idx:04d}"
        xml = generate_mapping_xml(name, "Simple", idx)
        path = output_dir / f"{name}.xml"
        path.write_text(xml, encoding="utf-8")
        stats["simple"] += 1
        stats["files"].append(str(path))
        idx += 1

    for i in range(medium_count):
        name = f"M_BENCH_{idx:04d}"
        xml = generate_mapping_xml(name, "Medium", idx)
        path = output_dir / f"{name}.xml"
        path.write_text(xml, encoding="utf-8")
        stats["medium"] += 1
        stats["files"].append(str(path))
        idx += 1

    for i in range(complex_count):
        name = f"M_BENCH_{idx:04d}"
        xml = generate_mapping_xml(name, "Complex", idx)
        path = output_dir / f"{name}.xml"
        path.write_text(xml, encoding="utf-8")
        stats["complex"] += 1
        stats["files"].append(str(path))
        idx += 1

    stats["total"] = idx
    return stats


def main():
    parser = argparse.ArgumentParser(description="Synthetic Mapping Generator (Sprint 95)")
    parser.add_argument("--count", type=int, default=100, help="Number of mappings")
    parser.add_argument("--complexity-mix", type=str, default="0.4,0.35,0.25",
                        help="Comma-separated: simple%%,medium%%,complex%%")
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    mix = tuple(float(x) for x in args.complexity_mix.split(","))
    stats = generate_benchmark_mappings(args.count, mix, args.output_dir)

    print(f"\n  Synthetic Mapping Generator")
    print(f"  ──────────────────────────")
    print(f"  Generated: {stats['total']} mappings")
    print(f"    Simple:  {stats['simple']}")
    print(f"    Medium:  {stats['medium']}")
    print(f"    Complex: {stats['complex']}")
    print()


if __name__ == "__main__":
    main()
