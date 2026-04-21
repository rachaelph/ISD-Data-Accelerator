"""
Generate a summary report of metadata SQL files.

This script parses metadata SQL files and generates a human-readable summary
showing all tables, their configurations, transformations, and data quality checks.

Usage:
    python scripts/summarize_metadata.py [path_to_sql_file]
    python scripts/summarize_metadata.py  # summarizes all metadata notebooks in src/metadata/

Author: Generated for Databricks Data Platform Accelerator
"""

import re
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TableMetadata:
    """Holds metadata for a single table."""
    table_id: int
    trigger_name: str = ""
    order: int = 0
    target_datastore: str = ""
    target_entity: str = ""
    primary_keys: str = ""
    processing_method: str = ""
    active: bool = True
    
    # Primary config
    source_table: str = ""
    merge_type: str = ""
    
    # Transformations (list of dicts)
    transformations: list = field(default_factory=list)
    
    # Data quality checks (list of dicts)
    quality_checks: list = field(default_factory=list)


def find_insert_sections(content: str) -> dict:
    """Find the three INSERT INTO sections in the metadata SQL file."""
    sections = {}
    # Handle both formats: INSERT INTO dbo.Table and INSERT INTO [dbo].[Table]
    pattern = r'INSERT INTO \[?dbo\]?\.\[?(Data_Pipeline_\w+)\]?'
    
    lines = content.split('\n')
    current_section = None
    section_start = 0
    
    for i, line in enumerate(lines, 1):
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            if current_section:
                sections[current_section] = {
                    'start': section_start,
                    'end': i - 1,
                    'lines': lines[section_start-1:i-1]
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
            'lines': lines[section_start-1:]
        }
    
    return sections


def parse_orchestration(lines: list) -> dict:
    """Parse orchestration rows. Returns dict keyed by table_id."""
    tables = {}
    # Handle both 'value' and NULL for Primary_Keys field, and SQL-escaped quotes ('')
    pattern = r"\(\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*(?:'((?:[^']|'')*)'|NULL)\s*,\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*\)"
    
    for line in lines:
        match = re.search(pattern, line)
        if match:
            table_id = int(match.group(3))
            tables[table_id] = TableMetadata(
                table_id=table_id,
                trigger_name=match.group(1),
                order=int(match.group(2)),
                target_datastore=match.group(4),
                target_entity=match.group(5),
                primary_keys=match.group(6),
                processing_method=match.group(7),
                active=match.group(8) == '1'
            )
    
    return tables


def parse_primary_config(lines: list, tables: dict) -> dict:
    """Parse primary config and add to tables dict."""
    # Handle SQL-escaped quotes ('') in values
    pattern = r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'"
    
    for line in lines:
        for match in re.finditer(pattern, line):
            table_id = int(match.group(1))
            category = match.group(2)
            name = match.group(3)
            value = match.group(4)
            
            if table_id not in tables:
                tables[table_id] = TableMetadata(table_id=table_id)
            
            if category == 'source_details' and name == 'table_name':
                tables[table_id].source_table = value
            elif category == 'target_details' and name == 'merge_type':
                tables[table_id].merge_type = value
    
    return tables


def parse_advanced_config(lines: list, tables: dict) -> dict:
    """Parse advanced config and add transformations/quality checks to tables."""
    # Handle SQL-escaped quotes ('') in values
    pattern = r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)"
    
    # Group by table_id, category, name, instance
    grouped = defaultdict(lambda: defaultdict(dict))
    
    for line in lines:
        for match in re.finditer(pattern, line):
            table_id = int(match.group(1))
            category = match.group(2)
            name = match.group(3)
            instance = int(match.group(4))
            attribute = match.group(5)
            value = match.group(6)
            
            key = (table_id, category, name, instance)
            grouped[table_id][(category, name, instance)][attribute] = value
    
    # Convert to table metadata
    for table_id, instances in grouped.items():
        if table_id not in tables:
            tables[table_id] = TableMetadata(table_id=table_id)
        
        for (category, name, instance), attrs in instances.items():
            if category == 'data_transformation_steps':
                tables[table_id].transformations.append({
                    'type': name,
                    'instance': instance,
                    'attributes': attrs
                })
            elif category == 'data_quality':
                tables[table_id].quality_checks.append({
                    'type': name,
                    'instance': instance,
                    'attributes': attrs
                })
    
    # Sort transformations by instance
    for table_id in tables:
        tables[table_id].transformations.sort(key=lambda x: x['instance'])
        tables[table_id].quality_checks.sort(key=lambda x: x['instance'])
    
    return tables


def format_transformation(t: dict) -> str:
    """Format a transformation for display."""
    ttype = t['type']
    attrs = t['attributes']
    
    if ttype == 'columns_to_rename':
        old = attrs.get('existing_column_name', '?')
        new = attrs.get('new_column_name', '?')
        return f"RENAME: {old} → {new}"
    
    elif ttype == 'derived_column':
        col = attrs.get('column_name', '?')
        expr = attrs.get('expression', '?')
        if len(expr) > 50:
            expr = expr[:47] + '...'
        return f"DERIVE: {col} = {expr}"
    
    elif ttype == 'filter_data':
        logic = attrs.get('filter_logic', '?')
        if len(logic) > 60:
            logic = logic[:57] + '...'
        return f"FILTER: {logic}"
    
    elif ttype == 'join_data':
        table = attrs.get('right_table_name', '?')
        jtype = attrs.get('join_type', 'inner')
        return f"JOIN ({jtype}): {table}"
    
    elif ttype == 'aggregate_data':
        cols = attrs.get('column_name', '?')
        aggs = attrs.get('aggregation', '?')
        group = attrs.get('group_by_columns', '')
        return f"AGGREGATE: {aggs}({cols})" + (f" GROUP BY {group}" if group else "")
    
    elif ttype == 'select_columns':
        cols = attrs.get('column_name', '?')
        if len(cols) > 60:
            cols = cols[:57] + '...'
        return f"SELECT: {cols}"
    
    elif ttype == 'custom_transformation_function':
        func = attrs.get('functions_to_execute', '?')
        return f"CUSTOM: {func}"
    
    elif ttype == 'remove_columns':
        cols = attrs.get('column_name', '?')
        return f"REMOVE: {cols}"
    
    elif ttype == 'drop_duplicates':
        cols = attrs.get('column_name', '?')
        return f"DEDUPE: {cols}"
    
    elif ttype == 'change_data_types':
        cols = attrs.get('column_name', '?')
        dtype = attrs.get('new_type', '?')
        return f"CAST: {cols} to {dtype}"
    
    elif ttype == 'apply_null_handling':
        cols = attrs.get('column_name', '?')
        action = attrs.get('action', '?')
        return f"NULL_HANDLING ({action}): {cols}"
    
    elif ttype == 'add_window_function':
        func = attrs.get('window_function', '?')
        out = attrs.get('output_column_name', '?')
        return f"WINDOW: {func} → {out}"
    
    elif ttype == 'create_surrogate_key':
        col = attrs.get('column_name', '?')
        return f"SURROGATE_KEY: {col}"
    
    elif ttype == 'attach_dimension_surrogate_key':
        dim = attrs.get('dimension_table_name', '?')
        out = attrs.get('dimension_key_output_column_name', '?')
        return f"DIM_LOOKUP: {dim} → {out}"
    
    else:
        return f"{ttype.upper()}: {list(attrs.values())[:2]}"


def format_quality_check(q: dict) -> str:
    """Format a quality check for display."""
    qtype = q['type']
    attrs = q['attributes']
    action = attrs.get('if_not_compliant', 'warn')
    
    if qtype == 'validate_condition':
        cond = attrs.get('condition', '?')
        if len(cond) > 50:
            cond = cond[:47] + '...'
        return f"[{action}] CONDITION: {cond}"
    
    elif qtype == 'validate_not_null':
        cols = attrs.get('column_name', '?')
        return f"[{action}] NOT_NULL: {cols}"
    
    elif qtype == 'validate_referential_integrity':
        col = attrs.get('current_table_column_name', '?')
        ref = attrs.get('reference_table_name', '?')
        return f"[{action}] REF_INTEGRITY: {col} → {ref}"
    
    elif qtype == 'validate_pattern':
        col = attrs.get('column_name', '?')
        pattern = attrs.get('pattern_type', '?')
        return f"[{action}] PATTERN ({pattern}): {col}"
    
    elif qtype == 'validate_range':
        col = attrs.get('column_name', '?')
        minv = attrs.get('min_value', '')
        maxv = attrs.get('max_value', '')
        return f"[{action}] RANGE: {col} ({minv} to {maxv})"
    
    else:
        return f"[{action}] {qtype.upper()}"


def generate_summary(file_path: Path) -> str:
    """Generate a summary report for a metadata SQL file."""
    content = file_path.read_text(encoding='utf-8')
    sections = find_insert_sections(content)
    
    tables = {}
    
    if 'orchestration' in sections:
        tables = parse_orchestration(sections['orchestration']['lines'])
    
    if 'primary_config' in sections:
        tables = parse_primary_config(sections['primary_config']['lines'], tables)
    
    if 'advanced_config' in sections:
        tables = parse_advanced_config(sections['advanced_config']['lines'], tables)
    
    # Build output
    output = []
    output.append("=" * 80)
    output.append(f"METADATA SUMMARY: {file_path.name}")
    output.append("=" * 80)
    output.append("")
    
    # Group by trigger name and order
    by_trigger = defaultdict(list)
    for table_id, meta in sorted(tables.items()):
        by_trigger[meta.trigger_name].append(meta)
    
    # Statistics
    total_tables = len(tables)
    total_transforms = sum(len(t.transformations) for t in tables.values())
    total_quality = sum(len(t.quality_checks) for t in tables.values())
    active_tables = sum(1 for t in tables.values() if t.active)
    
    output.append("📊 STATISTICS")
    output.append("-" * 40)
    output.append(f"  Total Tables: {total_tables} ({active_tables} active)")
    output.append(f"  Total Transformations: {total_transforms}")
    output.append(f"  Total Quality Checks: {total_quality}")
    output.append("")
    
    # Detail by trigger
    for trigger_name, trigger_tables in sorted(by_trigger.items()):
        if not trigger_name:
            trigger_name = "(No Trigger)"
        
        output.append("=" * 80)
        output.append(f"📁 TRIGGER: {trigger_name}")
        output.append("=" * 80)
        
        # Sort by order
        for meta in sorted(trigger_tables, key=lambda x: (x.order, x.table_id)):
            status = "✅" if meta.active else "⏸️"
            output.append("")
            output.append(f"{status} [{meta.table_id}] {meta.target_datastore}.{meta.target_entity}")
            output.append(f"   Step: {meta.order} | Method: {meta.processing_method} | Merge: {meta.merge_type or 'N/A'}")
            
            if meta.source_table:
                output.append(f"   Source: {meta.source_table}")
            
            if meta.primary_keys:
                output.append(f"   PK: {meta.primary_keys}")
            
            # Transformations
            if meta.transformations:
                output.append(f"   📝 Transformations ({len(meta.transformations)}):")
                for t in meta.transformations:
                    output.append(f"      {t['instance']:2d}. {format_transformation(t)}")
            
            # Quality checks
            if meta.quality_checks:
                output.append(f"   🔍 Quality Checks ({len(meta.quality_checks)}):")
                for q in meta.quality_checks:
                    output.append(f"      {q['instance']:2d}. {format_quality_check(q)}")
    
    output.append("")
    output.append("=" * 80)
    
    return "\n".join(output)


def main():
    """Main entry point."""
    import io
    import sys
    
    # Force UTF-8 output
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    
    if len(sys.argv) > 1:
        file_path = Path(sys.argv[1])
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            sys.exit(1)
        files = [file_path]
    else:
        # Look for metadata notebooks
        metadata_dir = repo_root / "src" / "metadata"
        files = list(metadata_dir.glob("*.Notebook/notebook-content.sql"))
        
        if not files:
            print(f"No metadata notebooks found in {metadata_dir}")
            print(f"Expected: {metadata_dir}/metadata_<TriggerName>.Notebook/notebook-content.sql")
            sys.exit(1)
    
    for file_path in files:
        print(generate_summary(file_path))
        print()


if __name__ == "__main__":
    main()
