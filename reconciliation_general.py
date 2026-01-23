"""
Generalized Reconciliation Engine
This script reconciles data between sheets based on a JSON configuration schema.
Supports 1 primary sheet (FAEU) and multiple secondary sheets (insurers).
"""

import pandas as pd
import re
import json
from typing import Optional, Dict, List, Any, Tuple


# =============================================================================
# NORMALIZATION FUNCTIONS REGISTRY
# =============================================================================

def normalize_policy(value: str) -> str:
    """
    Normalize policy number by removing brackets and everything after '-'
    Examples:
    - "OIGM202300135224 (EBP)" -> "OIGM202300135224"
    - "OIGM202400150383 - CAT B" -> "OIGM202400150383"
    """
    if pd.isna(value) or value == "":
        return ""
    
    policy_str = str(value).strip()
    
    # Remove everything after '-' (including the dash)
    if '-' in policy_str:
        policy_str = policy_str.split('-')[0].strip()
    
    # Remove brackets and their contents
    policy_str = re.sub(r'\([^)]*\)', '', policy_str).strip()
    
    return policy_str


def extract_document(value: str) -> Optional[str]:
    """
    Extract document number from invoice number by taking the second part after splitting by '/'
    If no '/' exists, return the invoice number itself.
    Examples:
    - "DNMD001224838/SHMIU25000012942" -> "SHMIU25000012942"
    - "SHMIU25000012942" -> "SHMIU25000012942"
    """
    if pd.isna(value) or value == "":
        return None
    
    invoice_str = str(value).strip()
    parts = invoice_str.split('/')
    
    if len(parts) >= 2:
        return parts[1].strip()
    else:
        return invoice_str


def normalize_document(value: str) -> str:
    """
    Normalize document number by taking only the part before the first '/'
    Example: "SHMOU21000023491/1" -> "SHMOU21000023491"
    """
    if pd.isna(value) or value == "":
        return ""
    
    doc_str = str(value).strip()
    
    if '/' in doc_str:
        doc_str = doc_str.split('/')[0].strip()
    
    return doc_str


# Registry mapping normalization function names to functions
NORMALIZATION_REGISTRY = {
    "normalize_policy": normalize_policy,
    "extract_document": extract_document,
    "normalize_document": normalize_document,
}


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_numeric(value) -> float:
    """Parse numeric value, handling commas, blanks, and nulls"""
    if pd.isna(value) or value == "" or value is None:
        return 0.0
    
    value_str = str(value).replace(',', '').replace(' ', '').strip()
    
    if value_str == "" or value_str == "-":
        return 0.0
    
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0


def is_empty_or_null(value) -> bool:
    """Check if value is empty, null, or missing"""
    if pd.isna(value) or value == "" or value is None:
        return True
    return False


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe by removing duplicate columns and unnamed columns"""
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    pattern = r'\.\d+$'
    cols_to_keep = [col for col in df.columns if not re.search(pattern, str(col))]
    return df[cols_to_keep]


# =============================================================================
# CONFIGURATION LOADER
# =============================================================================

def load_config(config_path: str) -> Dict:
    """Load and validate JSON configuration"""
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required sections
    required_sections = ['sheets', 'matching', 'remarks_rules']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required section: {section}")
    
    return config


def get_column_name(sheet_config: Dict, logical_name: str) -> str:
    """Get actual column name from logical name"""
    columns = sheet_config.get('columns', {})
    return columns.get(logical_name, logical_name)


def apply_normalization(value: Any, func_name: str) -> Any:
    """Apply a normalization function by name"""
    if func_name in NORMALIZATION_REGISTRY:
        return NORMALIZATION_REGISTRY[func_name](value)
    return value


# =============================================================================
# KEY GENERATION
# =============================================================================

def generate_composite_keys(row: pd.Series, sheet_config: Dict) -> List[str]:
    """
    Generate composite keys for a row based on sheet configuration.
    Returns a list of keys (one for each document column).
    """
    key_fields = sheet_config.get('key_fields', {})
    normalizations = sheet_config.get('normalizations', {})
    columns = sheet_config.get('columns', {})
    
    # Get policy column and normalize
    policy_logical = key_fields.get('policy_column', '')
    policy_actual = columns.get(policy_logical, policy_logical)
    policy_value = row.get(policy_actual, '')
    
    if policy_logical in normalizations:
        policy_value = apply_normalization(policy_value, normalizations[policy_logical])
    else:
        policy_value = str(policy_value).strip() if pd.notna(policy_value) else ""
    
    # Generate keys for each document column
    keys = []
    doc_columns = key_fields.get('document_columns', [])
    
    for doc_logical in doc_columns:
        doc_actual = columns.get(doc_logical, doc_logical)
        doc_value = row.get(doc_actual, '')
        
        if doc_logical in normalizations:
            doc_value = apply_normalization(doc_value, normalizations[doc_logical])
        else:
            doc_value = str(doc_value).strip() if pd.notna(doc_value) else ""
        
        if doc_value:
            composite_key = f"{policy_value}|{doc_value}"
            keys.append((composite_key, doc_actual))
    
    return keys


# =============================================================================
# MATCHING ENGINE
# =============================================================================

def perform_matching(
    primary_df: pd.DataFrame,
    secondary_dfs: Dict[str, pd.DataFrame],
    config: Dict
) -> pd.DataFrame:
    """
    Perform matching between primary sheet and all secondary sheets.
    Creates separate availability and matched_column columns for each secondary sheet.
    Sets internal _matched_key and _has_match columns for backward compatibility.
    """
    matching_config = config['matching']
    sheets_config = config['sheets']
    
    primary_name = matching_config['primary_sheet']
    primary_config = sheets_config[primary_name]
    
    # Get output column configuration (per-sheet format)
    output_cols = matching_config.get('output_columns', {})
    
    # Build separate key sets for each secondary sheet
    secondary_key_sets = {}
    for sec_name, sec_df in secondary_dfs.items():
        sec_config = sheets_config[sec_name]
        sec_keys = set()
        for idx, row in sec_df.iterrows():
            keys = generate_composite_keys(row, sec_config)
            for key, col in keys:
                sec_keys.add(key)
        secondary_key_sets[sec_name] = sec_keys
    
    # Match primary rows against each secondary sheet separately
    matched_keys_list = []
    has_match_list = []
    per_sheet_availability = {sec_name: [] for sec_name in secondary_dfs.keys()}
    per_sheet_trans_type = {sec_name: [] for sec_name in secondary_dfs.keys()}
    
    # Get transaction type column labels mapping from primary config
    trans_type_labels = primary_config.get('trans_type_column_labels', {})
    
    for idx, row in primary_df.iterrows():
        keys = generate_composite_keys(row, primary_config)
        matched_key = ""
        overall_match = False
        
        # Check against each secondary sheet
        for sec_name in secondary_dfs.keys():
            sec_matched = False
            sec_trans_type = ""
            for key, col_name in keys:
                if key in secondary_key_sets[sec_name]:
                    sec_matched = True
                    # Apply transaction type label if available, otherwise use actual column name
                    sec_trans_type = trans_type_labels.get(col_name, col_name)
                    if not matched_key:  # Save first match for backward compatibility
                        matched_key = key
                    break
            
            per_sheet_availability[sec_name].append("Yes" if sec_matched else "No")
            per_sheet_trans_type[sec_name].append(sec_trans_type)
            if sec_matched:
                overall_match = True
        
        matched_keys_list.append(matched_key)
        has_match_list.append(overall_match)
    
    # Set internal columns for backward compatibility
    primary_df['_matched_key'] = matched_keys_list
    primary_df['_has_match'] = has_match_list
    
    # Add per-sheet availability columns
    for sec_name, availability_values in per_sheet_availability.items():
        if sec_name in output_cols and 'availability' in output_cols[sec_name]:
            col_name = output_cols[sec_name]['availability']
            primary_df[col_name] = availability_values
    
    # Add per-sheet transaction type columns
    for sec_name, trans_type_values in per_sheet_trans_type.items():
        if sec_name in output_cols and 'trans_type' in output_cols[sec_name]:
            col_name = output_cols[sec_name]['trans_type']
            primary_df[col_name] = trans_type_values
    
    return primary_df


# =============================================================================
# CONDITION EVALUATOR
# =============================================================================

def get_value_from_operand(
    operand: Dict,
    current_row: pd.Series,
    current_sheet_name: str,
    sheets_data: Dict[str, pd.DataFrame],
    sheets_config: Dict,
    matched_row: Optional[pd.Series] = None
) -> Any:
    """
    Get value from an operand specification.
    Operand can be:
    - {"value": <literal>} - a literal value
    - {"sheet": "<name>", "column": "<logical or actual>"}  - column reference
    """
    if 'value' in operand:
        return operand['value']
    
    sheet_name = operand.get('sheet', current_sheet_name)
    column_ref = operand.get('column')
    
    if sheet_name == current_sheet_name:
        row = current_row
        config = sheets_config[sheet_name]
    else:
        # Cross-sheet reference - use matched row
        if matched_row is not None:
            row = matched_row
            config = sheets_config[sheet_name]
        else:
            return None
    
    # Get actual column name
    actual_col = get_column_name(config, column_ref)
    
    # Check if it's a generated column (like 'Available')
    if actual_col in row.index:
        return row.get(actual_col)
    elif column_ref in row.index:
        return row.get(column_ref)
    
    return None


def evaluate_condition(
    condition: Dict,
    current_row: pd.Series,
    current_sheet_name: str,
    sheets_data: Dict[str, pd.DataFrame],
    sheets_config: Dict,
    matched_row: Optional[pd.Series] = None
) -> bool:
    """Evaluate a single condition"""
    cond_type = condition.get('type')
    
    if cond_type == 'equals':
        left_val = get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        )
        right_val = get_value_from_operand(
            condition['right'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        )
        
        # Handle numeric comparison
        if isinstance(right_val, (int, float)):
            left_val = parse_numeric(left_val)
        
        return left_val == right_val
    
    elif cond_type == 'approx_equals':
        left_val = parse_numeric(get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        right_val = parse_numeric(get_value_from_operand(
            condition['right'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        tolerance = condition.get('tolerance', 0.01)
        return abs(left_val - right_val) < tolerance
    
    elif cond_type == 'less_than':
        left_val = parse_numeric(get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        right_val = parse_numeric(get_value_from_operand(
            condition['right'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        return left_val < right_val
    
    elif cond_type == 'greater_than':
        left_val = parse_numeric(get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        right_val = parse_numeric(get_value_from_operand(
            condition['right'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        ))
        return left_val > right_val
    
    elif cond_type == 'empty':
        left_val = get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        )
        return is_empty_or_null(left_val)
    
    elif cond_type == 'not_empty':
        left_val = get_value_from_operand(
            condition['left'], current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        )
        return not is_empty_or_null(left_val)
    
    elif cond_type == 'key_exists_in':
        # Check if current row's key exists in target sheet
        return matched_row is not None
    
    elif cond_type == 'has_match':
        # Check if current row has a match (via internal _has_match column)
        has_match = current_row.get('_has_match', False)
        return bool(has_match)
    
    elif cond_type == 'no_match':
        # Check if current row does NOT have a match
        has_match = current_row.get('_has_match', False)
        return not bool(has_match)
    
    elif cond_type == 'cross_sheet_or':
        # Evaluate nested conditions with OR logic
        sub_conditions = condition.get('conditions', [])
        for sub_cond in sub_conditions:
            if evaluate_condition(
                sub_cond, current_row, current_sheet_name,
                sheets_data, sheets_config, matched_row
            ):
                return True
        return False
    
    return False


def evaluate_conditions(
    conditions: List[Dict],
    condition_logic: str,
    current_row: pd.Series,
    current_sheet_name: str,
    sheets_data: Dict[str, pd.DataFrame],
    sheets_config: Dict,
    matched_row: Optional[pd.Series] = None
) -> bool:
    """Evaluate all conditions with AND/OR logic"""
    if not conditions:
        return True
    
    results = [
        evaluate_condition(
            cond, current_row, current_sheet_name,
            sheets_data, sheets_config, matched_row
        )
        for cond in conditions
    ]
    
    if condition_logic == "AND":
        return all(results)
    elif condition_logic == "OR":
        return any(results)
    
    return all(results)


# =============================================================================
# REMARKS ENGINE
# =============================================================================

def build_key_to_row_map(
    df: pd.DataFrame,
    sheet_config: Dict
) -> Dict[str, pd.Series]:
    """Build a mapping from composite keys to rows"""
    key_map = {}
    for idx, row in df.iterrows():
        keys = generate_composite_keys(row, sheet_config)
        for key, col in keys:
            if key not in key_map:
                key_map[key] = row
    return key_map


def apply_remarks_rules(
    df: pd.DataFrame,
    sheet_name: str,
    config: Dict,
    sheets_data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Apply remarks rules to a dataframe"""
    rules_config = config['remarks_rules'].get(sheet_name, {})
    sheets_config = config['sheets']
    
    if not rules_config:
        return df
    
    output_col = rules_config.get('output_column', 'Remarks')
    rules = rules_config.get('rules', [])
    default_remark = rules_config.get('default_remark', 'Not Reconciled')
    
    # Build key maps for cross-sheet lookups
    key_maps = {}
    for name, sdf in sheets_data.items():
        if name != sheet_name:
            key_maps[name] = build_key_to_row_map(sdf, sheets_config[name])
    
    # For primary sheet, also build its own key map for self-reference
    primary_key_map = build_key_to_row_map(df, sheets_config[sheet_name])
    
    remarks_list = []
    
    for idx, row in df.iterrows():
        remark = default_remark
        
        # Find matched row in other sheets if applicable
        matched_row = None
        current_keys = generate_composite_keys(row, sheets_config[sheet_name])
        
        for name, key_map in key_maps.items():
            for key, col in current_keys:
                if key in key_map:
                    matched_row = key_map[key]
                    break
            if matched_row is not None:
                break
        
        # If evaluating secondary sheet, look for match in primary
        if matched_row is None and '_matched_key' in row.index:
            matched_key = row.get('_matched_key', '')
            if matched_key:
                for name, key_map in key_maps.items():
                    if matched_key in key_map:
                        matched_row = key_map[matched_key]
                        break
        
        # Evaluate rules in order - first match wins
        for rule in rules:
            conditions = rule.get('conditions', [])
            condition_logic = rule.get('condition_logic', 'AND')
            
            if evaluate_conditions(
                conditions, condition_logic, row, sheet_name,
                sheets_data, sheets_config, matched_row
            ):
                remark = rule['remark']
                break
        
        remarks_list.append(remark)
    
    df[output_col] = remarks_list
    
    return df


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def reconcile(
    config_path: str,
    faeu_file: str,
    insurer_files: Dict[str, str],
    output_path: str
):
    """
    Main reconciliation function.
    
    Args:
        config_path: Path to JSON configuration file
        faeu_file: Path to primary (FAEU) Excel file
        insurer_files: Dict mapping sheet name to file path for secondary sheets
        output_path: Path for output Excel file
    """
    print(f"Loading configuration from: {config_path}")
    config = load_config(config_path)
    
    matching_config = config['matching']
    sheets_config = config['sheets']
    
    primary_name = matching_config['primary_sheet']
    secondary_names = matching_config['secondary_sheets']
    
    # Load primary sheet
    print(f"Loading primary sheet ({primary_name}) from: {faeu_file}")
    primary_df = pd.read_excel(faeu_file, header=0)
    primary_df = clean_dataframe(primary_df)
    print(f"  Loaded {len(primary_df)} rows")
    
    # Load secondary sheets
    secondary_dfs = {}
    for sec_name in secondary_names:
        file_path = None
        
        # Try explicit mapping first (backward compatibility)
        if insurer_files and sec_name in insurer_files:
            file_path = insurer_files[sec_name]
        else:
            # Auto-discover: look for {sheet_name}.xlsx
            auto_path = f"{sec_name}.xlsx"
            import os
            if os.path.exists(auto_path):
                file_path = auto_path
        
        if file_path:
            print(f"Loading secondary sheet ({sec_name}) from: {file_path}")
            try:
                sec_df = pd.read_excel(file_path, header=0)
                sec_df = clean_dataframe(sec_df)
                secondary_dfs[sec_name] = sec_df
                print(f"  Loaded {len(sec_df)} rows")
            except Exception as e:
                print(f"  ERROR loading {file_path}: {e}")
                raise
        else:
            print(f"  WARNING: No file found for secondary sheet '{sec_name}'")
            print(f"    Looked for: {sec_name}.xlsx")
            print(f"    Skipping this sheet...")
    
    # Combine all sheets data for cross-sheet lookups
    sheets_data = {primary_name: primary_df}
    sheets_data.update(secondary_dfs)
    
    print("\nStep 1: Performing matching...")
    primary_df = perform_matching(primary_df, secondary_dfs, config)
    sheets_data[primary_name] = primary_df
    
    print("Step 2: Applying remarks rules to secondary sheets...")
    for sec_name, sec_df in secondary_dfs.items():
        secondary_dfs[sec_name] = apply_remarks_rules(
            sec_df, sec_name, config, sheets_data
        )
    
    print("Step 3: Applying remarks rules to primary sheet...")
    primary_df = apply_remarks_rules(primary_df, primary_name, config, sheets_data)
    
    # Remove internal columns
    internal_cols = ['_matched_key', '_has_match']
    for col in internal_cols:
        if col in primary_df.columns:
            primary_df = primary_df.drop(columns=[col])
    
    # Final cleanup
    primary_df = clean_dataframe(primary_df)
    for sec_name in secondary_dfs:
        secondary_dfs[sec_name] = clean_dataframe(secondary_dfs[sec_name])
    
    print(f"\nSaving results to: {output_path}")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write primary sheet with proper name
        primary_df.to_excel(writer, sheet_name='FAEU', index=False)
        
        # Write secondary sheets
        for sec_name, sec_df in secondary_dfs.items():
            sec_df.to_excel(writer, sheet_name=sec_name.capitalize(), index=False)
    
    # Print summary
    print("\n=== Reconciliation Summary ===")
    print(f"\n{primary_name.upper()} Sheet:")
    print(f"  Total rows: {len(primary_df)}")
    
    
    # Print per-sheet availability columns
    output_cols = config['matching'].get('output_columns', {})
    for sec_name in secondary_dfs.keys():
        if sec_name in output_cols and 'availability' in output_cols[sec_name]:
            col_name = output_cols[sec_name]['availability']
            if col_name in primary_df.columns:
                print(f"  {col_name}: {primary_df[col_name].value_counts().to_dict()}")
    
    remarks_col = config['remarks_rules'].get(primary_name, {}).get('output_column', 'Remarks')
    if remarks_col in primary_df.columns:
        print(f"  {remarks_col}: {primary_df[remarks_col].value_counts().to_dict()}")
        
        # Add transaction type breakdown for each remark
        for sec_name in secondary_dfs.keys():
            if sec_name in output_cols and 'trans_type' in output_cols[sec_name]:
                trans_type_col = output_cols[sec_name]['trans_type']
                if trans_type_col in primary_df.columns:
                    print(f"\n  Breakdown by {sec_name.capitalize()} Transaction Type:")
                    # Group by remarks and trans_type
                    grouped = primary_df.groupby([remarks_col, trans_type_col]).size()
                    for (remark, trans_type), count in grouped.items():
                        if trans_type:  # Only show if trans_type is not empty
                            print(f"    {remark} via {trans_type}: {count}")
    
    for sec_name, sec_df in secondary_dfs.items():
        print(f"\n{sec_name.upper()} Sheet:")
        print(f"  Total rows: {len(sec_df)}")
        remarks_col = config['remarks_rules'].get(sec_name, {}).get('output_column', 'Remarks')
        if remarks_col in sec_df.columns:
            print(f"  {remarks_col}: {sec_df[remarks_col].value_counts().to_dict()}")
    
    print("\nReconciliation complete!")


if __name__ == "__main__":
    import sys
    
    # Default configuration
    config_file = "reconcil.json"
    faeu_file = "faeu.xlsx"
    insurer_files = {"insurer": "sukoon.xlsx"}
    output_file = "reconciliation_output_general.xlsx"
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    reconcile(config_file, faeu_file, insurer_files, output_file)
