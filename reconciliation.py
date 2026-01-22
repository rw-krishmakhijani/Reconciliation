"""
Reconciliation Script for FAEU and Insurer Sheets
This script reconciles data between FAEU (Praktora) and Insurer sheets.
"""

import pandas as pd
import re
from typing import Optional, Tuple



def normalize_policy_number(policy_no: str) -> str:
    """
    Normalize policy number by removing brackets and everything after '-'
    Examples:
    - "OIGM202300135224 (EBP)" -> "OIGM202300135224"
    - "OIGM202400150383 - CAT B" -> "OIGM202400150383"
    - "OIGM202400157006" -> "OIGM202400157006"
    """
    if pd.isna(policy_no) or policy_no == "":
        return ""
    
    policy_str = str(policy_no).strip()
    
    # Remove everything after '-' (including the dash)
    if '-' in policy_str:
        policy_str = policy_str.split('-')[0].strip()
    
    # Remove brackets and their contents
    policy_str = re.sub(r'\([^)]*\)', '', policy_str).strip()
    
    return policy_str


def extract_document_number(invoice_no: str) -> Optional[str]:
    """
    Extract document number from invoice number by taking the second part after splitting by '/'
    If no '/' exists, return the invoice number itself.
    Examples:
    - "DNMD001224838/SHMIU25000012942" -> "SHMIU25000012942"
    - "SHMIU25000012942" -> "SHMIU25000012942"
    - "ABC/12345/XYZ" -> "12345"
    """
    if pd.isna(invoice_no) or invoice_no == "":
        return None
    
    invoice_str = str(invoice_no).strip()
    parts = invoice_str.split('/')
    
    if len(parts) >= 2:
        # If there's a '/', return the second part
        return parts[1].strip()
    else:
        # If there's no '/', return the original value
        return invoice_str


def normalize_insurer_document_number(doc_no: str) -> str:
    """
    Normalize Insurer document number by taking only the part before the first '/'
    Example: "SHMOU21000023491/1" -> "SHMOU21000023491"
    Example: "SHMOU21000023491" -> "SHMOU21000023491"
    """
    if pd.isna(doc_no) or doc_no == "":
        return ""
    
    doc_str = str(doc_no).strip()
    
    # Take only the part before the first '/'
    if '/' in doc_str:
        doc_str = doc_str.split('/')[0].strip()
    
    return doc_str


def create_composite_key(policy_no: str, document_no: str) -> str:
    """
    Create a composite key from policy number and document number
    """
    policy = normalize_policy_number(policy_no) if pd.notna(policy_no) else ""
    doc = str(document_no).strip() if pd.notna(document_no) else ""
    return f"{policy}|{doc}"


def parse_numeric(value) -> float:
    """
    Parse numeric value, handling commas, blanks, and nulls
    """
    if pd.isna(value) or value == "" or value is None:
        return 0.0
    
    # Convert to string, remove commas and spaces
    value_str = str(value).replace(',', '').replace(' ', '').strip()
    
    if value_str == "" or value_str == "-":
        return 0.0
    
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0


def is_empty_or_null(value) -> bool:
    """
    Check if value is empty, null, or missing
    """
    if pd.isna(value) or value == "" or value is None:
        return True
    return False


def reconcile_faeu_with_insurer(faeu_df: pd.DataFrame, insurer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconcile FAEU sheet with Insurer sheet
    Creates 'Available' column in FAEU sheet
    """
    # Create composite keys for Insurer sheet
    insurer_df['composite_key'] = insurer_df.apply(
        lambda row: create_composite_key(
            row.get('Policy Number', ''),
            normalize_insurer_document_number(row.get('Doc Number', ''))
        ),
        axis=1
    )
    
    # Create set of Insurer composite keys for fast lookup
    insurer_keys = set(insurer_df['composite_key'].values)
    
    # Create Available column in FAEU
    available_list = []
    
    for idx, row in faeu_df.iterrows():
        policy_no = row.get('Policy No', '')
        
        # Try Ins Tax Invoice first
        insta_invoice = row.get('Ins Tax Invoice', '')
        doc_no_insta = extract_document_number(insta_invoice)
        
        matched = False
        
        if doc_no_insta:
            faeu_key_insta = create_composite_key(policy_no, doc_no_insta)
            if faeu_key_insta in insurer_keys:
                matched = True
                available_list.append("Yes")
            else:
                # Try Tax.Invoice No(Broker)
                brkt_invoice = row.get('Tax.Invoice No(Broker)', '')
                doc_no_brkt = extract_document_number(brkt_invoice)
                
                if doc_no_brkt:
                    faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
                    if faeu_key_brkt in insurer_keys:
                        matched = True
                        available_list.append("Yes")
                    else:
                        available_list.append("No")
                else:
                    available_list.append("No")
        else:
            # Try Tax.Invoice No(Broker) if Ins Tax Invoice doesn't have document number
            brkt_invoice = row.get('Tax.Invoice No(Broker)', '')
            doc_no_brkt = extract_document_number(brkt_invoice)
            
            if doc_no_brkt:
                faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
                if faeu_key_brkt in insurer_keys:
                    matched = True
                    available_list.append("Yes")
                else:
                    available_list.append("No")
            else:
                available_list.append("No")
    
    faeu_df['Available'] = available_list
    
    return faeu_df


def add_insurer_remarks(faeu_df: pd.DataFrame, insurer_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Remarks column to Insurer sheet
    Logic:
    - Reconciled: CompositeKey matches FAEU AND Insurer AND (TransAmt Local == Total Premium(AED) OR TransAmt Local == Total Commission(AED))
    - Not Reconciled: Otherwise
    """
    # Create composite keys for both sheets
    insurer_df['composite_key'] = insurer_df.apply(
        lambda row: create_composite_key(
            row.get('Policy Number', ''),
            normalize_insurer_document_number(row.get('Doc Number', ''))
        ),
        axis=1
    )
    
    # Create a mapping from FAEU composite keys to FAEU rows
    faeu_key_to_row = {}
    
    for idx, row in faeu_df.iterrows():
        policy_no = row.get('Policy No', '')
        
        # Try Ins Tax Invoice
        insta_invoice = row.get('Ins Tax Invoice', '')
        doc_no_insta = extract_document_number(insta_invoice)
        
        if doc_no_insta:
            faeu_key_insta = create_composite_key(policy_no, doc_no_insta)
            if faeu_key_insta not in faeu_key_to_row:
                faeu_key_to_row[faeu_key_insta] = row
        
        # Try Tax.Invoice No(Broker)
        brkt_invoice = row.get('Tax.Invoice No(Broker)', '')
        doc_no_brkt = extract_document_number(brkt_invoice)
        
        if doc_no_brkt:
            faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
            if faeu_key_brkt not in faeu_key_to_row:
                faeu_key_to_row[faeu_key_brkt] = row
    
    remarks_list = []
    
    for idx, row in insurer_df.iterrows():
        insurer_key = row.get('composite_key', '')
        insurer_trans_amt = parse_numeric(row.get('Trans Amt Local', 0))
        
        # Check if key exists in FAEU and is available
        if insurer_key in faeu_key_to_row:
            faeu_row = faeu_key_to_row[insurer_key]
            
            # Check if Available is "Yes" in FAEU
            faeu_available = faeu_row.get('Available', 'No')
            
            if faeu_available == "Yes":
                # Get FAEU amounts
                total_premium = parse_numeric(faeu_row.get('Total Premium(AED)', 0))
                inst_commission = parse_numeric(faeu_row.get('Total Commission(AED)', 0))
                
                # Check if amounts match
                if (abs(insurer_trans_amt - total_premium) < 0.01) or (abs(insurer_trans_amt - inst_commission) < 0.01):
                    remarks_list.append("Reconciled")
                else:
                    remarks_list.append("Not Reconciled")
            else:
                remarks_list.append("Not Reconciled")
        else:
            remarks_list.append("Not Reconciled")
    
    insurer_df['Remarks'] = remarks_list
    
    return insurer_df


def add_faeu_remarks(faeu_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Remarks column to FAEU sheet
    Logic:
    - Reconciled: Customer Settled (AED) == Total Premium(AED) AND Commission Colleted (AED) == Total Commission(AED)
    - DS_PENDING: (Customer Settled (AED) == 0 OR empty) AND Commission Colleted (AED) == Total Commission(AED)
    - Premium and Commission outstanding: Customer Settled (AED) == 0 AND Commission Colleted (AED) == 0
    - Prem Missing: Total Premium(AED) is missing/null/empty
    - Reconciled and Comm Receivable: Customer Settled (AED) == Total Premium(AED) AND Commission Colleted (AED) == 0 (or < Total Commission(AED))
    """
    remarks_list = []
    
    for idx, row in faeu_df.iterrows():
        # Check if Available is "Yes"
        available = row.get('Available', 'No')
        
        if available != "Yes":
            remarks_list.append("Not Available in Insurer")
            continue
        
        # Get all relevant amounts
        cs_settlement = parse_numeric(row.get('Customer Settled (AED)', 0))
        tax_settlement = parse_numeric(row.get('Commission Colleted (AED)', 0))
        total_premium = row.get('Total Premium(AED)', None)
        inst_commission = parse_numeric(row.get('Total Commission(AED)', 0))
        
        # Check if premium is missing
        if is_empty_or_null(total_premium):
            remarks_list.append("Prem Missing")
            continue
        
        total_premium_num = parse_numeric(total_premium)
        
        # Check for Premium and Commission outstanding
        if cs_settlement == 0 and tax_settlement == 0:
            remarks_list.append("Premium and Commission outstanding")
            continue
        
        # Check for DS_PENDING
        if (cs_settlement == 0 or is_empty_or_null(row.get('Customer Settled (AED)', None))) and abs(tax_settlement - inst_commission) < 0.01:
            remarks_list.append("DS_PENDING")
            continue
        
        # Check for Reconciled and Comm Receivable
        if abs(cs_settlement - total_premium_num) < 0.01 and (tax_settlement == 0 or tax_settlement < inst_commission):
            remarks_list.append("Reconciled and Comm Receivable")
            continue
        
        # Check for Reconciled
        if abs(cs_settlement - total_premium_num) < 0.01 and abs(tax_settlement - inst_commission) < 0.01:
            remarks_list.append("Reconciled")
            continue
        
        # Default case
        remarks_list.append("Not Reconciled")
    
    faeu_df['Remarks'] = remarks_list
    
    return faeu_df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean dataframe by removing duplicate columns and unnamed columns
    """
    # Remove columns that start with 'Unnamed' (pandas default for missing headers)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    
    # Remove duplicate columns (pandas adds .1, .2 suffixes for duplicates)
    # Pattern: column name ending with . followed by one or more digits
    import re
    pattern = r'\.\d+$'  # Matches .1, .2, .10, etc. at the end of column name
    
    cols_to_keep = [col for col in df.columns if not re.search(pattern, str(col))]
    
    # Return dataframe with only the columns we want to keep
    return df[cols_to_keep]


def main(faeu_file: str, insurer_file: str, output_file: str):
    """
    Main reconciliation function
    Reads two separate Excel files: faeu.xlsx and insurer.xlsx
    """
    print(f"Reading FAEU file: {faeu_file}")
    
    # Read FAEU file (header starts from row 0)
    try:
        faeu_df = pd.read_excel(faeu_file, header=0)
        print(f"FAEU file loaded: {len(faeu_df)} rows")
    except Exception as e:
        print(f"Error reading FAEU file: {e}")
        raise
    
    # Clean FAEU dataframe
    faeu_df = clean_dataframe(faeu_df)
    
    print(f"\nReading Insurer file: {insurer_file}")
    
    # Read Insurer file (header starts from row 0)
    try:
        insurer_df = pd.read_excel(insurer_file, header=0)
        print(f"Insurer file loaded: {len(insurer_df)} rows")
    except Exception as e:
        print(f"Error reading Insurer file: {e}")
        raise
    
    # Clean Insurer dataframe
    insurer_df = clean_dataframe(insurer_df)
    
    print("\nStarting reconciliation...")
    
    # Step 1: Reconcile FAEU with Insurer (create Available column)
    print("Step 1: Creating 'Available' column in FAEU sheet...")
    faeu_df = reconcile_faeu_with_insurer(faeu_df, insurer_df.copy())
    
    # Step 2: Add Remarks to Insurer sheet
    print("Step 2: Adding 'Remarks' column to Insurer sheet...")
    insurer_df = add_insurer_remarks(faeu_df.copy(), insurer_df)
    
    # Step 3: Add Remarks to FAEU sheet
    print("Step 3: Adding 'Remarks' column to FAEU sheet...")
    faeu_df = add_faeu_remarks(faeu_df)
    
    # Remove temporary composite_key column if it exists
    if 'composite_key' in faeu_df.columns:
        faeu_df = faeu_df.drop(columns=['composite_key'])
    if 'composite_key' in insurer_df.columns:
        insurer_df = insurer_df.drop(columns=['composite_key'])
    
    # Final cleanup before saving
    faeu_df = clean_dataframe(faeu_df)
    insurer_df = clean_dataframe(insurer_df)
    
    print("\nSaving results...")
    
    # Save to output file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        faeu_df.to_excel(writer, sheet_name='FAEU', index=False)
        insurer_df.to_excel(writer, sheet_name='Insurer', index=False)
    
    print(f"Reconciliation complete! Results saved to: {output_file}")
    
    # Print summary
    print("\n=== Reconciliation Summary ===")
    print(f"\nFAEU Sheet:")
    print(f"  Total rows: {len(faeu_df)}")
    if 'Available' in faeu_df.columns:
        available_counts = faeu_df['Available'].value_counts()
        print(f"  Available: {available_counts.to_dict()}")
    if 'Remarks' in faeu_df.columns:
        remarks_counts = faeu_df['Remarks'].value_counts()
        print(f"  Remarks: {remarks_counts.to_dict()}")
    
    print(f"\nInsurer Sheet:")
    print(f"  Total rows: {len(insurer_df)}")
    if 'Remarks' in insurer_df.columns:
        remarks_counts = insurer_df['Remarks'].value_counts()
        print(f"  Remarks: {remarks_counts.to_dict()}")


if __name__ == "__main__":
    import sys
    
    # Default file names
    faeu_file = "faeu.xlsx"
    insurer_file = "insurer.xlsx"
    output_file = "reconciliation_output.xlsx"
    
    # Allow command line arguments
    if len(sys.argv) > 1:
        faeu_file = sys.argv[1]
    if len(sys.argv) > 2:
        insurer_file = sys.argv[2]
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    
    main(faeu_file, insurer_file, output_file)
