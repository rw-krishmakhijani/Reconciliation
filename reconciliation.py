"""
Reconciliation Script for FAEU and Sukoon Sheets
This script reconciles data between FAEU (Praktora) and Sukoon (Insurer) sheets.
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
    Example: "ABC/12345/XYZ" -> "12345"
    """
    if pd.isna(invoice_no) or invoice_no == "":
        return None
    
    parts = str(invoice_no).split('/')
    if len(parts) >= 2:
        return parts[1].strip()
    return None


def normalize_sukoon_document_number(doc_no: str) -> str:
    """
    Normalize Sukoon document number by taking only the part before the first '/'
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


def reconcile_faeu_with_sukoon(faeu_df: pd.DataFrame, sukoon_df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconcile FAEU sheet with Sukoon sheet
    Creates 'Available' column in FAEU sheet
    """
    # Create composite keys for Sukoon sheet
    sukoon_df['composite_key'] = sukoon_df.apply(
        lambda row: create_composite_key(
            row.get('Policy Number', ''),
            normalize_sukoon_document_number(row.get('Doc Number', ''))
        ),
        axis=1
    )
    
    # Create set of Sukoon composite keys for fast lookup
    sukoon_keys = set(sukoon_df['composite_key'].values)
    
    # Create Available column in FAEU
    available_list = []
    
    for idx, row in faeu_df.iterrows():
        policy_no = row.get('POLICYNO', '')
        
        # Try INSTAXINVOICE first (note: column name is INSTAXINVOICE, not INSTAXINVOICENO)
        insta_invoice = row.get('INSTAXINVOICE', '')
        doc_no_insta = extract_document_number(insta_invoice)
        
        matched = False
        
        if doc_no_insta:
            faeu_key_insta = create_composite_key(policy_no, doc_no_insta)
            if faeu_key_insta in sukoon_keys:
                matched = True
                available_list.append("Yes")
            else:
                # Try BRKTAXINVOICENO
                brkt_invoice = row.get('BRKTAXINVOICENO', '')
                doc_no_brkt = extract_document_number(brkt_invoice)
                
                if doc_no_brkt:
                    faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
                    if faeu_key_brkt in sukoon_keys:
                        matched = True
                        available_list.append("Yes")
                    else:
                        available_list.append("No")
                else:
                    available_list.append("No")
        else:
            # Try BRKTAXINVOICENO if INSTAXINVOICENO doesn't have document number
            brkt_invoice = row.get('BRKTAXINVOICENO', '')
            doc_no_brkt = extract_document_number(brkt_invoice)
            
            if doc_no_brkt:
                faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
                if faeu_key_brkt in sukoon_keys:
                    matched = True
                    available_list.append("Yes")
                else:
                    available_list.append("No")
            else:
                available_list.append("No")
    
    faeu_df['Available'] = available_list
    
    return faeu_df


def add_sukoon_remarks(faeu_df: pd.DataFrame, sukoon_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Remarks column to Sukoon sheet
    Logic:
    - Reconciled: CompositeKey matches FAEU AND Sukoon AND (TransAmt Local == TOTPREMIUMINCLUDINGVAT OR TransAmt Local == INSTOTALCOMMAMOUNT)
    - Not Reconciled: Otherwise
    """
    # Create composite keys for both sheets
    sukoon_df['composite_key'] = sukoon_df.apply(
        lambda row: create_composite_key(
            row.get('Policy Number', ''),
            normalize_sukoon_document_number(row.get('Doc Number', ''))
        ),
        axis=1
    )
    
    # Create a mapping from FAEU composite keys to FAEU rows
    faeu_key_to_row = {}
    
    for idx, row in faeu_df.iterrows():
        policy_no = row.get('POLICYNO', '')
        
        # Try INSTAXINVOICE
        insta_invoice = row.get('INSTAXINVOICE', '')
        doc_no_insta = extract_document_number(insta_invoice)
        
        if doc_no_insta:
            faeu_key_insta = create_composite_key(policy_no, doc_no_insta)
            if faeu_key_insta not in faeu_key_to_row:
                faeu_key_to_row[faeu_key_insta] = row
        
        # Try BRKTAXINVOICENO
        brkt_invoice = row.get('BRKTAXINVOICENO', '')
        doc_no_brkt = extract_document_number(brkt_invoice)
        
        if doc_no_brkt:
            faeu_key_brkt = create_composite_key(policy_no, doc_no_brkt)
            if faeu_key_brkt not in faeu_key_to_row:
                faeu_key_to_row[faeu_key_brkt] = row
    
    remarks_list = []
    
    for idx, row in sukoon_df.iterrows():
        sukoon_key = row.get('composite_key', '')
        sukoon_trans_amt = parse_numeric(row.get('Trans Amt Local', 0))
        
        # Check if key exists in FAEU and is available
        if sukoon_key in faeu_key_to_row:
            faeu_row = faeu_key_to_row[sukoon_key]
            
            # Check if Available is "Yes" in FAEU
            faeu_available = faeu_row.get('Available', 'No')
            
            if faeu_available == "Yes":
                # Get FAEU amounts
                total_premium = parse_numeric(faeu_row.get('TOTPREMIUMINCLUDINGVAT', 0))
                inst_commission = parse_numeric(faeu_row.get('INSTOTALCOMMAMOUNT', 0))
                
                # Check if amounts match
                if (abs(sukoon_trans_amt - total_premium) < 0.01) or (abs(sukoon_trans_amt - inst_commission) < 0.01):
                    remarks_list.append("Reconciled")
                else:
                    remarks_list.append("Not Reconciled")
            else:
                remarks_list.append("Not Reconciled")
        else:
            remarks_list.append("Not Reconciled")
    
    sukoon_df['Remarks'] = remarks_list
    
    return sukoon_df


def add_faeu_remarks(faeu_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Remarks column to FAEU sheet
    Logic:
    - Reconciled: CSSET_SETAMOUNT == TOTPREMIUMINCLUDINGVAT AND TAXSET_SETAMOUNT == INSTOTALCOMMAMOUNT
    - DS_PENDING: (CSSET_SETAMOUNT == 0 OR empty) AND TAXSET_SETAMOUNT == INSTOTALCOMMAMOUNT
    - Premium and Commission outstanding: CSSET_SETAMOUNT == 0 AND TAXSET_SETAMOUNT == 0
    - Prem Missing: TOTPREMIUMINCLUDINGVAT is missing/null/empty
    - Reconciled and Comm Receivable: CSSET_SETAMOUNT == TOTPREMIUMINCLUDINGVAT AND TAXSET_SETAMOUNT == 0 (or < INSTOTALCOMMAMOUNT)
    """
    remarks_list = []
    
    for idx, row in faeu_df.iterrows():
        # Check if Available is "Yes"
        available = row.get('Available', 'No')
        
        if available != "Yes":
            remarks_list.append("Not Available in Sukoon")
            continue
        
        # Get all relevant amounts
        cs_settlement = parse_numeric(row.get('CSSET_SETAMOUNT', 0))
        tax_settlement = parse_numeric(row.get('TAXSET_SETAMOUNT', 0))
        total_premium = row.get('TOTPREMIUMINCLUDINGVAT', None)
        inst_commission = parse_numeric(row.get('INSTOTALCOMMAMOUNT', 0))
        
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
        if (cs_settlement == 0 or is_empty_or_null(row.get('CSSET_SETAMOUNT', None))) and abs(tax_settlement - inst_commission) < 0.01:
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


def main(input_file: str, output_file: str):
    """
    Main reconciliation function
    """
    print(f"Reading Excel file: {input_file}")
    
    # Read both sheets (headers are in row 2, index 2)
    try:
        faeu_df = pd.read_excel(input_file, sheet_name='FAEU', header=2)
        print(f"FAEU sheet loaded: {len(faeu_df)} rows")
    except Exception as e:
        print(f"Error reading FAEU sheet: {e}")
        print("Trying alternative sheet names...")
        # Try alternative names
        sheet_names = pd.ExcelFile(input_file).sheet_names
        print(f"Available sheets: {sheet_names}")
        for name in sheet_names:
            if 'faeu' in name.lower() or 'praktora' in name.lower():
                faeu_df = pd.read_excel(input_file, sheet_name=name, header=2)
                print(f"Using sheet: {name}")
                break
    
    try:
        sukoon_df = pd.read_excel(input_file, sheet_name='Sukoon', header=2)
        print(f"Sukoon sheet loaded: {len(sukoon_df)} rows")
    except Exception as e:
        print(f"Error reading Sukoon sheet: {e}")
        print("Trying alternative sheet names...")
        sheet_names = pd.ExcelFile(input_file).sheet_names
        for name in sheet_names:
            if 'sukoon' in name.lower() or 'insurer' in name.lower():
                sukoon_df = pd.read_excel(input_file, sheet_name=name, header=2)
                print(f"Using sheet: {name}")
                break
    
    print("\nStarting reconciliation...")
    
    # Step 1: Reconcile FAEU with Sukoon (create Available column)
    print("Step 1: Creating 'Available' column in FAEU sheet...")
    faeu_df = reconcile_faeu_with_sukoon(faeu_df, sukoon_df.copy())
    
    # Step 2: Add Remarks to Sukoon sheet
    print("Step 2: Adding 'Remarks' column to Sukoon sheet...")
    sukoon_df = add_sukoon_remarks(faeu_df.copy(), sukoon_df)
    
    # Step 3: Add Remarks to FAEU sheet
    print("Step 3: Adding 'Remarks' column to FAEU sheet...")
    faeu_df = add_faeu_remarks(faeu_df)
    
    # Remove temporary composite_key column if it exists
    if 'composite_key' in faeu_df.columns:
        faeu_df = faeu_df.drop(columns=['composite_key'])
    if 'composite_key' in sukoon_df.columns:
        sukoon_df = sukoon_df.drop(columns=['composite_key'])
    
    print("\nSaving results...")
    
    # Save to output file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        faeu_df.to_excel(writer, sheet_name='FAEU', index=False)
        sukoon_df.to_excel(writer, sheet_name='Sukoon', index=False)
    
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
    
    print(f"\nSukoon Sheet:")
    print(f"  Total rows: {len(sukoon_df)}")
    if 'Remarks' in sukoon_df.columns:
        remarks_counts = sukoon_df['Remarks'].value_counts()
        print(f"  Remarks: {remarks_counts.to_dict()}")


if __name__ == "__main__":
    import sys
    
    # Default file names
    input_file = "sheet.xlsx"
    output_file = "reconciliation_output.xlsx"
    
    # Allow command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    main(input_file, output_file)
