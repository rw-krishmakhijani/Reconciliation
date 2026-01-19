# Reconciliation Script

This script reconciles data between FAEU (Praktora) and Sukoon (Insurer) Excel sheets.

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python reconciliation.py
```

This will:
- Read `sheet.xlsx` (default input file)
- Process both FAEU and Sukoon sheets
- Save results to `reconciliation_output.xlsx` (default output file)

### Custom File Names

```bash
python reconciliation.py input_file.xlsx output_file.xlsx
```

## What the Script Does

### 1. Policy Number Normalization
- Removes brackets and their contents: `OIGM202300135224 (EBP)` → `OIGM202300135224`
- Removes everything after dash: `OIGM202400150383 - CAT B` → `OIGM202400150383`

### 2. Composite Key Creation
- **Sukoon**: `Policy number` + `Document number`
- **FAEU**: `Policy number` + `Document number` (extracted from `INSTAXINVOICENO` or `BRKTAXINVOICENO`)

### 3. Available Column (FAEU Sheet)
Checks if the FAEU composite key matches any Sukoon composite key:
- First tries `INSTAXINVOICENO` document number
- If not found, tries `BRKTAXINVOICENO` document number
- Values: "Yes" or "No"

### 4. Remarks Column (Sukoon Sheet)
- **Reconciled**: Composite key matches FAEU AND (`TransAmt` == `TotalPremiumIncludingVAT` OR `TransAmt` == `INSTTOTALCOMISSION`)
- **Not Reconciled**: Otherwise

### 5. Remarks Column (FAEU Sheet)
- **Reconciled**: `CSSET_SETAMOUNT` == `TotalPremiumIncludingVAT` AND `TAXSET_SETAMOUNT` == `INSTTOTALCOMISSION`
- **DS_PENDING**: (`CSSET_SETAMOUNT` == 0 OR empty) AND `TAXSET_SETAMOUNT` == `INSTTOTALCOMISSION`
- **Premium and Commission outstanding**: `CSSET_SETAMOUNT` == 0 AND `TAXSET_SETAMOUNT` == 0
- **Prem Missing**: `TotalPremiumIncludingVAT` is missing/null/empty
- **Reconciled and Comm Receivable**: `CSSET_SETAMOUNT` == `TotalPremiumIncludingVAT` AND (`TAXSET_SETAMOUNT` == 0 OR < `INSTTOTALCOMISSION`)
- **Not Available in Sukoon**: Available column is "No"
- **Not Reconciled**: Default case

## Excel Sheet Requirements

The input Excel file must contain two sheets:
- **FAEU** (or sheet name containing "faeu" or "praktora")
- **Sukoon** (or sheet name containing "sukoon" or "insurer")

### Required Columns

#### FAEU Sheet:
- `Policy number`
- `INSTAXINVOICENO`
- `BRKTAXINVOICENO`
- `TotalPremiumIncludingVAT`
- `INSTTOTALCOMISSION`
- `CSSET_SETAMOUNT`
- `TAXSET_SETAMOUNT`

#### Sukoon Sheet:
- `Policy number`
- `Document number`
- `Trans amt`

## Output

The script generates a new Excel file with:
- Both sheets updated with new columns:
  - FAEU: `Available`, `Remarks`
  - Sukoon: `Remarks`
- All original data preserved
- Summary statistics printed to console
