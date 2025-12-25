import pandas as pd
import re
import random
import string

# ==========================================
# 1. CONFIGURATION: TERM MAPPING (From LRIS Doc)
# ==========================================
# We define the Urdu keywords that trigger column splitting
TOKENS = {
    'RELATION': r'(pisar|pisaran|dukhtar|dukhtaran|zoja|byuh|w/o|s/o|d/o)',
    'CASTE': r'(kaum)',
    'RESIDENCE': r'(sakin|sakindeh)',
    'CULTIVATOR_PREFIX': r'(kasht)'
}

STATE_CODE = "JK"
DISTRICT_CODE = "BAR"

# ==========================================
# 2. PARSING ENGINE (The "Split Logic")
# ==========================================
def parse_cultivator_column(text):
    """
    Parses column 5 (Nam Kashtakar) into structured AgriStack fields
    using the specific keywords provided in the Term Mapping table.
    """
    if not isinstance(text, str):
        return {'Name': text, 'Parentage': '', 'Caste': '', 'Residence': '', 'Ownership_Type': ''}

    # 1. Normalize Text
    text = text.lower().strip()
    
    # 2. Identify Ownership Type
    ownership_type = "Owner"
    if "kasht" in text:
        ownership_type = "Cultivator"
        text = text.replace("kasht", "").strip() # Remove keyword to isolate Name
        
    # 3. Extract Residence (Right-to-Left parsing strategy)
    residence = ""
    res_match = re.search(TOKENS['RESIDENCE'], text)
    if res_match:
        split_index = res_match.start()
        residence = text[split_index:].replace(res_match.group(), "").strip()
        text = text[:split_index].strip()

    # 4. Extract Caste
    caste = ""
    caste_match = re.search(TOKENS['CASTE'], text)
    if caste_match:
        split_index = caste_match.start()
        caste = text[split_index:].replace(caste_match.group(), "").strip()
        text = text[:split_index].strip()

    # 5. Extract Parentage
    parentage = ""
    rel_match = re.search(TOKENS['RELATION'], text)
    if rel_match:
        split_index = rel_match.start()
        relation_type = rel_match.group()
        parentage_raw = text[split_index:].replace(relation_type, "").strip()
        parentage = f"{relation_type} {parentage_raw}"
        text = text[:split_index].strip()

    # 6. Remaining String is the Name
    name = text.strip()

    return {
        'Name': name.title(),
        'Parentage': parentage.title(),
        'Caste': caste.title(),
        'Residence': residence.title(),
        'Ownership_Type': ownership_type
    }

# ==========================================
# 3. ID GENERATION (AgriStack Standard)
# ==========================================
def generate_farm_id():
    # 14-digit ID with Checksum (Simulated Verhoeff)
    digits = ''.join(random.choices(string.digits, k=11))
    check = sum(int(d) for d in digits) % 10
    return f"{STATE_CODE}{digits}{check}"

def generate_farmer_id():
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    return f"FID-{STATE_CODE}-{suffix}"

# ==========================================
# 4. EXECUTION PIPELINE
# ==========================================
def run_pipeline(input_file):
    print(f"Processing {input_file}...")
    
    # Read CSV (Skipping first 2 metadata rows to get to data)
    # Adjust 'header' or 'skiprows' based on your exact file version
    try:
        df = pd.read_csv(input_file)
    except:
        print("Error reading file. Ensure it is a valid CSV.")
        return

    # Define Column Indices (Based on your Sample File)
    COL_CULTIVATOR = '5'  # Nam Kashtakar
    COL_KHASRA = '7'      # Khasra No
    
    processed_rows = []

    # Iterate through data (skipping metadata rows 0-1)
    for index, row in df.iloc[2:].iterrows():
        
        # Check if row has valid data
        raw_text = str(row.get(COL_CULTIVATOR, ''))
        if pd.isna(raw_text) or 'nan' in raw_text.lower() or not raw_text.strip():
            continue

        # 1. Parse Farmer Demographics
        farmer_data = parse_cultivator_column(raw_text)
        
        # 2. Generate Farmer ID (Unique to this person)
        f_id = generate_farmer_id()
        
        # 3. Explode Khasra Numbers (One Row -> Multiple Plots)
        khasra_raw = str(row.get(COL_KHASRA, ''))
        # Split by newline or comma
        plots = re.split(r'[\n,]', khasra_raw)
        
        for plot in plots:
            if not plot.strip(): continue
            
            # Create Record
            record = farmer_data.copy()
            record['Farmer_ID'] = f_id
            record['Farm_ID'] = generate_farm_id() # Unique per plot
            record['Original_Khasra'] = plot.strip()
            
            # Carry over Context
            record['Khewat'] = row.get('1', '')
            record['Khata'] = row.get('2', '')
            record['Verification_Status'] = 'AUTO_PARSED'
            
            processed_rows.append(record)

    # Export
    df_out = pd.DataFrame(processed_rows)
    
    # Reorder columns for the final report
    final_cols = ['Farmer_ID', 'Name', 'Parentage', 'Caste', 'Residence', 
                  'Farm_ID', 'Original_Khasra', 'Khewat', 'Khata', 'Verification_Status']
    
    # Only select columns that exist
    df_out = df_out[[c for c in final_cols if c in df_out.columns]]
    
    df_out.to_csv("LRIS_Final_Output.csv", index=False)
    print(f"Success! Generated 'LRIS_Final_Output.csv' with {len(df_out)} rows.")

if __name__ == "__main__":
    # Point this to your actual file
    run_pipeline("1. Scheme - Land Record.xlsx - Sample  - output expected.csv")