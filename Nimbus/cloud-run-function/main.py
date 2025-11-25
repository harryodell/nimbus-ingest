import requests
import pandas as pd
from google.cloud import bigquery
import functions_framework

# --- CONFIGURATION ---
PROJECT_ID = "nimbus-479211"
DATASET_ID = "nimbus_assignment"
TABLE_ID = "stg_ev_chargepoints"

OCM_API_URL = "https://api.openchargemap.io/v3/poi/"
API_KEY = "f561c179-7f48-4705-a7af-012fdab7f987"
USER_AGENT = "NimbusAssignment/1.0"

CENTRAL_LONDON_PREFIXES = ['EC1', 'EC2', 'EC3', 'EC4', 'WC1', 'WC2', 'SE1', 'SW1', 'W1']

@functions_framework.http
def run_pipeline(request):
    """HTTP Cloud Function entry point."""
    print("--- Starting Cloud Pipeline ---")

    # 1. EXTRACT
    params = {
        "output": "json", "countrycode": "GB", "maxresults": 5000, "compact": False, 
        "key": API_KEY, "latitude": 51.5074, "longitude": -0.1278, "distance": 5, "distanceunit": "Miles"
    }
    
    response = requests.get(OCM_API_URL, params=params, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    raw_data = response.json()
    print(f"Fetched {len(raw_data)} records.")

    # 2. TRANSFORM
    clean_rows = []
    
    for device in raw_data:
        addr = device.get('AddressInfo') or {}
        postcode = (addr.get('Postcode') or '').strip().upper()
        
        matched = False
        for area in CENTRAL_LONDON_PREFIXES:
            if postcode.startswith(area):
                suffix = postcode[len(area):]
                if not suffix or suffix[0] == " " or not suffix[0].isdigit():
                    matched = True
                    break
        if not matched: continue

        operator_info = device.get('OperatorInfo') or {}
        usage_info = device.get('UsageType') or {}
        status_info = device.get('StatusType') or {}
        conns = device.get('Connections') or []

        max_kw = max([float(c.get('PowerKW') or 0) for c in conns], default=0.0)
        conn_types = {c.get('ConnectionType', {}).get('Title', 'Unknown') for c in conns if c.get('ConnectionType')}
        curr_types = {c.get('CurrentType', {}).get('Title', '') for c in conns if c.get('CurrentType')}
        primary_current = "DC" if any("DC" in t for t in curr_types) else "AC"
        operational_count = sum(1 for c in conns if (c.get('StatusType') or {}).get('IsOperational', False))

        clean_rows.append({
            'charge_device_id': str(device.get('ID')),
            'name': (addr.get('Title') or 'Unknown'),
            'latitude': float(addr.get('Latitude') or 0),
            'longitude': float(addr.get('Longitude') or 0),
            'postcode': postcode,
            'town': (addr.get('Town') or 'London'),
            'district': postcode.split()[0][:4], 
            'operator': (operator_info.get('Title') or 'Unknown'),
            'usage_type': (usage_info.get('Title') or 'Unknown'),
            'status': (status_info.get('Title') or 'Unknown'),
            'pay_at_location': bool(usage_info.get('IsPayAtLocation', False)),
            'is_membership_required': bool(usage_info.get('IsMembershipRequired', False)),
            'max_power_kw': max_kw,
            'connector_types': ", ".join(conn_types),
            'current_type': primary_current,
            'total_plugs': len(conns),
            'operational_plugs': operational_count,
            'date_created': pd.to_datetime(device.get('DateCreated'), errors='coerce'),
            'data_quality_level': int(device.get('DataQualityLevel') or 1),
            'last_verified': pd.to_datetime(device.get('DateLastVerified'), errors='coerce'),
            'data_source': 'OpenChargeMap',
            'ingested_at': pd.Timestamp.now()
        })

    df = pd.DataFrame(clean_rows)
    if df.empty: 
        return "No data matched filters."

    # 3. LOAD
    client = bigquery.Client(project=PROJECT_ID)
    
    table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("charge_device_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("postcode", "STRING"),
        bigquery.SchemaField("town", "STRING"),
        bigquery.SchemaField("district", "STRING"),
        bigquery.SchemaField("operator", "STRING"),
        bigquery.SchemaField("usage_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("pay_at_location", "BOOLEAN"),
        bigquery.SchemaField("is_membership_required", "BOOLEAN"),
        bigquery.SchemaField("max_power_kw", "FLOAT"),
        bigquery.SchemaField("connector_types", "STRING"),
        bigquery.SchemaField("current_type", "STRING"),
        bigquery.SchemaField("total_plugs", "INTEGER"),
        bigquery.SchemaField("operational_plugs", "INTEGER"),
        bigquery.SchemaField("date_created", "TIMESTAMP"),
        bigquery.SchemaField("data_quality_level", "INTEGER"),
        bigquery.SchemaField("last_verified", "TIMESTAMP"),
        bigquery.SchemaField("data_source", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)
    job.result()
    
    return f"SUCCESS: Loaded {len(df)} rows into {table_full_id}"
