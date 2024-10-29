from transformation.marly_helper import process_data
from auth.anon_helper import raw_profile_data
from output_source.sql_helper import SQLiteHelper

import json

def extract_metrics_data(results):
    if not results or not results[0].results:
        return None
    
    metrics = results[0].results[0]['metrics']
    if not metrics:
        return None
    
    json_string = metrics[next(iter(metrics.keys()))]
    
    data = json.loads(json_string)
    
    if 'data_entries' in data:
        records = data['data_entries']
    else:
        records = [data]
    
    cleaned_data = []
    for record in records:
        cleaned_record = {
            'id': record.get('id'),
            'first_name': record.get('firstName'),
            'last_name': record.get('lastName'),
            'headline': record.get('headline'),
            'location': record.get('location'),
            'summary': record.get('summary'),
            'connections_count': record.get('connectionsCount')
        }
        cleaned_data.append(cleaned_record)
    
    return cleaned_data

if __name__ == "__main__":
    raw_data = raw_profile_data()
    processed_data = process_data(raw_data)
    cleaned_data = extract_metrics_data(processed_data)
    db = SQLiteHelper()
    success = db.insert_contact(cleaned_data)
    db.get_all_contacts()
