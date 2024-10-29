import requests
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import os
import logging

load_dotenv()

BASE_URL = "https://svc.sandbox.anon.com/actions/linkedin/"  
ANON_USER_ID = os.environ.get("ANON_USER_ID")

def get_headers():
    return {
        "Authorization": f"Bearer {os.getenv('ANON_TOKEN')}"
    }

def make_api_request(endpoint, method="GET", params=None):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.request(method, url, headers=get_headers(), params=params)
    return json.loads(response.text)

def get_recent_conversations(days=7):
    params = {"appUserId": ANON_USER_ID}
    data = make_api_request("listConversations", params=params)
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days=days)
    
    recent_profile_ids = []
    for conversation in data['conversations']:
        conversation_date = datetime.strptime(conversation['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        if conversation_date > cutoff_date:
            for profile in conversation['profiles']:
                if not profile['isSelf']:
                    profile_id = profile.get('id', '')
                    if profile_id.startswith('profile-'):
                        profile_id = profile_id[8:]  # Remove 'profile-' prefix
                    if profile_id:
                        recent_profile_ids.append(profile_id)
    
    return recent_profile_ids

def get_profile(profile_id):
    params = {"id": profile_id, "appUserId": ANON_USER_ID}
    return make_api_request("getProfile", params=params)

def raw_profile_data():
    recent_profile_ids = get_recent_conversations()
    profiles = []

    for profile_id in recent_profile_ids:
        try:
            if profile_id.isdigit():
                profile_id = f"ACoAA{profile_id}"
            
            profile_data = get_profile(profile_id)
            
            if profile_data:
                profiles.append(profile_data)
            else:
                logging.warning(f"No data returned for profile ID: {profile_id}")
        except Exception as e:
            logging.error(f"Error fetching profile {profile_id}: {str(e)}")

    return profiles
