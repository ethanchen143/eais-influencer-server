import requests
import json

# Base URL of the API
BASE_URL = "http://127.0.0.1:5000/api"

# Function to insert a new hashtag using POST request
def insert_hashtag(name, topic=None, description=None):
    url = f"{BASE_URL}/hashtags/"
    payload = {
        "name": name,
        "topic": topic,
        "description": description
    }
    response = requests.post(url, json=payload)
    if response.status_code == 201:
        hashtag_id = response.json().get('id')
        print(f"Hashtag '{name}' created with ID: {hashtag_id}")
        return hashtag_id
    else:
        print(f"Failed to create hashtag '{name}'. Status Code: {response.status_code}, Response: {response.text}")
        return None

# Function to insert a new influencer using POST request
def insert_influencer(username, full_name=None, profile_link=None, bio=None,
                     creator_gender=None, creator_language=None, creator_city=None,
                     creator_state=None, creator_country=None, followers_count=0,
                     average_likes=0, average_views=0, engagement_rate=0.0,
                     email=None, linktree_link=None, twitter_link=None,
                     contact_instagram=None, facebook_profile=None,
                     contact_whatsapp=None, contact_telegram=None,
                     contact_snapchat=None, contact_phone=None,
                     contact_pinterest=None, youtube_channel=None):
    url = f"{BASE_URL}/influencers/"
    payload = {
        "username": username,
        "full_name": full_name,
        "profile_link": profile_link,
        "bio": bio,
        "creator_gender": creator_gender,
        "creator_language": creator_language,
        "creator_city": creator_city,
        "creator_state": creator_state,
        "creator_country": creator_country,
        "followers_count": followers_count,
        "average_likes": average_likes,
        "average_views": average_views,
        "engagement_rate": engagement_rate,
        "email": email,
        "linktree_link": linktree_link,
        "twitter_link": twitter_link,
        "contact_instagram": contact_instagram,
        "facebook_profile": facebook_profile,
        "contact_whatsapp": contact_whatsapp,
        "contact_telegram": contact_telegram,
        "contact_snapchat": contact_snapchat,
        "contact_phone": contact_phone,
        "contact_pinterest": contact_pinterest,
        "youtube_channel": youtube_channel
    }
    # Remove keys with None values to avoid sending them in the payload
    payload = {k: v for k, v in payload.items() if v is not None}
    
    response = requests.post(url, json=payload)
    if response.status_code == 201:
        influencer_id = response.json().get('id')
        print(f"Influencer '{username}' created with ID: {influencer_id}")
        return influencer_id
    else:
        print(f"Failed to create influencer '{username}'. Status Code: {response.status_code}, Response: {response.text}")
        return None

# Function to associate a hashtag with an influencer using POST request
def associate_hashtag_to_influencer(influencer_id, hashtag_id):
    url = f"{BASE_URL}/influencers/{influencer_id}/hashtags"
    payload = {
        "hashtag_id": hashtag_id
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print(f"Hashtag ID {hashtag_id} associated with Influencer ID {influencer_id}.")
    else:
        print(f"Failed to associate Hashtag ID {hashtag_id} with Influencer ID {influencer_id}. Status Code: {response.status_code}, Response: {response.text}")

# Function to retrieve influencers by a specific hashtag using GET request
def get_influencers_by_hashtag(hashtag_id):
    url = f"{BASE_URL}/hashtags/{hashtag_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Influencers associated with Hashtag '{data['name']}':")
        for influencer in data['influencers']:
            print(f" - {influencer}")
    else:
        print(f"Failed to retrieve influencers for Hashtag ID {hashtag_id}. Status Code: {response.status_code}, Response: {response.text}")

def main():
    # Expected Output:
    # === Creating Hashtags ===
    # Hashtag 'travel' created with ID: 1
    # Hashtag 'food' created with ID: 2
    # Hashtag 'fashion' created with ID: 3

    # === Creating Influencers ===
    # Influencer 'traveler_jane' created with ID: 1
    # Influencer 'foodie_mike' created with ID: 2
    # Influencer 'fashionista_anna' created with ID: 3

    # === Associating Hashtags with Influencers ===
    # Hashtag ID 1 associated with Influencer ID 1.
    # Hashtag ID 2 associated with Influencer ID 2.
    # Hashtag ID 3 associated with Influencer ID 3.

    # === Creating a Fourth Influencer with an Existing Hashtag ===
    # Influencer 'explorer_tim' created with ID: 4
    # Hashtag ID 1 associated with Influencer ID 4.

    # === Retrieving Influencers by Hashtag '#travel' ===
    # Influencers associated with Hashtag 'travel':
    # - traveler_jane
    # - explorer_tim

    # === Retrieving Influencers by Other Hashtags ===
    # Influencers associated with Hashtag 'food':
    # - foodie_mike
    # Influencers associated with Hashtag 'fashion':
    # - fashionista_anna

    print("=== Creating Hashtags ===")
    # Create three unique hashtags
    hashtag1_id = insert_hashtag("travel", "Traveling the world", "All about travel experiences.")
    hashtag2_id = insert_hashtag("food", "Culinary delights", "Sharing delicious food recipes and reviews.")
    hashtag3_id = insert_hashtag("fashion", "Latest trends", "Showcasing the latest in fashion.")

    print("\n=== Creating Influencers ===")
    # Create three influencers, each with a different hashtag
    influencer1_id = insert_influencer(
        username="traveler_jane",
        full_name="Jane Doe",
        followers_count=10000
    )
    influencer2_id = insert_influencer(
        username="foodie_mike",
        full_name="Mike Smith",
        followers_count=15000
    )
    influencer3_id = insert_influencer(
        username="fashionista_anna",
        full_name="Anna Lee",
        followers_count=20000
    )

    print("\n=== Associating Hashtags with Influencers ===")
    # Associate each influencer with their respective unique hashtag
    if influencer1_id and hashtag1_id:
        associate_hashtag_to_influencer(influencer1_id, hashtag1_id)  # traveler_jane -> #travel
    if influencer2_id and hashtag2_id:
        associate_hashtag_to_influencer(influencer2_id, hashtag2_id)  # foodie_mike -> #food
    if influencer3_id and hashtag3_id:
        associate_hashtag_to_influencer(influencer3_id, hashtag3_id)  # fashionista_anna -> #fashion

    print("\n=== Creating a Fourth Influencer with an Existing Hashtag ===")
    # Create a fourth influencer associated with an existing hashtag (#travel)
    influencer4_id = insert_influencer(
        username="explorer_tim",
        full_name="Tim Brown",
        followers_count=12000
    )
    if influencer4_id and hashtag1_id:
        associate_hashtag_to_influencer(influencer4_id, hashtag1_id)  # explorer_tim -> #travel

    print("\n=== Retrieving Influencers by Hashtag '#travel' ===")
    # Retrieve and display influencers associated with the '#travel' hashtag
    get_influencers_by_hashtag(hashtag1_id)

    print("\n=== Retrieving Influencers by Other Hashtags ===")
    # Optionally, retrieve influencers for other hashtags to verify associations
    get_influencers_by_hashtag(hashtag2_id)  # #food
    get_influencers_by_hashtag(hashtag3_id)  # #fashion

if __name__ == "__main__":
    main()