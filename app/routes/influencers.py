# influencers.py
from flask import Blueprint, request, jsonify, abort
from app import db
from app.models import Influencer, Brand, Hashtag
from sqlalchemy.exc import IntegrityError
import os
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
import warnings

warnings.filterwarnings('ignore')

model = SentenceTransformer('all-MiniLM-L6-v2')

pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))

index_name = "experiment"

# connect to index
index = pc.Index(index_name)

# view index stats
print("Index stats:",index.describe_index_stats())

def retrieve_from_pinecone(user_query, num_results=20):
    ## Pinecone context code:
    results=index.query(
    vector=model.encode(user_query).tolist(),
    # filter={
    #     "genre": {"$eq": "documentary"}
    # },
    top_k=num_results,
    include_metadata=True # Include metadata in the response.
    )
    return results

# matches=retrieve_from_pinecone('I am selling a new nutritional supplement')
# print(matches)

influencers_bp = Blueprint('influencers', __name__, url_prefix='/influencers')

# Create an Influencer
@influencers_bp.route('/', methods=['POST'])
def create_influencer():
    data = request.get_json()
    try:
        influencer = Influencer(
            username=data['username'],
            full_name=data.get('full_name'),
            profile_link=data.get('profile_link'),
            bio=data.get('bio'),
            creator_gender=data.get('creator_gender'),
            creator_city=data.get('creator_city'),
            creator_state=data.get('creator_state'),
            creator_country=data.get('creator_country'),
            followers_count=data.get('followers_count'),
            average_likes=data.get('average_likes'),
            average_views=data.get('average_views'),
            engagement_rate=data.get('engagement_rate'),
            email=data.get('email'),
            instagram_link=data.get('instagram_link'),
            youtube_link=data.get('youtube_link'),
            video_desc=data.get('video_desc'),
            video_count = data.get('video_count'),
            view_counts = data.get('view_counts'),
            most_view_count = data.get('most_view_count'),
            most_recent_upload = data.get('most_recent_upload'),
            verified = data.get('verified'),
            audience_desc=data.get('audience_desc')
        )
        db.session.add(influencer)
        db.session.commit()
        return jsonify({'message': 'Influencer created', 'id': influencer.id}), 201
    except IntegrityError:
        db.session.rollback()
        abort(400, description="Username must be unique.")

from random import randint, sample
# Get random influencers with matching score
@influencers_bp.route('/random', methods=['GET'])
def get_random_influencers():
    # Get influencer count from query params, default to 200
    influencer_count = request.args.get('influencer_count', default=10, type=int)
    # Get all influencers
    influencers = Influencer.query.all()
    # Select random influencers if we have more than requested count
    if len(influencers) > influencer_count:
        influencers = sample(influencers, influencer_count)
    results = []
    for inf in influencers:
        # Determine platforms
        platforms = ['TikTok']  # TikTok is assumed as base platform
        if inf.youtube_channel:
            platforms.append('YouTube')
        if inf.contact_instagram:
            platforms.append('Instagram')
            
        results.append({
            'id': inf.id,
            'username': inf.username,
            'followers_count': inf.followers_count,
            'platforms': platforms,
            'geo_location': inf.geo_location,
            'matching_score': randint(1, 100)
        })
    
    return jsonify(results)

# Retrieve influencers with matching scores
@influencers_bp.route('/match', methods=['POST'])
def retrieve_matches():
    # Get request data from the JSON body
    request_data = request.get_json()
    user_query = request_data.get('user_query')
    influencer_count = request_data.get('influencer_count', 10)
    # Assuming `retrieve_from_pinecone` function works and is already imported
    results = retrieve_from_pinecone(user_query, influencer_count)
    # Extract all scores for scaling
    all_scores = [match['score'] for match in results['matches']]
    min_score = min(all_scores)
    max_score = max(all_scores)
    formatted_results = []
    for match in results['matches']:
        # Rescale the score to be a percentile from 0 to 100
        if max_score > min_score:
            score = (match['score'] - min_score) / (max_score - min_score) * 100
        else:
            score = 100  # If all scores are equal, set it to 100
        platforms = []
        if 'TikTok' in match['metadata']['platforms']:
            platforms.append("TikTok")
        if match['metadata'].get('contact_instagram'):
            platforms.append("Instagram")
        if match['metadata'].get('youtube_channel'):
            platforms.append("YouTube")
        
        formatted_results.append({
            "followers_count": int(match['metadata']['followers_count']),
            "id": int(match['metadata']['id']),
            "matching_score": int(score),
            "platforms": platforms,
            "geo_location": match['metadata']['geolocation'],
            "username": match['metadata']['username']
        })
    
    return jsonify(formatted_results)

# Read all Influencers
@influencers_bp.route('/', methods=['GET'])
def get_influencers():
    influencers = Influencer.query.all()
    return jsonify([{
        'id': inf.id,
        'username': inf.username,
        'followers_count': inf.followers_count,
    } for inf in influencers])

# Read a single Influencer
@influencers_bp.route('/<int:influencer_id>', methods=['GET'])
def get_influencer(influencer_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    return jsonify({
        'id': influencer.id,
        'username': influencer.username,
        'full_name': influencer.full_name,
        'profile_link': influencer.profile_link,
        'bio': influencer.bio,
        'creator_gender': influencer.creator_gender,
        'creator_city': influencer.creator_city,
        'creator_state': influencer.creator_state,
        'creator_country': influencer.creator_country,
        'followers_count': influencer.followers_count,
        'average_likes': influencer.average_likes,
        'average_views': influencer.average_views,
        'engagement_rate': influencer.engagement_rate,
        'email': influencer.email,
        'instagram_link': influencer.instagram_link,
        'youtube_link': influencer.youtube_link,
        'video_desc': influencer.video_desc,
        'video_count': influencer.video_count,
        'view_counts': influencer.view_counts,
        'most_view_count': influencer.most_view_count,
        'most_recent_upload': influencer.most_recent_upload,
        'verified': influencer.verified,
        'audience_desc': influencer.audience_desc,
        'hashtags': [hashtag.name for hashtag in influencer.hashtags],
    })

# Update an Influencer
@influencers_bp.route('/<int:influencer_id>', methods=['PUT'])
def update_influencer(influencer_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    data = request.get_json()
    try:
        influencer.username = data.get('username', influencer.username)
        influencer.full_name = data.get('full_name', influencer.full_name)
        influencer.profile_link = data.get('profile_link', influencer.profile_link)
        influencer.bio = data.get('bio', influencer.bio)
        influencer.creator_gender = data.get('creator_gender', influencer.creator_gender)
        influencer.creator_city = data.get('creator_city', influencer.creator_city)
        influencer.creator_state = data.get('creator_state', influencer.creator_state)
        influencer.creator_country = data.get('creator_country', influencer.creator_country)
        influencer.followers_count = data.get('followers_count', influencer.followers_count)
        influencer.average_likes = data.get('average_likes', influencer.average_likes)
        influencer.average_views = data.get('average_views', influencer.average_views)
        influencer.engagement_rate = data.get('engagement_rate', influencer.engagement_rate)
        influencer.email = data.get('email', influencer.email)
        influencer.instagram_link = data.get('instagram_link', influencer.instagram_link)
        influencer.youtube_link = data.get('youtube_link', influencer.youtube_link)
        influencer.video_desc = data.get('video_desc', influencer.video_desc)
        influencer.view_counts = data.get('view_counts', influencer.view_counts)
        influencer.most_view_count = data.get('most_view_count', influencer.most_view_count)
        influencer.most_recent_upload = data.get('most_recent_upload', influencer.most_recent_upload)
        influencer.video_count = data.get('video_count', influencer.video_count)
        influencer.verified = data.get('verified', influencer.verified)
        influencer.audience_desc = data.get('audience_desc', influencer.audience_desc)
        db.session.commit()
        return jsonify({'message': 'Influencer updated'})

    except IntegrityError:
        db.session.rollback()
        abort(400, description="Username must be unique.")

# Delete an Influencer
@influencers_bp.route('/<int:influencer_id>', methods=['DELETE'])
def delete_influencer(influencer_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    db.session.delete(influencer)
    db.session.commit()
    return jsonify({'message': 'Influencer deleted'})

# Add a Brand to an Influencer
@influencers_bp.route('/<int:influencer_id>/brands', methods=['POST'])
def add_brand_to_influencer(influencer_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    data = request.get_json()
    brand_id = data.get('brand_id')
    if not brand_id:
        abort(400, description="brand_id is required.")
    brand = Brand.query.get_or_404(brand_id)
    influencer.brands.append(brand)
    db.session.commit()
    return jsonify({'message': f'Brand {brand.name} added to Influencer {influencer.username}.'})

# Remove a Brand from an Influencer
@influencers_bp.route('/<int:influencer_id>/brands/<int:brand_id>', methods=['DELETE'])
def remove_brand_from_influencer(influencer_id, brand_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    brand = Brand.query.get_or_404(brand_id)
    if brand in influencer.brands:
        influencer.brands.remove(brand)
        db.session.commit()
        return jsonify({'message': f'Brand {brand.name} removed from Influencer {influencer.username}.'})
    else:
        abort(404, description="Brand not associated with this Influencer.")

# Add a Hashtag to an Influencer
@influencers_bp.route('/<int:influencer_id>/hashtags', methods=['POST'])
def add_hashtag_to_influencer(influencer_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    data = request.get_json()
    hashtag_id = data.get('hashtag_id')
    if not hashtag_id:
        abort(400, description="hashtag_id is required.")
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    influencer.hashtags.append(hashtag)
    db.session.commit()
    return jsonify({'message': f'Hashtag {hashtag.name} added to Influencer {influencer.username}.'})

# Remove a Hashtag from an Influencer
@influencers_bp.route('/<int:influencer_id>/hashtags/<int:hashtag_id>', methods=['DELETE'])
def remove_hashtag_from_influencer(influencer_id, hashtag_id):
    influencer = Influencer.query.get_or_404(influencer_id)
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    if hashtag in influencer.hashtags:
        influencer.hashtags.remove(hashtag)
        db.session.commit()
        return jsonify({'message': f'Hashtag {hashtag.name} removed from Influencer {influencer.username}.'})
    else:
        abort(404, description="Hashtag not associated with this Influencer.")