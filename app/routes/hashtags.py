# hashtags.py
from flask import Blueprint, request, jsonify, abort
from app import db
from app.models import Influencer, Hashtag, influencer_hashtag
from sqlalchemy.exc import IntegrityError
from sqlalchemy import desc

hashtags_bp = Blueprint('hashtags', __name__, url_prefix='/hashtags')

# Read all Hashtags
@hashtags_bp.route('/', methods=['GET'])
def get_hashtags():
    hashtags = Hashtag.query.all()
    return jsonify([{
        'id': hashtag.id,
        'name': hashtag.name,
        'topic': hashtag.topic,
        'description': hashtag.description
    } for hashtag in hashtags])

# Get all influencers for a hashtag
@hashtags_bp.route('/<int:hashtag_id>/influencers', methods=['GET'])
def get_influencers_for_hashtag(hashtag_id):
    # Get optional query parameters
    limit = request.args.get('limit', default=10, type=int)
    min_usage = request.args.get('min_usage', default=1, type=int)
    
    # First verify hashtag exists
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    
    # Query through junction table
    influencers = (
        db.session.query(
            Influencer,
            influencer_hashtag.c.usage_count
        )
        .join(influencer_hashtag)
        .filter(
            influencer_hashtag.c.hashtag_id == hashtag_id,
            influencer_hashtag.c.usage_count >= min_usage
        )
        .order_by(desc(influencer_hashtag.c.usage_count))
        .limit(limit)
        .all()
    )
    
    return jsonify([{
        'id': inf.id,
        'username': inf.username,
        'follower_count': inf.follower_count,
        'engagement_rate': inf.engagement_rate,
        'usage_count': count  # How many times they used this hashtag
    } for inf, count in influencers])

@hashtags_bp.route('/search', methods=['GET'])
def search_hashtags():
    query = request.args.get('q', '')
    limit = request.args.get('limit', default=10, type=int)
    
    hashtags = (
        db.session.query(
            Hashtag,
            db.func.count(influencer_hashtag.c.influencer_id).label('influencer_count'),
            db.func.sum(influencer_hashtag.c.usage_count).label('total_uses')
        )
        .join(influencer_hashtag)
        .filter(Hashtag.name.ilike(f'%{query}%'))
        .group_by(Hashtag.id)
        .order_by(desc('total_uses'))
        .limit(limit)
        .all()
    )
    
    return jsonify([{
        'id': tag.id,
        'name': tag.name,
        'topic': tag.topic,
        'influencer_count': inf_count,  # How many influencers use this
        'total_uses': total_uses       # Total times used across all influencers
    } for tag, inf_count, total_uses in hashtags])

# Create a Hashtag
@hashtags_bp.route('/', methods=['POST'])
def create_hashtag():
    data = request.get_json()
    try:
        hashtag = Hashtag(
            name=data['name'],
            topic=data.get('topic'),
            description=data.get('description')
        )
        db.session.add(hashtag)
        db.session.commit()
        return jsonify({'message': 'Hashtag created', 'id': hashtag.id}), 201
    except IntegrityError as e:
        db.session.rollback()
        abort(400, description="Hashtag name must be unique.")
    except Exception as e:
        abort(500, description="An error occurred.")

# Read a single Hashtag
@hashtags_bp.route('/<int:hashtag_id>', methods=['GET'])
def get_hashtag(hashtag_id):
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    return jsonify({
        'id': hashtag.id,
        'name': hashtag.name,
        'topic': hashtag.topic,
        'description': hashtag.description,
        'influencers': [inf.username for inf in hashtag.influencers]
    })

# Update a Hashtag
@hashtags_bp.route('/<int:hashtag_id>', methods=['PUT'])
def update_hashtag(hashtag_id):
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    data = request.get_json()
    try:
        hashtag.name = data.get('name', hashtag.name)
        hashtag.topic = data.get('topic', hashtag.topic)
        hashtag.description = data.get('description', hashtag.description)
        db.session.commit()
        return jsonify({'message': 'Hashtag updated'})
    except IntegrityError:
        db.session.rollback()
        abort(400, description="Hashtag name must be unique.")

# Delete a Hashtag
@hashtags_bp.route('/<int:hashtag_id>', methods=['DELETE'])
def delete_hashtag(hashtag_id):
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    db.session.delete(hashtag)
    db.session.commit()
    return jsonify({'message': 'Hashtag deleted'})

# Add an Influencer to a Hashtag
@hashtags_bp.route('/<int:hashtag_id>/influencers', methods=['POST'])
def add_influencer_to_hashtag(hashtag_id):
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    data = request.get_json()
    influencer_id = data.get('influencer_id')
    if not influencer_id:
        abort(400, description="influencer_id is required.")
    influencer = Influencer.query.get_or_404(influencer_id)
    hashtag.influencers.append(influencer)
    db.session.commit()
    return jsonify({'message': f'Influencer {influencer.username} added to Hashtag {hashtag.name}.'})

# Remove an Influencer from a Hashtag
@hashtags_bp.route('/<int:hashtag_id>/influencers/<int:influencer_id>', methods=['DELETE'])
def remove_influencer_from_hashtag(hashtag_id, influencer_id):
    hashtag = Hashtag.query.get_or_404(hashtag_id)
    influencer = Influencer.query.get_or_404(influencer_id)
    if influencer in hashtag.influencers:
        hashtag.influencers.remove(influencer)
        db.session.commit()
        return jsonify({'message': f'Influencer {influencer.username} removed from Hashtag {hashtag.name}.'})
    else:
        abort(404, description="Influencer not associated with this Hashtag.")