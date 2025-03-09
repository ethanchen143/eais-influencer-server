# brands.py
from flask import Blueprint, request, jsonify, abort
from app import db
from app.models import Influencer, Brand
from sqlalchemy.exc import IntegrityError

brands_bp = Blueprint('brands', __name__, url_prefix='/brands')

# Create a Brand
@brands_bp.route('/', methods=['POST'])
def create_brand():
    data = request.get_json()
    try:
        brand = Brand(
            name=data['name'],
            industry=data.get('industry'),
            website=data.get('website'),
            description=data.get('description'),
            contact_email=data.get('contact_email')
        )
        db.session.add(brand)
        db.session.commit()
        return jsonify({'message': 'Brand created', 'id': brand.id}), 201
    except IntegrityError:
        db.session.rollback()
        abort(400, description="Brand name must be unique.")

# Read all Brands
@brands_bp.route('/', methods=['GET'])
def get_brands():
    brands = Brand.query.all()
    return jsonify([{
        'id': brand.id,
        'name': brand.name,
        'industry': brand.industry,
        'website': brand.website
    } for brand in brands])

# Read a single Brand
@brands_bp.route('/<int:brand_id>', methods=['GET'])
def get_brand(brand_id):
    brand = Brand.query.get_or_404(brand_id)
    return jsonify({
        'id': brand.id,
        'name': brand.name,
        'industry': brand.industry,
        'website': brand.website,
        'description': brand.description,
        'contact_email': brand.contact_email,
        'influencers': [inf.username for inf in brand.influencers]
    })

# Update a Brand
@brands_bp.route('/<int:brand_id>', methods=['PUT'])
def update_brand(brand_id):
    brand = Brand.query.get_or_404(brand_id)
    data = request.get_json()
    try:
        brand.name = data.get('name', brand.name)
        brand.industry = data.get('industry', brand.industry)
        brand.website = data.get('website', brand.website)
        brand.description = data.get('description', brand.description)
        brand.contact_email = data.get('contact_email', brand.contact_email)
        db.session.commit()
        return jsonify({'message': 'Brand updated'})
    except IntegrityError:
        db.session.rollback()
        abort(400, description="Brand name must be unique.")

# Delete a Brand
@brands_bp.route('/<int:brand_id>', methods=['DELETE'])
def delete_brand(brand_id):
    brand = Brand.query.get_or_404(brand_id)
    db.session.delete(brand)
    db.session.commit()
    return jsonify({'message': 'Brand deleted'})

# Add an Influencer to a Brand
@brands_bp.route('/<int:brand_id>/influencers', methods=['POST'])
def add_influencer_to_brand(brand_id):
    brand = Brand.query.get_or_404(brand_id)
    data = request.get_json()
    influencer_id = data.get('influencer_id')
    if not influencer_id:
        abort(400, description="influencer_id is required.")
    influencer = Influencer.query.get_or_404(influencer_id)
    brand.influencers.append(influencer)
    db.session.commit()
    return jsonify({'message': f'Influencer {influencer.username} added to Brand {brand.name}.'})

# Remove an Influencer from a Brand
@brands_bp.route('/<int:brand_id>/influencers/<int:influencer_id>', methods=['DELETE'])
def remove_influencer_from_brand(brand_id, influencer_id):
    brand = Brand.query.get_or_404(brand_id)
    influencer = Influencer.query.get_or_404(influencer_id)
    if influencer in brand.influencers:
        brand.influencers.remove(influencer)
        db.session.commit()
        return jsonify({'message': f'Influencer {influencer.username} removed from Brand {brand.name}.'})
    else:
        abort(404, description="Influencer not associated with this Brand.")