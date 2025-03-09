from app import db

# Association Table: Influencer ↔ Brand
influencer_brand = db.Table('influencer_brand',
    db.Column('influencer_id', db.Integer, db.ForeignKey('influencers.id'), primary_key=True),
    db.Column('brand_id', db.Integer, db.ForeignKey('brands.id'), primary_key=True),
    db.Column('sales',db.Integer),
    db.Column('payout',db.Integer),
    db.Column('collaboration_details', db.Text),
    db.Column('start_date', db.Date),
    db.Column('end_date', db.Date)
)

# Association Table: Influencer ↔ Hashtag
influencer_hashtag = db.Table('influencer_hashtag',
    db.Column('influencer_id', db.Integer, db.ForeignKey('influencers.id'), primary_key=True),
    db.Column('hashtag_id', db.Integer, db.ForeignKey('hashtags.id'), primary_key=True),
    db.Column('usage_count', db.Integer, default=1)
)

class Influencer(db.Model):
    __tablename__ = 'influencers'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    full_name = db.Column(db.String(100))
    profile_link = db.Column(db.String(255))
    bio = db.Column(db.Text)
    creator_gender = db.Column(db.String(20))
    creator_city = db.Column(db.String(100))
    creator_state = db.Column(db.String(100))
    creator_country = db.Column(db.String(100))
    followers_count = db.Column(db.BigInteger, default=0)
    average_likes = db.Column(db.BigInteger, default=0)
    average_views = db.Column(db.BigInteger, default=0)
    engagement_rate = db.Column(db.Float, default=0.0)
    email = db.Column(db.String(100))
    instagram_link = db.Column(db.String(255))
    youtube_link = db.Column(db.String(255))
    video_desc = db.Column(db.Text)
    video_count = db.Column(db.BigInteger, default=0)
    view_counts = db.Column(db.Text)
    most_view_count = db.Column(db.BigInteger, default=0)
    most_recent_upload = db.Column(db.DateTime)
    verified = db.Column(db.Boolean, default=False)
    audience_desc = db.Column(db.Text)

    brands = db.relationship('Brand', secondary=influencer_brand, backref=db.backref('influencers', lazy='dynamic'))
    hashtags = db.relationship('Hashtag', secondary=influencer_hashtag, backref=db.backref('influencers', lazy='dynamic'))

    def __repr__(self):
        return f'<Influencer {self.username}>'

class Brand(db.Model):
    __tablename__ = 'brands'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    industry = db.Column(db.String(50))
    website = db.Column(db.String(255))
    description = db.Column(db.Text)
    contact_email = db.Column(db.String(100))

    def __repr__(self):
        return f'<Brand {self.name}>'

class Hashtag(db.Model):
    __tablename__ = 'hashtags'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    topic = db.Column(db.String(100))
    description = db.Column(db.String(255))

    def __repr__(self):
        return f'<Hashtag {self.name}>'