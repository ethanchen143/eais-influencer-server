from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from config import Config

db = SQLAlchemy()
migrate = Migrate()

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    CORS(app, resources={
        r"/api/*": {
            "origins": "*",  
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"], 
            "allow_headers": ["Content-Type", "Authorization"]  
        }
    })

    db.init_app(app)
    migrate.init_app(app, db)

    # Register Blueprints
    from app.routes.influencers import influencers_bp
    from app.routes.brands import brands_bp
    from app.routes.hashtags import hashtags_bp

    app.register_blueprint(influencers_bp, url_prefix='/api/influencers')
    app.register_blueprint(brands_bp, url_prefix='/api/brands')
    app.register_blueprint(hashtags_bp, url_prefix='/api/hashtags')

    @app.route('/')
    def index():
        return "CreatoRain Influencers API is running."

    return app