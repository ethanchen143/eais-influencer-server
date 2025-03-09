import pandas as pd
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from app.models import Influencer  # Import the Influencer model

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Set up database connection with proper error handling"""
    try:
        # Load environment variables
        basedir = os.path.abspath(os.path.dirname(__file__))
        load_dotenv(os.path.join(basedir, '../.env'))
        
        # Database connection setup
        SQLALCHEMY_DATABASE_URI = (
            os.environ.get('DATABASE_URL') or
            f"postgresql+psycopg2://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@"
            f"{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}"
        )
        
        # Create engine with connection pooling
        engine = create_engine(
            SQLALCHEMY_DATABASE_URI,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )
        
        # Create a session factory
        Session = sessionmaker(bind=engine)
        return Session
    
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def preprocess_dataframe(df):
    """Clean and preprocess the dataframe before database insertion"""
    try:
        # Make a copy to avoid warnings
        df = df.copy()
        
        # Convert NaN values to None for SQL compatibility
        df = df.where(pd.notna(df), None)
        
        # Type conversions with error handling
        # Convert most_recent_upload to datetime
        df['most_recent_upload'] = pd.to_datetime(df['most_recent_upload'], errors='coerce')
        
        # Convert boolean and numeric fields
        boolean_cols = ['verified']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else bool(x))
        
        int_cols = ['followers_count', 'average_likes', 'average_views', 'video_count', 'most_view_count']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))
        
        return df
    
    except Exception as e:
        logger.error(f"Error preprocessing data: {e}")
        raise

def load_influencer_data(csv_path, batch_size=1000):
    """
    Load influencer data from a CSV file into the 'influencers' table
    using bulk inserts and batch processing.
    
    Args:
        csv_path (str): Path to the CSV file
        batch_size (int): Number of records to process in each batch
    """
    Session = None
    session = None
    
    try:
        # Get database session
        Session = get_database_connection()
        session = Session()
        
        # Load and validate CSV
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info(f"Loading data from {csv_path}")
        df = pd.read_csv(csv_path)
        
        if df.empty:
            logger.warning("CSV file is empty. No data to import.")
            return
        
        # Preprocess data
        df = preprocess_dataframe(df)
        total_rows = len(df)
        logger.info(f"Processing {total_rows} records in batches of {batch_size}")
        
        # Process in batches to avoid memory issues with large datasets
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_records = []
            
            # Create list of model instances for bulk insert
            for _, row in batch_df.iterrows():
                influencer = Influencer(
                    id=row['id'],
                    username=row['username'],
                    full_name=row.get('full_name'),
                    profile_link=row.get('profile_link'),
                    bio=row.get('bio'),
                    creator_gender=row.get('creator_gender'),
                    creator_city=row.get('creator_city'),
                    creator_state=row.get('creator_state'),
                    creator_country=row.get('creator_country'),
                    followers_count=row.get('followers_count'),
                    average_likes=row.get('average_likes'),
                    average_views=row.get('average_views'),
                    engagement_rate=row.get('engagement_rate'),
                    email=row.get('email'),
                    instagram_link=row.get('instagram_link'),
                    youtube_link=row.get('youtube_link'),
                    video_desc=row.get('video_desc'),
                    video_count=row.get('video_count'),
                    view_counts=row.get('view_counts'),
                    most_view_count=row.get('most_view_count'),
                    most_recent_upload=row.get('most_recent_upload'),
                    verified=row.get('verified'),
                    audience_desc=row.get('audience_desc')
                )
                batch_records.append(influencer)
            
            try:
                # Bulk insert operation
                session.bulk_save_objects(batch_records)
                session.commit()
                logger.info(f"Processed batch: {i} to {min(i+batch_size, total_rows)}")
                
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Database error in batch {i}-{min(i+batch_size, total_rows)}: {e}")
                
                # Fall back to individual inserts for this batch to identify problematic records
                for record in batch_records:
                    try:
                        session.add(record)
                        session.commit()
                    except SQLAlchemyError as e:
                        session.rollback()
                        logger.error(f"Error inserting record {record.id} - {record.username}: {e}")
        
        logger.info("Data import completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to load influencer data: {e}")
        if session:
            session.rollback()
    
    finally:
        # Always close the session
        if session:
            session.close()
            logger.info("Database session closed")

if __name__ == "__main__":
    try:
        # Example usage: Load data from the CSV
        csv_path = './data/influencer.csv'
        load_influencer_data(csv_path)
    except Exception as e:
        logger.error(f"Script execution failed: {e}")

# SQL to reset
# TRUNCATE TABLE influencers RESTART IDENTITY CASCADE;