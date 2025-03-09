import pandas as pd
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from app.models import Hashtag  # Import the Hashtag model

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
        return Session, engine
    
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def preprocess_hashtag_data(df):
    """Clean and preprocess the hashtag dataframe before database insertion"""
    try:
        # Make a copy to avoid warnings
        df = df.copy()
        
        # Handle non-string names
        df['name'] = df['name'].apply(lambda x: str(x) if not pd.isna(x) else "")
        
        # Truncate names longer than 100 characters
        df['name'] = df['name'].apply(lambda x: x[:100])
        
        # Convert NaN values to None for SQL compatibility
        for col in ['topic', 'description']:
            if col in df.columns:
                df[col] = df[col].where(pd.notna(df[col]), None)
        
        return df
    
    except Exception as e:
        logger.error(f"Error preprocessing hashtag data: {e}")
        raise

def deduplicate_hashtags(df):
    """Remove duplicate hashtag names from the dataframe"""
    # Keep the first occurrence of each hashtag name
    df_unique = df.drop_duplicates(subset=['name'], keep='first')
    dupes_removed = len(df) - len(df_unique)
    
    if dupes_removed > 0:
        logger.info(f"Removed {dupes_removed} duplicate hashtag names from input data")
    
    return df_unique

def get_existing_hashtags(engine):
    """Retrieve existing hashtag names from the database to avoid conflicts"""
    try:
        # Direct SQL query for better performance with large tables
        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM hashtags"))
            existing_names = {row[0] for row in result}
            logger.info(f"Found {len(existing_names)} existing hashtags in database")
            return existing_names
    except Exception as e:
        logger.error(f"Error retrieving existing hashtags: {e}")
        return set()

def load_hashtag_data(csv_path, batch_size=500, skip_existing=True):
    """
    Load hashtag data from a CSV file into the 'hashtags' table
    using bulk inserts and batch processing with duplicate handling.
    
    Args:
        csv_path (str): Path to the CSV file
        batch_size (int): Number of records to process in each batch
        skip_existing (bool): Whether to skip hashtags that already exist in DB
    """
    Session = None
    session = None
    
    try:
        # Get database session
        Session, engine = get_database_connection()
        session = Session()
        
        # Load and validate CSV
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info(f"Loading hashtag data from {csv_path}")
        df = pd.read_csv(csv_path)
        
        if df.empty:
            logger.warning("CSV file is empty. No hashtag data to import.")
            return
        
        # Preprocess data
        df = preprocess_hashtag_data(df)
        
        # Remove duplicates within the input data
        df = deduplicate_hashtags(df)
        
        # Get existing hashtags from database to avoid conflicts
        existing_hashtags = set()
        if skip_existing:
            existing_hashtags = get_existing_hashtags(engine)
        
        # Filter out hashtags that already exist in the database
        if existing_hashtags and skip_existing:
            df_filtered = df[~df['name'].isin(existing_hashtags)]
            skipped = len(df) - len(df_filtered)
            logger.info(f"Skipping {skipped} hashtags that already exist in the database")
            df = df_filtered
        
        total_rows = len(df)
        logger.info(f"Processing {total_rows} unique hashtag records in batches of {batch_size}")
        
        if total_rows == 0:
            logger.info("No new hashtags to insert after filtering")
            return
        
        # Track statistics
        inserted_count = 0
        error_count = 0
        
        # Process in batches to avoid memory issues with large datasets
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_records = []
            
            # Create list of model instances for bulk insert
            for _, row in batch_df.iterrows():
                hashtag = Hashtag(
                    id=row['id'],
                    name=row['name'],
                    topic=row.get('topic'),
                    description=row.get('description')
                )
                batch_records.append(hashtag)
            
            try:
                # Bulk insert operation
                session.bulk_save_objects(batch_records)
                session.commit()
                inserted_count += len(batch_records)
                logger.info(f"Processed hashtag batch: {i} to {min(i+batch_size, total_rows)}")
                
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Database error in hashtag batch {i}-{min(i+batch_size, total_rows)}: {e}")
                
                # Fall back to individual inserts with ON CONFLICT DO NOTHING approach
                logger.info("Falling back to individual inserts with conflict handling")
                
                for record in batch_records:
                    try:
                        # Check if hashtag exists
                        existing = session.query(Hashtag).filter(Hashtag.name == record.name).first()
                        
                        if existing:
                            # Skip this record
                            logger.debug(f"Skipping duplicate hashtag: {record.name}")
                            continue
                        
                        # Try to insert
                        session.add(record)
                        session.commit()
                        inserted_count += 1
                        
                    except IntegrityError:
                        # Handle any remaining integrity errors (race conditions, etc.)
                        session.rollback()
                        logger.warning(f"Integrity error inserting hashtag: {record.name}")
                        error_count += 1
                        
                    except SQLAlchemyError as e:
                        # Handle other SQLAlchemy errors
                        session.rollback()
                        logger.error(f"Error inserting hashtag {record.id} - {record.name}: {e}")
                        error_count += 1
        
        logger.info(f"Hashtag data import completed: {inserted_count} inserted, {error_count} errors")
        
    except Exception as e:
        logger.error(f"Failed to load hashtag data: {e}")
        if session:
            session.rollback()
    
    finally:
        # Always close the session
        if session:
            session.close()
            logger.info("Database session closed")

def reset_hashtag_table(confirm=False):
    """
    Utility function to reset the hashtags table if needed.
    Requires explicit confirmation to prevent accidental data loss.
    
    Args:
        confirm (bool): Confirmation flag to proceed with truncation
    """
    if not confirm:
        logger.warning("Table reset not confirmed. Set confirm=True to proceed.")
        return
        
    Session = None
    session = None
    
    try:
        Session, _ = get_database_connection()
        session = Session()
        
        # Execute raw SQL to truncate the table
        session.execute(text("TRUNCATE TABLE hashtags RESTART IDENTITY CASCADE;"))
        session.commit()
        logger.info("Hashtags table has been reset successfully")
        
    except SQLAlchemyError as e:
        if session:
            session.rollback()
        logger.error(f"Error resetting hashtags table: {e}")
        
    finally:
        if session:
            session.close()

def upsert_hashtag_data(csv_path, batch_size=500):
    """
    Alternative approach: Use PostgreSQL's ON CONFLICT DO NOTHING syntax for upserts.
    This is more efficient than the individual record approach.
    
    Args:
        csv_path (str): Path to the CSV file
        batch_size (int): Number of records to process in each batch
    """
    Session = None
    session = None
    
    try:
        # Get database session
        Session, engine = get_database_connection()
        session = Session()
        
        # Load and validate CSV
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        logger.info(f"Loading hashtag data from {csv_path} with upsert strategy")
        df = pd.read_csv(csv_path)
        
        if df.empty:
            logger.warning("CSV file is empty. No hashtag data to import.")
            return
        
        # Preprocess data
        df = preprocess_hashtag_data(df)
        
        # Remove duplicates within the input data
        df = deduplicate_hashtags(df)
        
        total_rows = len(df)
        logger.info(f"Processing {total_rows} hashtag records with ON CONFLICT DO NOTHING")
        
        # Process in batches
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            # Convert DataFrame to list of dictionaries
            records = batch_df.to_dict(orient='records')
            
            try:
                # Use raw SQL with executemany for better performance
                insert_stmt = text("""
                    INSERT INTO hashtags (id, name, topic, description)
                    VALUES (:id, :name, :topic, :description)
                    ON CONFLICT (name) DO NOTHING
                """)
                
                with engine.begin() as conn:  # Auto commits/rollbacks
                    conn.execute(insert_stmt, records)
                
                logger.info(f"Processed hashtag batch with upsert: {i} to {min(i+batch_size, total_rows)}")
                
            except Exception as e:
                logger.error(f"Error in upsert batch {i}-{min(i+batch_size, total_rows)}: {e}")
        
        logger.info("Hashtag data upsert completed")
        
    except Exception as e:
        logger.error(f"Failed to upsert hashtag data: {e}")
    
    finally:
        # Always close the session
        if session:
            session.close()
            logger.info("Database session closed")

if __name__ == "__main__":
    try:
        # Example usage: Load data from the CSV
        csv_path = "./data/hashtag.csv"
        
        # Choose one of these methods:
        
        # Option 1: Standard load with duplicate checking
        load_hashtag_data(csv_path, batch_size=1000, skip_existing=True)
        
        # Option 2: PostgreSQL upsert approach (more efficient)
        # upsert_hashtag_data(csv_path, batch_size=1000)
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")