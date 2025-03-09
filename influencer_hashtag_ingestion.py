import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Load environment variables
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


# Database connection setup
SQLALCHEMY_DATABASE_URI = (
        os.environ.get('DATABASE_URL') or
        f"postgresql+psycopg2://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}"
    )


engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

def load_influencer_hashtag_data(csv_path):
    """Insert influencer-hashtag associations with validation."""
    # Load the CSV
    df = pd.read_csv(csv_path, usecols=['influencer_id', 'hashtag_id', 'count'])
    
    # Get existing hashtag and influencer IDs from the database
    existing_hashtags = pd.read_sql('SELECT id FROM hashtags', engine)['id'].tolist()
    existing_influencers = pd.read_sql('SELECT id FROM influencers', engine)['id'].tolist()
    
    # Filter the DataFrame to only include valid relationships
    valid_data = df[
        (df['hashtag_id'].isin(existing_hashtags)) & 
        (df['influencer_id'].isin(existing_influencers))
    ]
    
    # Report statistics
    total_records = len(df)
    valid_records = len(valid_data)
    skipped_records = total_records - valid_records
    
    print(f"\nValidation Summary:")
    print(f"Total records in CSV: {total_records}")
    print(f"Valid records to insert: {valid_records}")
    print(f"Skipped records: {skipped_records}")
    
    # Convert valid data to list of dictionaries
    data = [
        {'influencer_id': row.influencer_id, 'hashtag_id': row.hashtag_id, 'count': row.count}
        for row in valid_data.itertuples(index=False)
    ]
    
    try:
        # Insert in batches to avoid memory issues
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            insert_stmt = text("""
                INSERT INTO influencer_hashtag (influencer_id, hashtag_id, usage_count) 
                VALUES (:influencer_id, :hashtag_id, :count)
                ON CONFLICT (influencer_id, hashtag_id) DO UPDATE 
                SET usage_count = EXCLUDED.usage_count
            """)
            
            session.execute(insert_stmt, batch)
            session.commit()
            print(f"Inserted batch {i//batch_size + 1} of {(len(data) + batch_size - 1)//batch_size}")
        
        print("\nData import completed successfully!")
        
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # Optional: Clear existing relationships first
    # try:
    #     session.execute(text("TRUNCATE TABLE influencer_hashtag RESTART IDENTITY CASCADE;"))
    #     session.commit()
    #     print("Cleared existing relationships")
    # except Exception as e:
    #     print(f"Error clearing table: {e}")
    #     session.rollback()
    
    # Load new relationships
    csv_path = "./data/hashtag_influencer.csv"
    load_influencer_hashtag_data(csv_path)