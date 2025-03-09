import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Database URIs
LOCAL_URI = "postgresql://postgres:123@localhost/creatorain"
GCP_URI = "postgresql://postgres:123@34.29.76.27:5432/creatorain"

def get_table_count(engine, table):
    try:
        result = pd.read_sql(f'SELECT COUNT(*) FROM {table}', engine)
        return result.iloc[0,0]
    except Exception as e:
        print(f"Error getting count for {table}: {e}")
        return 0

def deploy_database():
    # Create engines
    local_engine = create_engine(LOCAL_URI)
    gcp_engine = create_engine(GCP_URI)
    
    # Tables in order of dependencies
    tables = [
        'brands',
        'influencers',
        'hashtags',
        'influencer_brand',
        'influencer_hashtag'
    ]
    
    print("Starting deployment to GCP...")
    
    try:
        # First, create tables on GCP using SQLAlchemy models
        from app import create_app, db
        app = create_app()
        with app.app_context():
            db.create_all()
        print("Created tables on GCP")
        
        # Now transfer data
        for table in tables:
            print(f"\nProcessing {table}...")
            
            # Get local record count
            local_count = get_table_count(local_engine, table)
            print(f"Local records: {local_count}")
            
            if local_count > 0:
                # Process in batches for large tables
                batch_size = 10000
                for offset in range(0, local_count, batch_size):
                    query = f'SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}'
                    df = pd.read_sql(query, local_engine)
                    df.to_sql(table, gcp_engine, if_exists='append' if offset > 0 else 'replace', 
                             index=False)
                    print(f"Transferred batch {offset//batch_size + 1} of {(local_count + batch_size - 1)//batch_size}")
                
                # Verify count
                gcp_count = get_table_count(gcp_engine, table)
                print(f"GCP records: {gcp_count}")
                if local_count != gcp_count:
                    print(f"⚠️ Count mismatch for {table}!")
            else:
                print(f"Skipping {table} - no local data")
            
        print("\nDeployment completed successfully!")
        
    except Exception as e:
        print(f"Error during deployment: {e}")

def check_local_tables():
    """Check which tables exist and have data locally"""
    engine = create_engine(LOCAL_URI)
    inspector = inspect(engine)
    
    print("\nLocal database status:")
    for table in ['brands', 'influencers', 'hashtags', 'influencer_brand', 'influencer_hashtag']:
        exists = table in inspector.get_table_names()
        count = get_table_count(engine, table) if exists else 0
        print(f"{table}: {'✅' if exists else '❌'} Exists, {count} records")

if __name__ == "__main__":
    print("Checking local database status first...")
    check_local_tables()
    
    response = input("\nContinue with deployment? (y/n): ")
    if response.lower() == 'y':
        deploy_database()