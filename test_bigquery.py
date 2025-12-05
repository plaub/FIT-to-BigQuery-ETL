"""
Connectivity test for BigQuery.
Tests authentication and connection to BigQuery without processing files.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import (
    GOOGLE_APPLICATION_CREDENTIALS, BIGQUERY_PROJECT_ID, 
    BIGQUERY_DATASET, validate_config
)
from src.bigquery_client import BigQueryClient


def test_bigquery_connection():
    """Test BigQuery connection and setup."""
    print("=" * 80)
    print("BigQuery Connection Test")
    print("=" * 80)
    print()
    
    # Step 1: Validate configuration
    print("1. Validating configuration...")
    try:
        validate_config()
        print("   ✅ Configuration valid")
        print(f"   • Project: {BIGQUERY_PROJECT_ID}")
        print(f"   • Dataset: {BIGQUERY_DATASET}")
        print(f"   • Credentials: {GOOGLE_APPLICATION_CREDENTIALS}")
        print()
    except ValueError as e:
        print(f"   ❌ Configuration error: {e}")
        print()
        print("Please run 'python setup.py' to configure your environment.")
        return False
    
    # Step 2: Test BigQuery client initialization
    print("2. Initializing BigQuery client...")
    try:
        bq_client = BigQueryClient(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET)
        print("   ✅ Client initialized successfully")
        print()
    except Exception as e:
        print(f"   ❌ Failed to initialize client: {e}")
        print()
        print("Please check:")
        print("  • Service Account JSON key file exists")
        print("  • GOOGLE_APPLICATION_CREDENTIALS path is correct")
        print("  • Service Account has necessary permissions")
        return False
    
    # Step 3: Test dataset access/creation
    print("3. Testing dataset access...")
    try:
        bq_client.ensure_dataset_exists()
        print(f"   ✅ Dataset '{BIGQUERY_DATASET}' accessible")
        print()
    except Exception as e:
        print(f"   ❌ Dataset error: {e}")
        print()
        print("Please check:")
        print("  • Service Account has BigQuery Admin or Editor role")
        print("  • Project ID is correct")
        return False
    
    # Step 4: Test query capability
    print("4. Testing query capability...")
    try:
        query = f"""
            SELECT 
                table_name,
                row_count,
                size_bytes
            FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.__TABLES__`
        """
        results = bq_client.query(query).result()
        
        tables = list(results)
        print(f"   ✅ Query successful")
        print(f"   • Tables in dataset: {len(tables)}")
        
        if tables:
            print()
            print("   Existing tables:")
            for row in tables:
                size_mb = row.size_bytes / (1024 * 1024) if row.size_bytes else 0
                print(f"     - {row.table_name}: {row.row_count:,} rows, {size_mb:.2f} MB")
        print()
    except Exception as e:
        # Dataset might be empty, which is fine
        if "Not found" in str(e):
            print(f"   ✅ Query works (dataset is empty)")
            print()
        else:
            print(f"   ⚠️  Query warning: {e}")
            print()
    
    # Step 5: Summary
    print("=" * 80)
    print("✅ All Tests Passed!")
    print("=" * 80)
    print()
    print("Your BigQuery connection is working correctly.")
    print()
    print("Next steps:")
    print("  1. Place FIT files in 'files/' directory")
    print("  2. Run: python run_etl.py")
    print()
    
    return True


if __name__ == "__main__":
    try:
        success = test_bigquery_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
