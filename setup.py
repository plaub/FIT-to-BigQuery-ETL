"""
Setup script to configure the ETL pipeline.
Run this script after installation to set up your environment.
"""

import os
from pathlib import Path


def setup_environment():
    """Interactive setup for environment configuration."""
    print("=" * 80)
    print("FIT to BigQuery ETL Pipeline - Setup")
    print("=" * 80)
    print()
    
    # Check if .env already exists
    env_file = Path(".env")
    if env_file.exists():
        print("⚠️  .env file already exists!")
        response = input("Do you want to overwrite it? (y/n): ").lower()
        if response != 'y':
            print("Setup cancelled.")
            return
    
    print("Please provide the following configuration:")
    print()
    
    # Google Cloud Configuration
    print("📋 Google Cloud Configuration")
    print("-" * 40)
    
    credentials_path = input("Service Account JSON key path: ").strip()
    project_id = input("BigQuery Project ID: ").strip()
    dataset = input("BigQuery Dataset (default: fitness_data): ").strip() or "fitness_data"
    
    print()
    print("📁 Directory Configuration (press Enter for defaults)")
    print("-" * 40)
    
    input_dir = input("Input directory (default: files): ").strip() or "files"
    processed_dir = input("Processed directory (default: processed): ").strip() or "processed"
    failed_dir = input("Failed directory (default: failed): ").strip() or "failed"
    log_dir = input("Log directory (default: logs): ").strip() or "logs"
    
    print()
    print("⚙️  Processing Configuration (press Enter for defaults)")
    print("-" * 40)
    
    batch_size = input("Batch size (default: 1000): ").strip() or "1000"
    log_level = input("Log level (DEBUG/INFO/WARNING/ERROR, default: INFO): ").strip() or "INFO"
    
    # Create .env file
    env_content = f"""# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS={credentials_path}
BIGQUERY_PROJECT_ID={project_id}
BIGQUERY_DATASET={dataset}

# Directory Configuration
INPUT_DIR={input_dir}
PROCESSED_DIR={processed_dir}
FAILED_DIR={failed_dir}
LOG_DIR={log_dir}

# Processing Configuration
BATCH_SIZE={batch_size}
LOG_LEVEL={log_level}
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print()
    print("✅ Configuration saved to .env")
    print()
    print("=" * 80)
    print("Setup Complete!")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Verify your Service Account has BigQuery permissions")
    print("2. Place FIT files in the 'files/' directory")
    print("3. Run the pipeline: python run_etl.py")
    print()


if __name__ == "__main__":
    try:
        setup_environment()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user.")
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
