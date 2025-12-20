"""
BigQuery client for managing table operations and data uploads.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


def serialize_datetime(obj: Any) -> Any:
    """
    Convert datetime objects to ISO format strings for JSON serialization.
    
    Args:
        obj: Object to serialize (can be dict, list, datetime, or other)
        
    Returns:
        Serialized object with datetime converted to ISO strings
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime(item) for item in obj]
    else:
        return obj


class BigQueryClient:
    """Client for BigQuery operations."""
    
    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize BigQuery client.
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
        """
        self.client = bigquery.Client(project=project_id)
        self.project = project_id
        self.dataset_id = dataset_id
        self.dataset_ref = self.client.dataset(dataset_id)
        
        logger.info(f"Initialized BigQuery client for {project_id}.{dataset_id}")
    
    def ensure_dataset_exists(self):
        """Create dataset if it doesn't exist."""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_id} exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def ensure_table_exists(self, table_id: str, schema: List[Dict[str, str]], 
                           partition_field: str = None, clustering_fields: List[str] = None):
        """
        Create table if it doesn't exist with specified schema.
        
        Args:
            table_id: Table name
            schema: List of field definitions
            partition_field: Field to partition by (optional)
            clustering_fields: Fields to cluster by (optional)
        """
        table_ref = self.dataset_ref.table(table_id)
        
        try:
            table = self.client.get_table(table_ref)
            logger.info(f"Table {table_id} exists. Checking for missing fields...")
            
            # Check for current fields and add missing ones
            current_fields = {f.name for f in table.schema}
            new_fields = []
            for field in schema:
                if field['name'] not in current_fields:
                    new_fields.append(bigquery.SchemaField(
                        field['name'], field['type'], mode=field.get('mode', 'NULLABLE')
                    ))
            
            if new_fields:
                logger.info(f"Updating table {table_id} with {len(new_fields)} new fields: {[f.name for f in new_fields]}")
                updated_schema = list(table.schema) + new_fields
                table.schema = updated_schema
                self.client.update_table(table, ["schema"])
                logger.info(f"Successfully updated schema for {table_id}")
            else:
                logger.info(f"No schema changes needed for {table_id}")
            return
        except NotFound:
            pass
        
        # Convert schema to BigQuery schema format
        bq_schema = []
        for field in schema:
            field_type = field['type']
            mode = field.get('mode', 'NULLABLE')
            bq_schema.append(bigquery.SchemaField(field['name'], field_type, mode=mode))
        
        table = bigquery.Table(table_ref, schema=bq_schema)
        
        # Set up partitioning
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        
        # Set up clustering
        if clustering_fields:
            table.clustering_fields = clustering_fields
        
        table = self.client.create_table(table)
        logger.info(f"Created table {table_id} with {len(bq_schema)} fields")
    
    def insert_rows_batch(self, table_id: str, rows: List[Dict[str, Any]], 
                         batch_size: int = 1000) -> int:
        """
        Insert rows in batches using streaming insert.
        
        Args:
            table_id: Table name
            rows: List of row dictionaries
            batch_size: Number of rows per batch
            
        Returns:
            Number of successfully inserted rows
        """
        if not rows:
            logger.warning(f"No rows to insert into {table_id}")
            return 0
        
        table_ref = self.dataset_ref.table(table_id)
        total_inserted = 0
        errors = []
        
        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            # Serialize datetime objects to ISO format strings
            serialized_batch = [serialize_datetime(row) for row in batch]
            
            try:
                insert_errors = self.client.insert_rows_json(table_ref, serialized_batch)
                
                if insert_errors:
                    logger.error(f"Errors inserting batch {i//batch_size + 1}: {insert_errors}")
                    errors.extend(insert_errors)
                else:
                    total_inserted += len(batch)
                    logger.debug(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
            
            except Exception as e:
                logger.error(f"Exception inserting batch {i//batch_size + 1}: {e}")
                raise
        
        if errors:
            logger.error(f"Total insertion errors: {len(errors)}")
            raise Exception(f"Failed to insert {len(errors)} rows into {table_id}")
        
        logger.info(f"Successfully inserted {total_inserted} rows into {table_id}")
        return total_inserted
    
    def upload_session_and_records(self, session_data: Dict[str, Any], 
                                   records_data: List[Dict[str, Any]], 
                                   sessions_table: str, details_table: str,
                                   batch_size: int = 1000):
        """
        Upload session and records data in a transactional manner.
        
        First uploads details (many rows), then session (1 row).
        If either fails, raises exception.
        
        Args:
            session_data: Session summary data
            records_data: List of record data
            sessions_table: Sessions table name
            details_table: Details table name
            batch_size: Batch size for records insert
        """
        try:
            # Step 1: Insert details (time-series data)
            logger.info(f"Uploading {len(records_data)} records to {details_table}")
            self.insert_rows_batch(details_table, records_data, batch_size)
            
            # Step 2: Insert session (summary data)
            logger.info(f"Uploading session to {sessions_table}")
            self.insert_rows_batch(sessions_table, [session_data], batch_size=1)
            
            logger.info(f"Successfully uploaded session {session_data['session_id']}")
        
        except Exception as e:
            logger.error(f"Failed to upload session {session_data['session_id']}: {e}")
            raise
    
    def query(self, sql: str):
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query job result
        """
        return self.client.query(sql)
