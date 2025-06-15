import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
from utils.logger import get_logger, LoggedOperation, log_api_request, log_data_stats, log_elasticsearch_operation
from utils.formatter import *
import from axoniusApi import AxoniusAPI
# Get configured logger
logger = get_logger(__name__)

def get_user_data():
    """Example: Get user data for security analysis"""
    with AxoniusAPI(AXONIUS_CONFIG, logger) as api:
        return api.get_all_assets('users')
    
def updated_get_and_format_axonius_data() -> List[Dict[str, Any]]:
    """Updated version of your existing function using the context manager"""
    
    with LoggedOperation(logger, "Axonius data fetch and format"):
        with AxoniusAPI(AXONIUS_CONFIG, logger) as api:
            try:
                # Check discovery status
                if not api.check_discovery_status():
                    logger.error("Discovery check failed - discovery has not succeeded")
                    return []
                
                logger.info("Discovery check successful, proceeding to fetch assets...")
                
                # Get assets with your specific fields
                device_fields = [
                    'specific_data.data.hostname',
                    'specific_data.data.network_interfaces.ips_preferred',
                    'specific_data.data.network_interfaces.mac_preferred',
                    'specific_data.data.last_seen',
                    'adapters_data.axonius_adapter.last_seen',
                ]
                
                # Get all devices (with automatic pagination if needed)
                all_devices = api.get_all_assets('devices', fields=device_fields, max_records=1000)
                log_data_stats(logger, "Raw assets retrieved", len(all_devices))
    

def create_index_if_not_exists(es_client: Elasticsearch):
    """Create the index with appropriate mapping if it doesn't exist"""
    
    with LoggedOperation(logger, f"Index creation check for {INDEX_NAME}"):
        if not es_client.indices.exists(index=INDEX_NAME):
            logger.info(f"Index {INDEX_NAME} does not exist, creating...")
            
            # Define mapping for the host data with ECS fields
            mapping = {
                "mappings": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "host": {
                            "properties": {
                                "ip": {
                                    "type": "ip"
                                },
                                "mac": {
                                    "type": "keyword"
                                },
                                "hostname": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "network": {
                            "properties": {
                                "ip_addresses": {
                                    "type": "ip"
                                },
                                "mac_addresses": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "axonius": {
                            "properties": {
                                "last_seen": {
                                    "type": "date"
                                },
                                "ingestion_time": {
                                    "type": "date"
                                }
                            }
                        }
                    }
                }
            }
            
            es_client.indices.create(index=INDEX_NAME, body=mapping)
            log_elasticsearch_operation(logger, "index created", INDEX_NAME)
        else:
            logger.info(f"Index {INDEX_NAME} already exists")


def bulk_index_to_elasticsearch(documents: List[Dict[str, Any]]) -> bool:
    """Bulk index documents to Elasticsearch"""
    
    if not documents:
        logger.warning("No documents provided for indexing")
        return False
    
    with LoggedOperation(logger, "Elasticsearch bulk indexing"):
        try:
            # Create Elasticsearch client
            es_client = Elasticsearch(**ELASTICSEARCH_CONFIG)
            
            # Test connection
            logger.info("Testing Elasticsearch connection...")
            if not es_client.ping():
                logger.error("Cannot connect to Elasticsearch - ping failed")
                return False
            
            logger.info("Elasticsearch connection successful")
            
            # Create index if it doesn't exist
            create_index_if_not_exists(es_client)
            
            # Bulk index documents
            log_data_stats(logger, "Starting bulk indexing", len(documents))
            
            success_count, failed_docs = bulk(
                es_client,
                documents,
                index=INDEX_NAME,
                chunk_size=100,
                request_timeout=60
            )
            
            log_elasticsearch_operation(logger, "bulk indexing completed", INDEX_NAME, success_count)
            
            if failed_docs:
                logger.warning(f"Failed to index {len(failed_docs)} documents")
                for i, failure in enumerate(failed_docs[:5]):  # Log first 5 failures
                    logger.error(f"Failed document {i+1}: {failure}")
                if len(failed_docs) > 5:
                    logger.error(f"... and {len(failed_docs) - 5} more failures")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during bulk indexing: {e}")
            return False


def es_conn():
    """Test Elasticsearch connection"""
    with LoggedOperation(logger, "Elasticsearch connection test"):
        es = Elasticsearch(
            api_key=ES_API_KEY
        )
        
        if es.ping():
            logger.info("Elasticsearch connection test successful")
        else:
            logger.error("Elasticsearch connection test failed")
        
        return es


def main():
    """Main function to orchestrate the data pipeline"""
    
    logger.info("=" * 60)
    logger.info("Starting Axonius to Elasticsearch data pipeline")
    logger.info("=" * 60)
    
    try:
        # Test ES connection first
        es_conn()
        
        # Step 1: Fetch and format data from Axonius
        logger.info("STEP 1: Fetching and formatting data from Axonius")
        formatted_data = get_and_format_axonius_data()
        
        if not formatted_data:
            logger.error("No data retrieved or formatted from Axonius. Pipeline terminated.")
            return
        
        # Step 2: Transform data for Elasticsearch
        logger.info("STEP 2: Transforming data for Elasticsearch")
        elasticsearch_docs = transform_data_for_elasticsearch(formatted_data)
        
        if not elasticsearch_docs:
            logger.error("No valid documents created for indexing. Pipeline terminated.")
            return
        
        log_data_stats(logger, "Documents prepared for indexing", len(elasticsearch_docs))
        
        # Step 3: Bulk index to Elasticsearch
        logger.info("STEP 3: Bulk indexing to Elasticsearch")
        success = bulk_index_to_elasticsearch(elasticsearch_docs)
        
        if success:
            logger.info("=" * 60)
            logger.info("Data pipeline completed successfully!")
            logger.info("=" * 60)
        else:
            logger.error("=" * 60)
            logger.error("Data pipeline failed during indexing")
            logger.error("=" * 60)
            
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()