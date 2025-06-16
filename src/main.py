import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
from utils.logger import get_logger, LoggedOperation, log_api_request, log_data_stats, log_elasticsearch_operation
from utils.formatter import *
from utils.load_config import load_config
from src.axoniusApi import AxoniusAPI

# Get configured logger
logger = get_logger(__name__)

def main():
    """Main function to orchestrate the data pipeline"""
    
    logger.info("=" * 60)
    logger.info("Starting Axonius to Elasticsearch data pipeline")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config_loader = load_config()
        
        # Validate configuration
        if not config_loader.validate_config():
            logger.error("Configuration validation failed. Please check your config files.")
            return
        
        # Print configuration summary
        config_loader.print_config_summary()
        
        # Get configuration objects
        es_config = config_loader.get_elasticsearch_config()
        ax_config = config_loader.get_axonius_config()
        
        # Test ES connection first
        es_client = create_elasticsearch_client(es_config)
        test_elasticsearch_connection(es_client)
        
        # Step 1: Fetch and format data from Axonius
        logger.info("STEP 1: Fetching and formatting data from Axonius")
        formatted_data = get_and_format_axonius_data(ax_config)
        
        if not formatted_data:
            logger.error("No data retrieved or formatted from Axonius. Pipeline terminated.")
            return
        
        # Step 2: Transform data for Elasticsearch
        logger.info("STEP 2: Transforming data for Elasticsearch")
        elasticsearch_docs = transform_data_for_elasticsearch(formatted_data, es_config['index'])
        
        if not elasticsearch_docs:
            logger.error("No valid documents created for indexing. Pipeline terminated.")
            return
        
        log_data_stats(logger, "Documents prepared for indexing", len(elasticsearch_docs))
        
        # Step 3: Bulk index to Elasticsearch
        logger.info("STEP 3: Bulk indexing to Elasticsearch")
        success = bulk_index_to_elasticsearch(es_client, elasticsearch_docs, es_config['index'])
        
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


def create_elasticsearch_client(es_config: Dict[str, Any]) -> Elasticsearch:
    """Create Elasticsearch client from configuration"""
    
    with LoggedOperation(logger, "Elasticsearch client creation"):
        client_config = {
            'cloud_id': es_config['cloud_id'],
            'api_key': es_config['api_key'],
            'request_timeout': 60,
        }
        
        # Add URL if provided (for custom endpoints)
        if es_config.get('url'):
            client_config['hosts'] = [es_config['url']]
        
        return Elasticsearch(**client_config)


def test_elasticsearch_connection(es_client: Elasticsearch):
    """Test Elasticsearch connection"""
    
    with LoggedOperation(logger, "Elasticsearch connection test"):
        if es_client.ping():
            logger.info("Elasticsearch connection test successful")
        else:
            logger.error("Elasticsearch connection test failed")
            raise ConnectionError("Failed to connect to Elasticsearch")


def get_and_format_axonius_data(ax_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fetch and format data from Axonius using the context manager"""
    
    with LoggedOperation(logger, "Axonius data fetch and format"):
        with AxoniusAPI(ax_config, logger) as api:
            try:
                # Log the configuration being used
                api.log_configuration()
                
                # Check discovery status
                if not api.check_discovery_status():
                    logger.error("Discovery check failed - discovery has not succeeded")
                    return []
                
                logger.info("Discovery check successful, proceeding to fetch assets...")
                
                # Get all devices using configured fields and limits
                all_devices = api.get_all_assets('devices')  # Uses configured fields automatically
                log_data_stats(logger, "Raw assets retrieved", len(all_devices))
                
                # Format the data (you'll need to implement this based on your existing formatter)
                formatted_data = format_axonius_data(all_devices)
                log_data_stats(logger, "Formatted assets", len(formatted_data))
                
                return formatted_data
                
            except Exception as e:
                logger.error(f"Error fetching data from Axonius: {e}")
                return []


def create_index_if_not_exists(es_client: Elasticsearch, index_name: str):
    """Create the index with appropriate mapping if it doesn't exist"""
    
    with LoggedOperation(logger, f"Index creation check for {index_name}"):
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Index {index_name} does not exist, creating...")
            
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
            
            es_client.indices.create(index=index_name, body=mapping)
            log_elasticsearch_operation(logger, "index created", index_name)
        else:
            logger.info(f"Index {index_name} already exists")


def bulk_index_to_elasticsearch(es_client: Elasticsearch, documents: List[Dict[str, Any]], index_name: str) -> bool:
    """Bulk index documents to Elasticsearch"""
    
    if not documents:
        logger.warning("No documents provided for indexing")
        return False
    
    with LoggedOperation(logger, "Elasticsearch bulk indexing"):
        try:
            # Create index if it doesn't exist
            create_index_if_not_exists(es_client, index_name)
            
            # Bulk index documents
            log_data_stats(logger, "Starting bulk indexing", len(documents))
            
            success_count, failed_docs = bulk(
                es_client,
                documents,
                index=index_name,
                chunk_size=100,
                request_timeout=60
            )
            
            log_elasticsearch_operation(logger, "bulk indexing completed", index_name, success_count)
            
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


if __name__ == "__main__":
    main()