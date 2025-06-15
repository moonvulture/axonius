import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
from utils.logger import get_logger, LoggedOperation, log_api_request, log_data_stats, log_elasticsearch_operation
from utils.formatter import *

# Get configured logger
logger = get_logger(__name__)


def get_and_format_axonius_data() -> List[Dict[str, Any]]:
    """Fetch and format host data from Axonius API"""
    
    with LoggedOperation(logger, "Axonius data fetch and format"):
        base_url = f"https://{AXONIUS_CONFIG['instance_url']}/api/v2"
        headers = {
            'accept': 'application/json',
            'api-key': AXONIUS_CONFIG['api_key'],
            'api-secret': AXONIUS_CONFIG['api_secret'],
        }
        
        try:
            # Check discovery status
            logger.info("Checking Axonius discovery status...")
            discovery_url = f'{base_url}/discovery'
            response = requests.get(url=discovery_url, headers=headers)
            log_api_request(logger, "GET", discovery_url, response.status_code)
            response.raise_for_status()
            
            discovery_result = response.json()
            
            if not discovery_result.get('has_succeeded'):
                logger.error("Discovery check failed - discovery has not succeeded")
                return []
            
            logger.info("Discovery check successful, proceeding to fetch assets...")
            
            # Add content-type for asset request
            headers['content-type'] = 'application/json'
            
            # Request parameters - fetch more data by increasing limit
            body_params = {
                'include_metadata': True,
                'page': {
                    'limit': 100,  # Increased limit for more data
                    'offset': 0
                },
                'use_cache_entry': True,
                'return_plain_data': True,
                'fields': [
                    'specific_data.data.hostname',
                    'specific_data.data.network_interfaces.ips_preferred',
                    'specific_data.data.network_interfaces.mac_preferred',
                    'specific_data.data.last_seen',  # Added last seen field
                    'adapters_data.axonius_adapter.last_seen',  # Alternative last seen field
                ],
            }
            
            # Get device assets
            assets_url = f'{base_url}/assets/devices'
            logger.info(f"Requesting assets with limit: {body_params['page']['limit']}")
            
            response = requests.get(
                url=assets_url,
                headers=headers,
                json=body_params,
            )
            log_api_request(logger, "GET", assets_url, response.status_code)
            response.raise_for_status()
            
            assets_result = response.json()
            raw_assets = assets_result.get('assets', [])
            log_data_stats(logger, "Raw assets retrieved", len(raw_assets))
            
            # Format the data immediately after fetching
            formatted_assets = []
            
            for i, asset in enumerate(raw_assets):
                try:
                    formatted_asset = {}
                    
                    # Extract and format basic fields
                    formatted_asset['hostname'] = normalize_hostname(asset.get('specific_data.data.hostname'))
                    formatted_asset['ip_addresses'] = normalize_ip_addresses(asset.get('specific_data.data.network_interfaces.ips_preferred'))
                    formatted_asset['mac_addresses'] = normalize_mac_addresses(asset.get('specific_data.data.network_interfaces.mac_preferred'))
                    
                    # Extract and format last seen date to ECS format
                    formatted_asset['last_seen'] = format_last_seen_date(
                        asset.get('specific_data.data.last_seen') or 
                        asset.get('adapters_data.axonius_adapter.last_seen')
                    )
                    
                    # Add original asset data for reference if needed
                    formatted_asset['_original'] = asset
                    
                    formatted_assets.append(formatted_asset)
                    
                except Exception as e:
                    logger.warning(f"Failed to format asset {i}: {e}")
                    continue
            
            log_data_stats(logger, "Assets formatted for ECS compatibility", len(formatted_assets))
            return formatted_assets
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while fetching data from Axonius: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during Axonius data fetch: {e}")
            return []


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