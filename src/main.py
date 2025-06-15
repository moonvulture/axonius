import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any
import logging
from utils.formatter import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_and_format_axonius_data() -> List[Dict[str, Any]]:
    """Fetch and format host data from Axonius API"""
    
    base_url = f"https://{AXONIUS_CONFIG['instance_url']}/api/v2"
    headers = {
        'accept': 'application/json',
        'api-key': AXONIUS_CONFIG['api_key'],
        'api-secret': AXONIUS_CONFIG['api_secret'],
    }
    
    try:
        # Check discovery status
        logger.info("Checking Axonius discovery status...")
        response = requests.get(url=f'{base_url}/discovery', headers=headers)
        response.raise_for_status()
        discovery_result = response.json()
        
        if not discovery_result.get('has_succeeded'):
            logger.error("Discovery check failed")
            return []
        
        logger.info("Discovery check successful, fetching assets...")
        
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
        response = requests.get(
            url=f'{base_url}/assets/devices',
            headers=headers,
            json=body_params,
        )
        response.raise_for_status()
        assets_result = response.json()
        
        raw_assets = assets_result.get('assets', [])
        logger.info(f"Retrieved {len(raw_assets)} assets from Axonius")
        
        # Format the data immediately after fetching
        formatted_assets = []
        
        for asset in raw_assets:
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
        
        logger.info(f"Formatted {len(formatted_assets)} assets for ECS compatibility")
        return formatted_assets
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Axonius: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []

def create_index_if_not_exists(es_client: Elasticsearch):
    """Create the index with appropriate mapping if it doesn't exist"""
    
    if not es_client.indices.exists(index=INDEX_NAME):
        logger.info(f"Creating index: {INDEX_NAME}")
        
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
        logger.info(f"Index {INDEX_NAME} created successfully")
    else:
        logger.info(f"Index {INDEX_NAME} already exists")

def bulk_index_to_elasticsearch(documents: List[Dict[str, Any]]) -> bool:
    """Bulk index documents to Elasticsearch"""
    
    if not documents:
        logger.warning("No documents to index")
        return False
    
    try:
        # Create Elasticsearch client
        es_client = Elasticsearch(**ELASTICSEARCH_CONFIG)
        
        # Test connection
        if not es_client.ping():
            logger.error("Cannot connect to Elasticsearch")
            return False
        
        logger.info("Connected to Elasticsearch successfully")
        
        # Create index if it doesn't exist
        create_index_if_not_exists(es_client)
        
        # Bulk index documents
        logger.info(f"Starting bulk indexing of {len(documents)} documents...")
        
        success_count, failed_docs = bulk(
            es_client,
            documents,
            index=INDEX_NAME,
            chunk_size=100,
            request_timeout=60
        )
        
        logger.info(f"Successfully indexed {success_count} documents")
        
        if failed_docs:
            logger.warning(f"Failed to index {len(failed_docs)} documents")
            for failure in failed_docs:
                logger.error(f"Failed document: {failure}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during bulk indexing: {e}")
        return False

def es_conn():
    es = Elasticsearch(
        api_key=ES_API_KEY
    )
    return es


def main():
    """Main function to orchestrate the data pipeline"""
    
    logger.info("Starting Axonius to Elasticsearch data pipeline...")
    
    es_conn()
    # Step 1: Fetch and format data from Axonius
    logger.info("Step 1: Fetching and formatting data from Axonius...")
    formatted_data = get_and_format_axonius_data()
    
    if not formatted_data:
        logger.error("No data retrieved or formatted from Axonius. Exiting.")
        return
    
    # Step 2: Transform data for Elasticsearch
    logger.info("Step 2: Transforming data for Elasticsearch...")
    elasticsearch_docs = transform_data_for_elasticsearch(formatted_data)
    
    if not elasticsearch_docs:
        logger.error("No valid documents created for indexing. Exiting.")
        return
    
    # Step 3: Bulk index to Elasticsearch
    logger.info("Step 3: Bulk indexing to Elasticsearch...")
    success = bulk_index_to_elasticsearch(elasticsearch_docs)
    
    if success:
        logger.info("Data pipeline completed successfully!")
    else:
        logger.error("Data pipeline failed during indexing.")

if __name__ == "__main__":
    main()