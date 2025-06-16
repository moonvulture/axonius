import requests
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
import logging

class AxoniusAPI:
    """
    Flexible context manager for Axonius API interactions
    Handles authentication, session management, and provides methods for different endpoints
    """
    def __init__(self, config: Dict[str, str], logger: Optional[logging.Logger] = None):
        """
        Initialize Axonius API client
        
        Args:
            config: Dictionary with configuration including 'instance_url', 'api_key', 'api_secret', 
                   'device_fields', 'user_fields', 'batch_size', 'max_records', 'request_timeout'
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.session = None
        self.base_url = f"https://{config['instance_url']}/api/v2"
        
        # Store configured fields and settings
        self.device_fields = config.get('device_fields', [])
        self.user_fields = config.get('user_fields', [])
        self.batch_size = int(config.get('batch_size', 100))
        self.max_records = int(config.get('max_records', 1000))
        self.request_timeout = int(config.get('request_timeout', 60))

    def __enter__(self):
        """Setup authenticated session"""
        self.logger.debug("Establishing Axonius API session...")
        
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'accept': 'application/json',
            'api-key': self.config['api_key'],
            'api-secret': self.config['api_secret'],
        })
        
        # Test authentication by checking discovery endpoint
        try:
            response = self.session.get(f'{self.base_url}/discovery', timeout=self.request_timeout)
            response.raise_for_status()
            self.logger.debug("Axonius API authentication successful")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to authenticate with Axonius API: {e}")
            raise
        
        return self
  
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup session"""
        if self.session:
            self.session.close()
            self.logger.debug("Axonius API session closed")
        return False
 
    def check_discovery_status(self) -> bool:
        """
        Check if discovery has been successful
        
        Returns:
            True if discovery is successful, False otherwise
        """
        try:
            assert self.session is not None
            response = self.session.get(f'{self.base_url}/discovery', timeout=self.request_timeout)
            response.raise_for_status()
            
            discovery_data = response.json()
            # You may need to adjust this logic based on your Axonius API response format
            status = discovery_data.get('status', 'unknown')
            
            self.logger.info(f"Discovery status: {status}")
            return status.lower() in ['success', 'completed', 'ok']
            
        except Exception as e:
            self.logger.error(f"Failed to check discovery status: {e}")
            return False

    def get_assets(self, 
                   asset_type: str = 'devices',
                   fields: Optional[List[str]] = None,
                   limit: Optional[int] = None,
                   offset: int = 0,
                   additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get assets from Axonius
        
        Args:
            asset_type: 'devices', 'users', 'networks', etc.
            fields: List of fields to retrieve (overrides config defaults)
            limit: Number of records to retrieve (overrides config default)
            offset: Offset for pagination
            additional_params: Any additional parameters for the request
        
        Returns:
            API response as dictionary
        """
        # Use provided fields or get from config based on asset type
        if asset_type == 'devices':
            fields = self.device_fields
        elif asset_type == 'users':
            fields = self.user_fields
        else:
            fields = []
        
        # Use provided limit or get from config
        if limit is None:
            limit = self.batch_size
        
        # Build request parameters
        params = {
            'include_metadata': True,
            'page': {
                'limit': limit,
                'offset': offset
            },
            'use_cache_entry': True,
            'return_plain_data': True,
            'fields': fields,
        }
        
        # Add any additional parameters
        if additional_params:
            params.update(additional_params)
        
        # Make request
        url = f'{self.base_url}/assets/{asset_type}'
        
        # Set content-type for POST-like requests
        self.session.headers['content-type'] = 'application/json'
        
        self.logger.info(f"Requesting {asset_type} assets (limit: {limit}, offset: {offset}, fields: {len(fields)})")
        self.logger.debug(f"Fields requested: {fields}")
        
        response = self.session.get(url, json=params, timeout=self.request_timeout)
        response.raise_for_status()
        
        result = response.json()
        assets = result.get('assets', [])
        self.logger.info(f"Retrieved {len(assets)} {asset_type} assets")
        
        return result

    def get_all_assets(self, 
                      asset_type: str = 'devices',
                      fields: Optional[List[str]] = None,
                      max_records: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all assets with automatic pagination
        
        Args:
            asset_type: 'devices', 'users', 'networks', etc.
            fields: List of fields to retrieve (overrides config defaults)
            max_records: Maximum number of records to retrieve (overrides config default)
        
        Returns:
            List of all assets
        """
        if max_records is None:
            max_records = self.max_records
            
        all_assets = []
        offset = 0
        
        self.logger.info(f"Starting to fetch all {asset_type} assets (max: {max_records})")
        
        while len(all_assets) < max_records:
            # Calculate how many more records we need
            remaining = max_records - len(all_assets)
            current_limit = min(self.batch_size, remaining)
            
            try:
                result = self.get_assets(
                    asset_type=asset_type,
                    fields=fields,
                    limit=current_limit,
                    offset=offset
                )
                
                assets = result.get('assets', [])
                
                if not assets:
                    self.logger.info("No more assets found, stopping pagination")
                    break
                
                all_assets.extend(assets)
                offset += len(assets)
                
                self.logger.info(f"Fetched {len(all_assets)} {asset_type} assets so far...")
                
                # Check if we got fewer assets than requested (indicates end of data)
                if len(assets) < current_limit:
                    self.logger.info("Received fewer assets than requested, reached end of data")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error during pagination at offset {offset}: {e}")
                break
        
        self.logger.info(f"Completed fetching {len(all_assets)} {asset_type} assets")
        return all_assets

    def get_asset_count(self, asset_type: str = 'devices') -> int:
        """
        Get total count of assets without retrieving all data
        
        Args:
            asset_type: 'devices', 'users', 'networks', etc.
            
        Returns:
            Total count of assets
        """
        try:
            # Get just one record to check metadata
            result = self.get_assets(asset_type=asset_type, limit=1)
            
            # Extract total count from metadata if available
            metadata = result.get('page', {})
            total_count = metadata.get('totalResources', 0)
            
            self.logger.info(f"Total {asset_type} count: {total_count}")
            return total_count
            
        except Exception as e:
            self.logger.error(f"Failed to get {asset_type} count: {e}")
            return 0

    def get_configured_fields(self, asset_type: str) -> List[str]:
        """
        Get the configured fields for a specific asset type
        
        Args:
            asset_type: 'devices', 'users', etc.
            
        Returns:
            List of configured fields for the asset type
        """
        if asset_type == 'devices':
            return self.device_fields.copy()
        elif asset_type == 'users':
            return self.user_fields.copy()
        else:
            return []

    def log_configuration(self):
        """Log the current configuration for debugging"""
        self.logger.info("Axonius API Configuration:")
        self.logger.info(f"  Base URL: {self.base_url}")
        self.logger.info(f"  Device fields ({len(self.device_fields)}): {self.device_fields}")
        self.logger.info(f"  User fields ({len(self.user_fields)}): {self.user_fields}")
        self.logger.info(f"  Batch size: {self.batch_size}")
        self.logger.info(f"  Max records: {self.max_records}")
        self.logger.info(f"  Request timeout: {self.request_timeout}s")