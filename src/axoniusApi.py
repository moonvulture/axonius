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
            config: Dictionary with 'instance_url', 'api_key', 'api_secret'
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.session = None
        self.base_url = f"https://{config['instance_url']}/api/v2"

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
            response = self.session.get(f'{self.base_url}/discovery')
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
 
  
    def get_assets(self, 
                   asset_type: str = 'devices',
                   fields: Optional[List[str]] = None,
                   limit: int = 100,
                   offset: int = 0,
                   additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get assets from Axonius
        
        Args:
            asset_type: 'devices', 'users', 'networks', etc.
            fields: List of fields to retrieve
            limit: Number of records to retrieve
            offset: Offset for pagination
            additional_params: Any additional parameters for the request
        
        Returns:
            API response as dictionary
        """
        # Default fields for different asset types
        default_fields = {
            'devices': [
                'specific_data.data.hostname',
                'specific_data.data.network_interfaces.ips_preferred',
                'specific_data.data.network_interfaces.mac_preferred',
                'specific_data.data.last_seen',
                'adapters_data.axonius_adapter.last_seen',
            ],
            'users': [
                'specific_data.data.username',
                'specific_data.data.email',
                'specific_data.data.last_logon',
                'specific_data.data.domain',
            ],
            'networks': [
                'specific_data.data.network_name',
                'specific_data.data.cidr',
                'specific_data.data.vlan_id',
            ]
        }
        
        # Use provided fields or defaults
        fields = fields or default_fields.get(asset_type, [])
        
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
        
        self.logger.info(f"Requesting {asset_type} assets (limit: {limit}, offset: {offset})")
        response = self.session.get(url, json=params)
        response.raise_for_status()
        
        result = response.json()
        assets = result.get('assets', [])
        self.logger.info(f"Retrieved {len(assets)} {asset_type} assets")
        
        return result
   
