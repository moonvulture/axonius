import os
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Configuration loader that handles both YAML config files and environment variables
    Supports loading from .env files and system environment variables
    """
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize the config loader
        
        Args:
            config_dir: Directory containing config files (default: "config")
        """
        self.config_dir = Path(config_dir)
        self.config_data = {}
        self.secrets_loaded = False
        
    def load_yaml_config(self, filename: str = "config.yaml") -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Args:
            filename: Name of the YAML config file
            
        Returns:
            Dictionary containing configuration data
        """
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
                logger.info(f"Successfully loaded configuration from {config_path}")
                return config_data or {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading configuration file {config_path}: {e}")
            raise
    
    def load_secrets(self, filename: str = "secrets.env") -> bool:
        """
        Load secrets from .env file
        
        Args:
            filename: Name of the secrets file
            
        Returns:
            True if secrets were loaded successfully, False otherwise
        """
        secrets_path = self.config_dir / filename
        
        if not secrets_path.exists():
            logger.warning(f"Secrets file not found: {secrets_path}")
            return False
        
        try:
            # Load the .env file into environment variables
            load_dotenv(secrets_path, override=True)
            logger.info(f"Successfully loaded secrets from {secrets_path}")
            self.secrets_loaded = True
            return True
        except Exception as e:
            logger.error(f"Error loading secrets from {secrets_path}: {e}")
            return False
    
    def get_env_var(self, var_name: str, default: Optional[str] = None, required: bool = True) -> Optional[str]:
        """
        Get environment variable with validation
        
        Args:
            var_name: Name of the environment variable
            default: Default value if variable is not set
            required: Whether the variable is required
            
        Returns:
            Environment variable value or default
        """
        value = os.getenv(var_name, default)
        
        if required and (value is None or value == ""):
            logger.error(f"Required environment variable '{var_name}' is not set")
            raise ValueError(f"Required environment variable '{var_name}' is not set")
        
        if value == "NOTSETYET":
            if required:
                logger.error(f"Environment variable '{var_name}' is set to placeholder value 'NOTSETYET' - please configure it")
                raise ValueError(f"Environment variable '{var_name}' needs to be configured (currently set to 'NOTSETYET')")
            else:
                logger.warning(f"Environment variable '{var_name}' is set to placeholder value 'NOTSETYET'")
                return None
        
        return value
    
    def load_all_config(self, yaml_file: str = "config.yaml", secrets_file: str = "secrets.env") -> Dict[str, Any]:
        """
        Load all configuration from both YAML and secrets files
        
        Args:
            yaml_file: Name of the YAML config file
            secrets_file: Name of the secrets file
            
        Returns:
            Combined configuration dictionary
        """
        # Load YAML configuration
        self.config_data = self.load_yaml_config(yaml_file)
        
        # Load secrets (optional)
        self.load_secrets(secrets_file)
        
        return self.config_data
    
    def get_elasticsearch_config(self) -> Dict[str, Any]:
        """
        Get Elasticsearch configuration with validation
        
        Returns:
            Dictionary containing Elasticsearch configuration
        """
        es_config = {}
        
        # From YAML config
        es_config['url'] = self.config_data.get('ES_URL')
        es_config['index'] = self.config_data.get('ES_INDEX')
        es_config['pipeline'] = self.config_data.get('ES_PIPELINE')
        
        # From environment variables (secrets)
        es_config['api_key'] = self.get_env_var('ES_API_KEY', required=True)
        es_config['cloud_id'] = self.get_env_var('ES_CLOUD_ID', required=True)
        
        # Validate required fields
        required_fields = ['url', 'index', 'api_key', 'cloud_id']
        for field in required_fields:
            if not es_config.get(field):
                raise ValueError(f"Elasticsearch configuration missing required field: {field}")
        
        logger.info("Elasticsearch configuration loaded successfully")
        return es_config
    
    def get_axonius_config(self) -> Dict[str, Any]:
        """
        Get Axonius configuration with validation
        
        Returns:
            Dictionary containing Axonius configuration
        """
        ax_config = {}
        
        # From YAML config
        ax_config['instance_url'] = self.config_data.get('AX_INSTANCE_URL')
        ax_config['device_fields'] = self.config_data.get('AX_DEVICE_FIELDS', [])
        ax_config['user_fields'] = self.config_data.get('AX_USER_FIELDS', [])
        
        # Processing settings from YAML
        ax_config['batch_size'] = self.config_data.get('BATCH_SIZE', 100)
        ax_config['max_records'] = self.config_data.get('MAX_RECORDS', 1000)
        ax_config['request_timeout'] = self.config_data.get('REQUEST_TIMEOUT', 60)
        
        # From environment variables
        ax_config['api_key'] = self.get_env_var('AX_API_KEY', required=True)
        ax_config['api_secret'] = self.get_env_var('AX_API_SECRET', required=True)
        
        # Validate required fields
        required_fields = ['api_key', 'api_secret']
        for field in required_fields:
            if not ax_config.get(field):
                raise ValueError(f"Axonius configuration missing required field: {field}")
        
        # Validate device fields
        if not ax_config['device_fields']:
            logger.warning("No device fields specified in configuration, using defaults")
            ax_config['device_fields'] = [
                'specific_data.data.hostname',
                'specific_data.data.network_interfaces.ips_preferred',
                'specific_data.data.network_interfaces.mac_preferred',
                'specific_data.data.last_seen',
                'adapters_data.axonius_adapter.last_seen',
            ]
        
        logger.info("Axonius configuration loaded successfully")
        logger.info(f"Device fields configured: {len(ax_config['device_fields'])} fields")
        return ax_config
    
    def validate_config(self) -> bool:
        """
        Validate that all required configuration is present
        
        Returns:
            True if configuration is valid
        """
        try:
            self.get_elasticsearch_config()
            self.get_axonius_config()
            logger.info("Configuration validation successful")
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False
    
    def print_config_summary(self) -> None:
        """Print a summary of loaded configuration (without sensitive values)"""
        print("\n" + "="*60)
        print("CONFIGURATION SUMMARY")
        print("="*60)
        
        # Elasticsearch config
        print("\nElasticsearch Configuration:")
        print(f"  URL: {self.config_data.get('ES_URL', 'Not set')}")
        print(f"  Index: {self.config_data.get('ES_INDEX', 'Not set')}")
        print(f"  Pipeline: {self.config_data.get('ES_PIPELINE', 'Not set')}")
        print(f"  API Key: {'*' * 8 if self.get_env_var('ES_API_KEY', required=False) else 'Not set'}")
        print(f"  Cloud ID: {'*' * 8 if self.get_env_var('ES_CLOUD_ID', required=False) else 'Not set'}")
        
        # Axonius config
        print("\nAxonius Configuration:")
        print(f"  Instance URL: {self.config_data.get('AX_INSTANCE_URL', 'Not set')}")
        print(f"  Device Fields: {len(self.config_data.get('AX_DEVICE_FIELDS', []))} fields configured")
        print(f"  User Fields: {len(self.config_data.get('AX_USER_FIELDS', []))} fields configured")
        print(f"  Batch Size: {self.config_data.get('BATCH_SIZE', 'Default')}")
        print(f"  Max Records: {self.config_data.get('MAX_RECORDS', 'Default')}")
        print(f"  API Key: {'*' * 8 if self.get_env_var('AX_API_KEY', required=False) else 'Not set'}")
        print(f"  API Secret: {'*' * 8 if self.get_env_var('AX_API_SECRET', required=False) else 'Not set'}")
        
        print(f"\nSecrets loaded: {self.secrets_loaded}")
        print("="*60)


def get_config_loader(config_dir: str = "config") -> ConfigLoader:
    """
    Create a configuration loader instance
    
    Args:
        config_dir: Directory containing config files
        
    Returns:
        ConfigLoader instance
    """
    return ConfigLoader(config_dir)


def load_config(config_dir: str = "config", 
                yaml_file: str = "config.yaml", 
                secrets_file: str = "secrets.env") -> ConfigLoader:
    """
    Convenience function to create and load all configuration
    
    Args:
        config_dir: Directory containing config files
        yaml_file: Name of the YAML config file
        secrets_file: Name of the secrets file
        
    Returns:
        Configured ConfigLoader instance
    """
    loader = ConfigLoader(config_dir)
    loader.load_all_config(yaml_file, secrets_file)
    return loader


if __name__ == "__main__":
    # Test the configuration loader
    try:
        loader = ConfigLoader()
        loader.load_all_config()
        loader.print_config_summary()
        
        if loader.validate_config():
            print("\n✅ Configuration validation passed!")
        else:
            print("\n❌ Configuration validation failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Configuration loading failed: {e}")
        sys.exit(1)