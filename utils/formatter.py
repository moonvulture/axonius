from datetime import datetime
import re

logger = get_logger(__name__)

def transform_data_for_elasticsearch(formatted_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform formatted Axonius data to Elasticsearch document format"""
    
    documents = []
    
    for asset in formatted_assets:
        hostname = asset.get('hostname')
        ip_addresses = asset.get('ip_addresses', [])
        mac_addresses = asset.get('mac_addresses', [])
        last_seen = asset.get('last_seen')
        
        # Skip assets without essential data
        if not hostname and not ip_addresses:
            continue
        
        # Use the first IP address for the main document
        primary_ip = ip_addresses[0] if ip_addresses else None
        primary_mac = mac_addresses[0] if mac_addresses else None
        
        # Create document in the specified format with ECS fields
        doc = {
            '_index': INDEX_NAME,
            '_source': {
                'host': {
                    'ip': primary_ip,
                    'mac': primary_mac,
                    'hostname': hostname
                },
                # Add ECS-compliant timestamp field
                '@timestamp': last_seen or datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                # Store all IPs and MACs if multiple exist
                'network': {
                    'ip_addresses': ip_addresses,
                    'mac_addresses': mac_addresses
                },
                # Axonius-specific metadata
                'axonius': {
                    'last_seen': last_seen,
                    'ingestion_time': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                }
            }
        }
        
        documents.append(doc)
    
    logger.info(f"Transformed {len(documents)} documents for indexing")
    return documents


def format_last_seen_date(last_seen_data: Any) -> str:
    """Format last seen date to ECS-compliant ISO 8601 format"""
    if not last_seen_data:
        return None
    
    try:
        # Handle different possible date formats from Axonius
        if isinstance(last_seen_data, str):
            # Try parsing common date formats
            date_formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format with microseconds
                '%Y-%m-%dT%H:%M:%SZ',     # ISO format without microseconds
                '%Y-%m-%d %H:%M:%S',      # Standard datetime format
                '%Y-%m-%d',               # Date only
                '%m/%d/%Y %H:%M:%S',      # US format
                '%d/%m/%Y %H:%M:%S',      # European format
            ]
            
            for date_format in date_formats:
                try:
                    dt = datetime.strptime(last_seen_data, date_format)
                    # Convert to ECS format (ISO 8601 with Z suffix)
                    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    continue
        
        elif isinstance(last_seen_data, (int, float)):
            # Handle Unix timestamps
            dt = datetime.fromtimestamp(last_seen_data)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        elif isinstance(last_seen_data, list) and last_seen_data:
            # If it's a list, use the first (most recent) date
            return format_last_seen_date(last_seen_data[0])
        
        logger.warning(f"Could not parse last_seen date: {last_seen_data}")
        return None
        
    except Exception as e:
        logger.error(f"Error formatting last_seen date '{last_seen_data}': {e}")
        return None


def normalize_ip_addresses(ip_data: Any) -> List[str]:
    """Normalize IP address data"""
    if not ip_data:
        return []
    
    if isinstance(ip_data, str):
        ip_data = [ip_data]
    elif not isinstance(ip_data, list):
        return []
    
    # Clean and validate IP addresses
    clean_ips = []
    for ip in ip_data:
        if isinstance(ip, str):
            ip = ip.strip()
            # Basic IP validation (could be enhanced with ipaddress module)
            if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', ip):
                clean_ips.append(ip)
    
    return clean_ips

def normalize_mac_addresses(mac_data: Any) -> List[str]:
    """Normalize MAC address data"""
    if not mac_data:
        return []
    
    if isinstance(mac_data, str):
        mac_data = [mac_data]
    elif not isinstance(mac_data, list):
        return []
    
    # Clean and standardize MAC addresses
    clean_macs = []
    for mac in mac_data:
        if isinstance(mac, str):
            # Remove common separators and convert to lowercase
            mac = re.sub(r'[:\-\s]', '', mac.strip().lower())
            # Validate MAC format (12 hex characters)
            if re.match(r'^[0-9a-f]{12}$', mac):
                # Format as standard MAC address (xx:xx:xx:xx:xx:xx)
                formatted_mac = ':'.join([mac[i:i+2] for i in range(0, 12, 2)])
                clean_macs.append(formatted_mac)
    
    return clean_macs


def normalize_hostname(hostname: Any) -> str:
    """Normalize hostname data"""
    if not hostname:
        return None
    
    if isinstance(hostname, list):
        hostname = hostname[0] if hostname else None
    
    if isinstance(hostname, str):
        # Clean hostname - remove extra whitespace, convert to lowercase
        hostname = hostname.strip().lower()
        # Remove any invalid characters for hostnames
        hostname = re.sub(r'[^a-zA-Z0-9\-\.]', '', hostname)
        return hostname if hostname else None
    
    return None
