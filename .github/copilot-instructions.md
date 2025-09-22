# Copilot Instructions for Video Platform Data Extraction

This workspace contains Python scripts for extracting video metadata from multiple video platforms (Brightcove, Vbrick) across different business accounts.

## Architecture Overview

### Multi-Platform Structure
- `Brightcove/` - Brightcove CMS API integration scripts
- `Vbrick/` - Vbrick platform scripts (currently empty)

Each platform has dedicated scripts for different data extraction workflows.

## Key Patterns & Conventions

### Authentication Management
Use the `BrightcoveAuthManager` class pattern for API authentication:
- Implements token caching with 30-second expiration buffer
- Uses Basic auth with base64-encoded client credentials
- Automatically refreshes tokens when expired
- Supports proxy configuration via `secrets.json`

```python
auth_manager = BrightcoveAuthManager(client_id, client_secret, proxies=proxies)
token = auth_manager.get_token()  # Handles refresh automatically
```

### Multi-Account Configuration
Account configurations are stored in dictionaries with consistent structure:
```python
accounts = {
    "account_name": {
        "account_id": "brightcove_account_id",
        "json_output_file": f'json/{name}_cms_metadata.json',
        "csv_output_file": f'csv/{name}_cms_metadata.csv',
    }
}
```

Current accounts include: Internet, Intranet, neo, research, research_internal, impact, circleone, digital_networks_events, fa_web, SuMiTrust, MyWay.

### Progress Tracking with tqdm
All long-running operations use `tqdm` for progress visualization:
- Get total count first for accurate progress bars
- Use `tqdm.write()` instead of `print()` to avoid progress bar interference
- Update progress incrementally with `pbar.update(len(batch))`

### Data Output Strategy
Scripts generate dual output formats:
- **JSON**: Complete API response preservation (`json.dump(all_videos, f, indent=2)`)
- **CSV**: Flattened structure with predefined + custom fields
- Custom fields are extracted from `video.get("custom_fields", {})` nested structure

### Custom Fields Schema
Standard custom fields for compliance/governance:
- `video_content_type`, `business_unit`, `video_category`
- Approval fields: `1a_comms_sign_off`, `1b_comms_sign_off_approver`
- Compliance: `2a_data_classification_disclaimer`, `3a_records_management_disclaimer`
- Archiving: `4a_archiving_disclaimer_comms_branding`, `4b_unique_sharepoint_id`

## API Integration Patterns

### Brightcove CMS API
- Base URL: `https://cms.api.brightcove.com/v1/accounts/{account_id}/videos`
- Batch processing: 100 items per request with offset pagination
- Authentication: Bearer tokens from OAuth endpoint
- Sort by `created_at` for consistent pagination

### Error Handling
- Use `response.raise_for_status()` for HTTP errors
- Implement token refresh on authentication failures
- Handle empty responses gracefully to exit pagination loops

## Required Dependencies
```python
import requests      # HTTP client
from base64 import b64encode  # Auth encoding
import json, csv     # Data formats
import time         # Token expiration
from tqdm import tqdm  # Progress bars
from datetime import datetime  # Timestamps
```

## Configuration Requirements

### secrets.json Structure
```json
{
    "client_id": "brightcove_client_id",
    "client_secret": "brightcove_client_secret",
    "proxies": {
        "http": "proxy_url",
        "https": "proxy_url"
    }
}
```

### Output Directory Structure
Scripts expect these directories to exist:
- `json/` - For JSON output files
- `csv/` - For CSV export files

## Development Guidelines

When adding new platforms or accounts:
1. Follow the multi-account dictionary pattern
2. Implement authentication manager classes for token handling
3. Use consistent progress tracking with tqdm
4. Generate both JSON and CSV outputs
5. Handle custom fields extraction for business-specific metadata
6. Include proxy support for corporate environments

When modifying existing scripts:
- Maintain backward compatibility with existing account configurations
- Preserve the dual output format (JSON + CSV)
- Keep custom fields list synchronized with business requirements
- Test token refresh logic under various network conditions