"""Read-only Fabric connection check using securely injected runtime variables.

No tokens or secrets are printed, persisted, accepted as CLI flags, or returned
in the public report. The client secret must be a real process value, not an
unverified proxy placeholder. No cloud writes are implemented by this module.
"""

import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

VARIABLES = (
    'FABRIC_TENANT_ID', 'FABRIC_CLIENT_ID', 'FABRIC_CLIENT_SECRET',
    'FABRIC_WORKSPACE_ID', 'FABRIC_LAKEHOUSE_ID', 'FABRIC_ENVIRONMENT_ID',
)


class ConnectionBlocked(RuntimeError):
    """Contains only safe diagnostic text, never a server response body."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        # Never forward a client secret or bearer token to a redirect destination.
        return None


def read_configuration():
    missing = [name for name in VARIABLES if not os.environ.get(name)]
    if missing:
        raise ConnectionBlocked('Missing runtime variables: ' + ', '.join(missing))
    config = {name: os.environ[name] for name in VARIABLES}
    for name in VARIABLES:
        if name != 'FABRIC_CLIENT_SECRET':
            try:
                config[name] = str(UUID(config[name]))
            except ValueError:
                raise ConnectionBlocked(f'{name} must be the resource GUID, not its display name') from None
    return config


def request_json(opener, request, operation):
    try:
        with opener.open(request, timeout=30) as response:
            # Do not dump body data on JSON, API or authentication errors.
            payload = json.load(response)
        if not isinstance(payload, dict):
            raise ConnectionBlocked(f'{operation} returned an unexpected response')
        return payload
    except HTTPError as error:
        raise ConnectionBlocked(f'{operation} failed: HTTP {error.code}; response body withheld') from None
    except (URLError, TimeoutError, OSError):
        raise ConnectionBlocked(f'{operation} could not connect; check network access and TLS trust') from None
    except (ValueError, UnicodeError):
        raise ConnectionBlocked(f'{operation} returned invalid JSON; response body withheld') from None


def acquire_token(config, opener):
    # Entra's documented client-credentials protocol: URL-encoded form body.
    body = urlencode({
        'client_id': config['FABRIC_CLIENT_ID'],
        'client_secret': config['FABRIC_CLIENT_SECRET'],
        'grant_type': 'client_credentials',
        'scope': 'https://api.fabric.microsoft.com/.default',
    }).encode()
    url = f"https://login.microsoftonline.com/{config['FABRIC_TENANT_ID']}/oauth2/v2.0/token"
    payload = request_json(opener, Request(
        url, data=body, method='POST',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
    ), 'Microsoft Entra token exchange')
    token = payload.get('access_token')
    if not isinstance(token, str) or not token or payload.get('token_type', '').lower() != 'bearer':
        raise ConnectionBlocked('Microsoft Entra did not return a usable bearer token; response withheld')
    return token


def verify_connection():
    config = read_configuration()
    # Default ProxyHandler and HTTPSHandler retain inherited proxy and CA trust.
    opener = build_opener(NoRedirect())
    token = acquire_token(config, opener)
    workspace = config['FABRIC_WORKSPACE_ID']
    resources = [
        (f'workspaces/{workspace}', workspace, 'Fabric Lakehouse Platform'),
        (f"workspaces/{workspace}/lakehouses/{config['FABRIC_LAKEHOUSE_ID']}",
         config['FABRIC_LAKEHOUSE_ID'], 'AgenticDataOps_Lakehouse'),
        (f"workspaces/{workspace}/environments/{config['FABRIC_ENVIRONMENT_ID']}",
         config['FABRIC_ENVIRONMENT_ID'], 'AgenticDataOps_Environment'),
    ]
    checked = []
    for path, expected_id, expected_name in resources:
        request = Request('https://api.fabric.microsoft.com/v1/' + path, method='GET',
                          headers={'Authorization': 'Bearer ' + token})
        resource = request_json(opener, request, f'Read {expected_name}')
        if resource.get('id', '').lower() != expected_id or resource.get('displayName') != expected_name:
            raise ConnectionBlocked(f'Resource identity does not match the approved {expected_name}; stop deployment')
        checked.append({'id': expected_id, 'name': expected_name, 'read_access': True})
    return {'authentication': 'verified', 'resources': checked, 'cloud_writes': 0,
            'deployment': 'not performed',
            'remaining_checks': ['Environment publication/runtime', 'Capacity and Spark execution']}


def main():
    try:
        result = verify_connection()
    except ConnectionBlocked as error:
        print(json.dumps({'authentication': 'blocked', 'reason': str(error), 'cloud_writes': 0}))
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
