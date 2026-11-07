import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import parse_qs

import pytest

from agentic_dataops import fabric_auth
from agentic_dataops.fabric_runtime import validate_fabric_runtime


class Responses:
    def __init__(self, payloads): self.payloads = iter(payloads); self.requests = []
    def open(self, request, timeout):
        self.requests.append(request)
        return io.BytesIO(json.dumps(next(self.payloads)).encode())


def configuration(monkeypatch):
    result = {name: f'00000000-0000-0000-0000-{i:012d}' for i, name in enumerate(fabric_auth.VARIABLES, 1)}
    result['FABRIC_CLIENT_SECRET'] = 'test-only value +&='
    for name, value in result.items(): monkeypatch.setenv(name, value)
    return result


def test_missing_configuration_never_attempts_network(monkeypatch, capsys):
    for name in fabric_auth.VARIABLES: monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(fabric_auth, 'build_opener', lambda *args: pytest.fail('Network must not run'))
    assert fabric_auth.main() == 1
    assert 'Missing runtime variables' in capsys.readouterr().out


def test_client_credentials_are_form_encoded_and_not_in_url(monkeypatch):
    config = configuration(monkeypatch)
    opener = Responses([{'access_token': 'test-token', 'token_type': 'Bearer'}])
    fabric_auth.acquire_token(config, opener)
    request = opener.requests[0]
    data = parse_qs(request.data.decode())
    assert data['client_secret'] == [config['FABRIC_CLIENT_SECRET']]
    assert data['grant_type'] == ['client_credentials']
    assert data['scope'] == ['https://api.fabric.microsoft.com/.default']
    assert request.method == 'POST'
    assert config['FABRIC_CLIENT_SECRET'] not in request.full_url


def test_readonly_check_returns_no_credentials(monkeypatch, capsys):
    config = configuration(monkeypatch)
    opener = Responses([
        {'access_token': 'test-token', 'token_type': 'Bearer'},
        {'id': config['FABRIC_WORKSPACE_ID'], 'displayName': 'Fabric Lakehouse Platform'},
        {'id': config['FABRIC_LAKEHOUSE_ID'], 'displayName': 'AgenticDataOps_Lakehouse'},
        {'id': config['FABRIC_ENVIRONMENT_ID'], 'displayName': 'AgenticDataOps_Environment'},
    ])
    monkeypatch.setattr(fabric_auth, 'build_opener', lambda *args: opener)
    assert fabric_auth.main() == 0
    output = capsys.readouterr().out
    assert 'test-token' not in output and config['FABRIC_CLIENT_SECRET'] not in output
    assert [r.method for r in opener.requests] == ['POST', 'GET', 'GET', 'GET']
    assert json.loads(output)['cloud_writes'] == 0


def test_http_error_body_is_never_logged(monkeypatch, capsys):
    config = configuration(monkeypatch)
    class Failure:
        def open(self, request, timeout):
            raise HTTPError(request.full_url, 401, config['FABRIC_CLIENT_SECRET'], {},
                            io.BytesIO(b'test-token'))
    monkeypatch.setattr(fabric_auth, 'build_opener', lambda *args: Failure())
    assert fabric_auth.main() == 1
    output = capsys.readouterr().out
    assert 'HTTP 401' in output
    assert 'test-token' not in output and config['FABRIC_CLIENT_SECRET'] not in output


def test_redirects_cannot_forward_credentials():
    assert fabric_auth.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://example.com') is None


def test_wrong_resource_stops_readonly_verification(monkeypatch):
    config = configuration(monkeypatch)
    opener = Responses([{'access_token': 'test-token', 'token_type': 'Bearer'},
                        {'id': config['FABRIC_WORKSPACE_ID'], 'displayName': 'Unrelated Workspace'}])
    monkeypatch.setattr(fabric_auth, 'build_opener', lambda *args: opener)
    with pytest.raises(fabric_auth.ConnectionBlocked, match='does not match'):
        fabric_auth.verify_connection()
    assert len(opener.requests) == 2


@pytest.mark.parametrize(('spark_version', 'delta', 'python'), [
    ('4.0.5', '4.2.0', (3, 13)), ('4.1.1', '4.0.0', (3, 13)), ('4.1.1', '4.2.0', (3, 12)),
])
def test_wrong_native_runtime_is_rejected(spark_version, delta, python):
    with pytest.raises(RuntimeError, match='without replacing built-in libraries'):
        validate_fabric_runtime(SimpleNamespace(version=spark_version), delta_version=delta, python_version=python)


def test_runtime_two_versions_are_accepted():
    result = validate_fabric_runtime(SimpleNamespace(version='4.1.1'), delta_version='4.2.0', python_version=(3, 13))
    assert result == {'spark': '4.1.1', 'delta': '4.2.0', 'python': '3.13'}
