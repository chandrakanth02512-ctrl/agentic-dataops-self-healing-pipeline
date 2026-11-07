# Secure Fabric connection

Cloud deployment is **blocked** until the runtime settings below are present and
the read-only check succeeds. The currently checked environment has none of the
six variables, no configured outbound cloud identity, and proxy `403` denials for
all three required Microsoft hosts. A saved draft is not an applied runtime change.

## Complete the environment settings

Open the Codex cloud environment settings for this project. The draft already
contains the following runtime-variable requirements (no values were supplied):

| Variable | Value to enter securely in settings |
| --- | --- |
| `FABRIC_TENANT_ID` | Directory (tenant) ID from Entra app registration Overview |
| `FABRIC_CLIENT_ID` | Application (client) ID for **AgenticDataOps_Codex**, not its object ID |
| `FABRIC_CLIENT_SECRET` | The created secret's **Value**, not Secret ID; use secure runtime-variable value entry |
| `FABRIC_WORKSPACE_ID` | GUID from the Fabric URL/settings for **Fabric Lakehouse Platform** |
| `FABRIC_LAKEHOUSE_ID` | Item GUID from the URL/settings for **AgenticDataOps_Lakehouse** |
| `FABRIC_ENVIRONMENT_ID` | Item GUID from the URL/settings for **AgenticDataOps_Environment** |

Never put the secret into chat, a shell command/CLI argument, notebook, JSON
configuration, GitHub, or a committed `.env` file. If the secret Value is no longer
visible, use an approved replacement through Entra's secure administration flow;
do not confuse its visible Secret ID with its Value.

The configuration tool distinguishes **runtime variables read directly by the
application** from **proxy-backed credentials**. This implementation requires the
former for `FABRIC_CLIENT_SECRET`. Microsoft's client-credentials protocol sends
the URL-encoded secret in the token request body. No documented proxy body
substitution capability has been established here, so a proxy-only placeholder
must not be assumed to work. If the settings interface does not support secure
injection of a real runtime secret, stop and use a supported approved Azure
identity/secret binding; do not paste it into an ordinary text field or chat as a
workaround. The OAuth request construction has unit tests; successful live
credential delivery has **not** been verified while credentials are missing.

Review the network additions, preserving all existing allowed domains:

- `login.microsoftonline.com` — Entra token exchange.
- `api.fabric.microsoft.com` — read-only verification and later approved Fabric operations.
- `onelake.dfs.fabric.microsoft.com` — OneLake access required for deployment/data.
- `api.github.com` — previously requested for PR metadata updates and API checks.

Save/apply the environment settings. Wait for runtime configuration to become
ready; saving this draft alone does not apply the changes. For reuse in future
cloud tasks, review/save and publish the prepared environment through its settings.
Never send secret values back to the agent in chat.

## Verify without cloud writes

From the checkout with the settings injected:

```bash
.venv/bin/python -m agentic_dataops.fabric_auth
```

The checker:

1. Requires the six variables and validates the five IDs as GUIDs.
2. Acquires an Entra token with `grant_type=client_credentials` and
   `scope=https://api.fabric.microsoft.com/.default` over verified HTTPS.
3. Reads only the configured workspace, Lakehouse and Environment, checking their
   IDs and exact approved display names.
4. Prints only safe status and resource metadata. It never prints or persists the
   secret, access token or error response body; cross-host redirects are disabled.

Exit 1 means stop deployment. Missing settings, HTTP errors, a resource-name
mismatch or failed TLS/connectivity do not authorize another identity or resource.
A successful check establishes token acquisition and resource read access only;
it does not establish usable capacity, Environment publication, attached runtime,
Spark job permission or cloud execution. Those checks still precede deployment.
`EnterpriseSalesLakehouse` and unrelated items are outside scope.

This checker deliberately implements no cloud writes. No token cache is written.
Any later OneLake API operations require a token for the appropriate storage
resource; a Fabric API token must not be assumed valid for OneLake.

## Sources

- [Entra client-credentials protocol](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow)
- [Microsoft's source for that protocol](https://github.com/MicrosoftDocs/entra-docs/blob/main/docs/identity-platform/v2-oauth2-client-creds-grant-flow.md)
- [Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)

The protocol documentation and runtime version documentation were read from
Microsoft's official documentation repositories. No account credential was used
for those reads.
