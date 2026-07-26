# Microsoft Fabric — Network Security

Fabric does not have a workspace-level IP allowlist equivalent to Snowflake
network policies or Databricks IP access lists. Network controls operate at
two higher levels:

## 1. Azure Tenant — Conditional Access (recommended)

Restrict who can authenticate to the Microsoft 365 / Entra ID tenant that
backs your Fabric capacity.

**Path:** Entra ID → Security → Conditional Access → New policy

Key settings:
- **Target resources:** Microsoft Fabric (app id `00000009-0000-0000-c000-000000000000`)
- **Conditions → Locations:** Include only named locations (your trusted IP ranges)
- **Grant:** Require MFA or compliant device (in addition to IP check)

This is the equivalent of an account-level network policy — it applies to
all Fabric users in the tenant.

## 2. Fabric Capacity — Private Link (network isolation)

For workloads that require network-level isolation rather than identity-based
controls:

**Path:** Azure Portal → Fabric capacity → Networking → Private endpoints

Private Link puts Fabric behind your VNet so it is not reachable from the
public internet at all. More secure than IP allowlisting but requires VNet
peering or a VPN/ExpressRoute from your dev machines.

## 3. Trusted Workspace Access (for OneLake / Azure Storage)

If Fabric reads from Azure Data Lake / OneLake via a managed identity, you
can restrict the storage account to only accept requests from trusted Fabric
workspaces:

**Path:** Azure Portal → Storage account → Networking →
Firewalls and virtual networks → Add Fabric workspace

## Action items

| Control | Status | Owner |
|---|---|---|
| Conditional Access policy scoped to Fabric | To do | Azure admin |
| Named location defined (trusted CIDR) | To do | Azure admin |
| Private Link (if required) | Optional | Azure admin |

Fabric security cannot be scripted via a CLI tool in this repo — all changes
are made through the Azure portal or ARM/Bicep templates. If you add
infrastructure-as-code for the Fabric capacity, add the Conditional Access
policy there.
