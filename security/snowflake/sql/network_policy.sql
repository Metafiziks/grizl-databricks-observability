-- Snowflake Network Policy — CIS Benchmark 3.1
-- SECURITY_ESSENTIALS_CIS3_1: all users must be covered by a network policy.
--
-- Replace <POLICY_NAME> and <YOUR_IP>/32 before running.
-- The apply script (security/snowflake/scripts/network-policy.sh) handles
-- substitution automatically from grizl.snowflake.security.env.

-- 1. Create or replace the policy
CREATE OR REPLACE NETWORK POLICY <POLICY_NAME>
  ALLOWED_IP_LIST = ('<YOUR_IP>/32')
  COMMENT = 'GRIZL trusted IP policy — CIS Benchmark 3.1';

-- 2. Assign to the account (covers all users with no user-level override)
ALTER ACCOUNT SET NETWORK_POLICY = <POLICY_NAME>;

-- 3. Optional: assign a separate policy to a CI/CD service account
--    that needs broader or different IP access.
-- CREATE OR REPLACE NETWORK POLICY <CICD_POLICY_NAME>
--   ALLOWED_IP_LIST = ('<CICD_IP>/32')
--   COMMENT = 'GRIZL CI/CD service account policy';
-- ALTER USER <CICD_USER> SET NETWORK_POLICY = <CICD_POLICY_NAME>;

-- Verify
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;
SHOW NETWORK POLICIES;
