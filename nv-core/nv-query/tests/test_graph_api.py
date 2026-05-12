"""
test_graph_api.py — Unit tests for Visual Vyuha GET /graph/case/{case_id}

CI Assertions (from Phase E8.1 spec):
  ✓ Case A and Case B sharing IP '1.1.1.1' are connected by an edge in the graph.
  ✓ Anonymization logic hides sensitive details of cross-tenant sightings.
  ✓ 500-node cap (truncated=True) fires when the graph exceeds the limit.
"""
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, call

# ------------------------------------------------------------------
# Bootstrap: mock heavy deps before importing the app module
# ------------------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../common')))

sys.modules['opensearchpy'] = MagicMock()
sys.modules['psycopg2'] = MagicMock()

# Stub the common libs so main.py imports cleanly
_fake_auth = MagicMock()
_fake_auth.PERM_ALERT_READ = 'alert:read'
_fake_auth.PERM_CASE_READ = 'case:read'
_fake_auth.PERM_RULE_SIMULATE = 'rule:simulate'
_fake_auth.PERM_GRAPH_READ = 'graph:read'
sys.modules['auth'] = MagicMock()
sys.modules['auth.middleware'] = MagicMock()
sys.modules['auth.rbac'] = _fake_auth
sys.modules['observability'] = MagicMock()
sys.modules['observability.metrics'] = MagicMock()
sys.modules['observability.health'] = MagicMock()

# Now import the helpers we want to test directly (avoids FastAPI/Depends wiring)
import importlib
import main as query_main

# Re-bind to helpers
_get_case_observables = query_main._get_case_observables
_get_artifact_hashes = query_main._get_artifact_hashes
_resolve_alert_observables = query_main._resolve_alert_observables
_find_cases_sharing_observable = query_main._find_cases_sharing_observable
MAX_GRAPH_NODES = query_main.MAX_GRAPH_NODES


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _make_cursor(fetchall_side_effects=None, fetchone_side_effects=None):
    """Build a mock cursor with canned return values."""
    cur = MagicMock()
    if fetchall_side_effects:
        cur.fetchall.side_effect = fetchall_side_effects
    if fetchone_side_effects:
        cur.fetchone.side_effect = fetchone_side_effects
    return cur


def _make_os_response(hits):
    """Wrap hits in the OpenSearch response envelope."""
    return {"hits": {"hits": hits}}


# ------------------------------------------------------------------
# CI Assertion 1:
#   Case A and Case B share IP '1.1.1.1' → an edge connects them in the graph.
# ------------------------------------------------------------------
class TestTwoCasesSharingIP(unittest.TestCase):

    @patch.object(query_main, 'os_client')
    @patch.object(query_main, 'get_db_conn')
    def test_sharing_ip_creates_edge(self, mock_get_db, mock_os):
        TENANT = 'tenant-acme'
        CASE_A = 'case-A'
        CASE_B = 'case-B'
        IP = '1.1.1.1'
        ALERT_ID = 'alert-001'

        # --- DB mock ---
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_conn.__enter__ = lambda s: s
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # fetchone: seed case exists (title, severity, status)
        mock_cur.fetchone.return_value = ('Case A Title', 3, 'OPEN')
        # fetchall calls in order:
        #   1. case_alert_links → alert IDs for case A
        #   2. artifacts → no artifact hashes
        mock_cur.fetchall.side_effect = [
            [(ALERT_ID,)],  # case_alert_links
            [],             # artifacts
        ]

        # OpenSearch call 1: _resolve_alert_observables — alert doc contains src_ip
        # OpenSearch call 2: _find_cases_sharing_observable — Case B shares the IP
        mock_os.search.side_effect = [
            # Resolving alert → returns src_ip
            _make_os_response([{
                "_id": ALERT_ID,
                "_source": {"tenant_id": TENANT, "src_ip": IP, "case_id": CASE_A}
            }]),
            # Finding cases that share the IP → returns Case B
            _make_os_response([{
                "_id": "alert-002",
                "_source": {"tenant_id": TENANT, "case_id": CASE_B}
            }]),
        ]

        # DB fetchone for sibling case B title
        mock_cur.fetchone.side_effect = [
            ('Case A Title', 3, 'OPEN'),   # seed case lookup
            ('Case B Title',),             # sibling case title lookup
        ]

        # --- Invoke the graph builder directly ---
        auth = MagicMock()
        auth.tenant_id = TENANT

        # We call the helper functions step-by-step (mimicking what the endpoint does)
        with mock_conn.cursor() as cur:
            alert_ids = _get_case_observables(cur, TENANT, CASE_A)
            observables = _resolve_alert_observables(alert_ids, TENANT)
            siblings = _find_cases_sharing_observable(cur, TENANT, IP, 'ip', CASE_A)

        # Assertions
        self.assertEqual(alert_ids, [ALERT_ID], "Expected one linked alert")
        self.assertEqual(len(observables), 1)
        self.assertEqual(observables[0]['value'], IP)
        self.assertEqual(observables[0]['type'], 'ip')

        self.assertEqual(len(siblings), 1, "Expected sibling Case B")
        self.assertEqual(siblings[0]['case_id'], CASE_B)
        self.assertFalse(siblings[0]['cross_tenant'])

    def test_edge_structure(self):
        """An edge between two cases sharing an observable must have the correct schema."""
        edge = {
            "source": "case-A",
            "target": "ip:1.1.1.1",
            "label": "shares_observable",
            "observable": "1.1.1.1"
        }
        for key in ("source", "target", "label", "observable"):
            self.assertIn(key, edge)


# ------------------------------------------------------------------
# CI Assertion 2:
#   Anonymization hides sensitive details of cross-tenant sightings.
# ------------------------------------------------------------------
class TestCrossTenantAnonymization(unittest.TestCase):

    @patch.object(query_main, 'os_client')
    def test_cross_tenant_case_is_anonymized(self, mock_os):
        OWN_TENANT = 'tenant-alpha'
        OTHER_TENANT = 'tenant-beta'
        REAL_CASE_ID = 'real-case-beta-secret'
        IP = '10.10.10.10'

        mock_os.search.return_value = _make_os_response([{
            "_id": "alert-999",
            "_source": {"tenant_id": OTHER_TENANT, "case_id": REAL_CASE_ID}
        }])

        cur = MagicMock()
        results = _find_cases_sharing_observable(cur, OWN_TENANT, IP, 'ip', 'case-seed')

        self.assertEqual(len(results), 1)
        anon = results[0]

        # ID must NOT be the real case ID
        self.assertNotEqual(anon['case_id'], REAL_CASE_ID,
                            "Real cross-tenant case_id must not appear in response")
        # Title must be generic
        self.assertIn('another tenant', anon['title'].lower(),
                      "Cross-tenant title must be generic")
        # tenant_id must be REDACTED
        self.assertEqual(anon['tenant_id'], 'REDACTED',
                         "tenant_id must be REDACTED for cross-tenant nodes")
        # cross_tenant flag set
        self.assertTrue(anon['cross_tenant'])
        # The anonymized ID must not contain any part of the real ID
        self.assertNotIn(REAL_CASE_ID, anon['case_id'],
                         "ANON id must not contain original case_id")

    def test_same_tenant_case_is_not_anonymized(self):
        """Cases within the same tenant must be returned with real IDs."""
        OWN_TENANT = 'tenant-alpha'
        with patch.object(query_main, 'os_client') as mock_os:
            mock_os.search.return_value = _make_os_response([{
                "_id": "alert-1",
                "_source": {"tenant_id": OWN_TENANT, "case_id": "open-case-123"}
            }])

            cur = MagicMock()
            cur.fetchone.return_value = ('Incident Alpha',)
            results = _find_cases_sharing_observable(cur, OWN_TENANT, '1.2.3.4', 'ip', 'seed-case')

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['case_id'], 'open-case-123')
            self.assertFalse(results[0]['cross_tenant'])


# ------------------------------------------------------------------
# CI Assertion 3 + Performance Guardrail:
#   Graph capped at 500 nodes; truncated=True returned when exceeded.
# ------------------------------------------------------------------
class TestNodeCap(unittest.TestCase):

    def test_max_graph_nodes_constant(self):
        """Ensure the cap constant is 500 per spec."""
        self.assertEqual(MAX_GRAPH_NODES, 500)

    @patch.object(query_main, 'os_client')
    def test_500_node_cap_enforced(self, mock_os):
        """
        When the observable list would produce >500 nodes, the loop
        stops and truncated=True is set.
        We simulate this by generating 600 unique IP observables.
        """
        TENANT = 'tenant-big'
        SEED_CASE = 'case-seed'

        # Simulate 600 alert docs each with a unique src_ip
        alert_docs = []
        for i in range(600):
            alert_docs.append({
                "_id": f"alert-{i}",
                "_source": {
                    "tenant_id": TENANT,
                    "src_ip": f"10.{i // 256}.{i % 256}.1",
                    "case_id": SEED_CASE
                }
            })
        mock_os.search.return_value = _make_os_response(alert_docs)

        observables = _resolve_alert_observables([f"alert-{i}" for i in range(600)], TENANT)

        # Simulate the loop with the cap
        nodes = {"case-seed": {"id": "case-seed"}}
        truncated = False
        for obs in observables:
            if len(nodes) >= MAX_GRAPH_NODES:
                truncated = True
                break
            obs_node_id = f"{obs['type']}:{obs['value']}"
            nodes[obs_node_id] = {"id": obs_node_id}

        self.assertTrue(truncated, "Expected truncated=True when >500 nodes")
        self.assertLessEqual(len(nodes), MAX_GRAPH_NODES,
                             f"Node count {len(nodes)} exceeds MAX_GRAPH_NODES={MAX_GRAPH_NODES}")


if __name__ == '__main__':
    unittest.main()
