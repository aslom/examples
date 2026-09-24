"""k8slib — kubectl JSON in, data structures out, and a loud error for a moved field.

The fixtures are real shapes captured from the ykt1 cluster (KEDA 2.20
ScaledObject conditions, a Deployment, the Strimzi CRD version list), so a
change in how those objects look shows up here rather than mid-deploy.

Gate for T0.2 (DESIGN_PHASE1.md §20): "incl. a missing-field case that must
raise rather than return empty".
"""
import json

import k8slib
import pytest
from k8slib import FieldMissing, Kubectl, condition_is, condition_reason, conditions, dig
from proclib import Result

# ---- fixtures: real object shapes -------------------------------------------

SCALEDOBJECT_IDLE = {
    "apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
    "metadata": {"name": "eventrunner", "namespace": "kev1"},
    "spec": {"minReplicaCount": 0, "maxReplicaCount": 1,
             "scaleTargetRef": {"name": "eventrunner"}},
    "status": {"conditions": [
        {"type": "Ready",    "status": "True",  "reason": "ScaledObjectReady"},
        {"type": "Active",   "status": "False", "reason": "ScalerNotActive",
         "message": "Scaling is not performed because triggers are not active"},
        {"type": "Fallback", "status": "Unknown"},
        {"type": "Paused",   "status": "False"},
    ]},
}

SCALEDOBJECT_TRIGGER_ERROR = {
    "metadata": {"name": "leaf-worker"},
    "status": {"conditions": [
        {"type": "Ready", "status": "False", "reason": "TriggerError",
         "message": "Triggers defined in ScaledJob are not working correctly"},
        {"type": "Active", "status": "False", "reason": "ScalerNotActive"},
    ]},
}

DEPLOY_SCALED_TO_ZERO = {
    "kind": "Deployment",
    "metadata": {"name": "eventrunner", "namespace": "kev1"},
    "spec": {"replicas": 0},
    "status": {"replicas": 0, "observedGeneration": 4},
}

DEPLOY_ACTIVE = {
    "kind": "Deployment",
    "metadata": {"name": "eventrunner"},
    "spec": {"replicas": 1},
    "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1},
}

NODES = {"items": [
    {"metadata": {"name": "ip-10-0-22-12"},
     "status": {"nodeInfo": {"architecture": "amd64", "kubeletVersion": "v1.33.13"}}},
    {"metadata": {"name": "ip-10-0-29-140"},
     "status": {"nodeInfo": {"architecture": "amd64", "kubeletVersion": "v1.33.13"}}},
]}

STRIMZI_CRD = {
    "metadata": {"name": "kafkatopics.kafka.strimzi.io"},
    "spec": {"versions": [
        {"name": "v1", "served": True, "storage": True},
        {"name": "v1beta2", "served": False, "storage": False},
    ]},
}


# ---- fake kubectl -----------------------------------------------------------

class FakeKubectl:
    """Records argv and replays canned Results keyed by a substring of the argv."""

    def __init__(self, responses: dict[str, object], default=None):
        self.responses = responses
        self.default = default
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv, **kw):
        argv = tuple(str(a) for a in argv)
        self.calls.append(argv)
        joined = " ".join(argv)
        for key, value in self.responses.items():
            if key in joined:
                if isinstance(value, Result):
                    return value
                return Result(argv, 0, json.dumps(value), "", 0.01)
        if self.default is not None:
            return self.default
        return Result(argv, 1, "", 'Error from server (NotFound): not found', 0.01)


@pytest.fixture
def fake(monkeypatch):
    def _install(responses, default=None):
        fk = FakeKubectl(responses, default)
        monkeypatch.setattr(k8slib, "run", fk)
        return fk
    return _install


# ---- dig(): the point of the module ----------------------------------------

def test_dig_walks_nested_dicts_and_lists():
    assert dig(SCALEDOBJECT_IDLE, "spec", "scaleTargetRef", "name") == "eventrunner"
    assert dig(SCALEDOBJECT_IDLE, "status", "conditions", 0, "type") == "Ready"
    assert dig(DEPLOY_SCALED_TO_ZERO, "spec", "replicas") == 0


def test_dig_raises_for_a_missing_field_rather_than_returning_empty():
    """`-o jsonpath` returns "" and exits 0 when a field moves. This does not."""
    with pytest.raises(FieldMissing) as ei:
        dig(DEPLOY_ACTIVE, "status", "unavailableReplicas")
    assert "status.unavailableReplicas" in str(ei.value)


def test_dig_raises_for_a_wrong_shape_not_just_a_missing_key():
    with pytest.raises(FieldMissing):
        dig(DEPLOY_ACTIVE, "metadata", "name", "nested-under-a-string")


def test_dig_raises_for_an_out_of_range_list_index():
    with pytest.raises(FieldMissing):
        dig(SCALEDOBJECT_IDLE, "status", "conditions", 99, "type")


def test_dig_honours_an_explicit_default():
    assert dig(DEPLOY_SCALED_TO_ZERO, "status", "readyReplicas", default=0) == 0
    assert dig({}, "a", "b", "c", default=None) is None


def test_dig_distinguishes_a_real_zero_from_a_defaulted_zero():
    """0 is a legitimate replica count. `dig` must not conflate it with absent."""
    assert dig(DEPLOY_SCALED_TO_ZERO, "spec", "replicas") == 0
    with pytest.raises(FieldMissing):
        dig({"spec": {}}, "spec", "replicas")


# ---- conditions -------------------------------------------------------------

def test_conditions_indexes_by_type():
    c = conditions(SCALEDOBJECT_IDLE)
    assert set(c) == {"Ready", "Active", "Fallback", "Paused"}
    assert c["Ready"]["reason"] == "ScaledObjectReady"


def test_condition_is_reads_the_idle_state_correctly():
    """The Phase 1 idle state: Ready=True, Active=False. Not a failure."""
    assert condition_is(SCALEDOBJECT_IDLE, "Ready") is True
    assert condition_is(SCALEDOBJECT_IDLE, "Active") is False
    assert condition_is(SCALEDOBJECT_IDLE, "Active", "False") is True
    assert condition_is(SCALEDOBJECT_IDLE, "Paused", "False") is True


def test_condition_is_false_for_an_absent_condition():
    assert condition_is({"status": {}}, "Ready") is False
    assert condition_is(SCALEDOBJECT_TRIGGER_ERROR, "Paused") is False


def test_condition_reason_surfaces_the_trigger_error_message():
    msg = condition_reason(SCALEDOBJECT_TRIGGER_ERROR, "Ready")
    assert "TriggerError" in msg and "not working correctly" in msg


def test_conditions_on_an_object_with_no_status_is_empty_not_an_error():
    assert conditions({"metadata": {"name": "x"}}) == {}


# ---- argv construction ------------------------------------------------------

def test_argv_carries_context_and_namespace():
    k = Kubectl(context="ykt1", namespace="kev1")
    assert k.argv(["get", "pods"]) == [
        "kubectl", "--context", "ykt1", "-n", "kev1", "get", "pods"]


def test_namespace_false_suppresses_the_flag_for_cluster_scoped_calls():
    k = Kubectl(namespace="kev1")
    assert "-n" not in k.argv(["get", "nodes"], namespace=False)


def test_namespace_override_beats_the_default():
    k = Kubectl(namespace="kev1")
    argv = k.argv(["get", "kafkatopic"], namespace="kafka")
    assert argv[argv.index("-n") + 1] == "kafka"


# ---- reads through the fake --------------------------------------------------

def test_get_returns_the_parsed_object(fake):
    fake({"get deploy eventrunner": DEPLOY_ACTIVE})
    k = Kubectl(namespace="kev1")
    assert dig(k.get("deploy", "eventrunner"), "status", "readyReplicas") == 1


def test_get_missing_ok_returns_none(fake):
    fake({})   # default response is NotFound
    k = Kubectl(namespace="kev1")
    assert k.get("deploy", "nope", missing_ok=True) is None


def test_get_without_missing_ok_raises(fake):
    fake({})
    k = Kubectl(namespace="kev1")
    with pytest.raises(RuntimeError, match="failed"):
        k.get("deploy", "nope")


def test_replicas_returns_minus_one_for_an_absent_deployment(fake):
    """-1, not 0: "not deployed" and "scaled to zero" are different answers and
    the e2e test asserts on exactly that difference."""
    fake({})
    k = Kubectl(namespace="kev1")
    assert k.replicas("eventrunner") == -1


def test_replicas_reads_a_real_zero(fake):
    fake({"get deploy eventrunner": DEPLOY_SCALED_TO_ZERO})
    assert Kubectl(namespace="kev1").replicas("eventrunner") == 0


def test_replicas_defaults_to_zero_for_a_keda_managed_deployment(fake):
    """A Deployment under a ScaledObject declares no spec.replicas (§14.1), so
    before KEDA's first scale the field is genuinely absent and means zero."""
    fake({"get deploy eventrunner": {"metadata": {"name": "eventrunner"}, "spec": {}}})
    assert Kubectl(namespace="kev1").replicas("eventrunner") == 0


def test_node_architectures(fake):
    fake({"get nodes": NODES})
    assert Kubectl().node_architectures() == {"amd64"}


def test_crd_served_versions_excludes_unserved(fake):
    """Strimzi 1.0.x serves only v1; v1beta2 is still listed but not served."""
    fake({"get crd kafkatopics.kafka.strimzi.io": STRIMZI_CRD})
    versions = Kubectl().crd_served_versions("kafkatopics.kafka.strimzi.io")
    assert versions == ["v1"]
    assert "v1beta2" not in versions


def test_crd_served_versions_empty_when_crd_absent(fake):
    fake({})
    assert Kubectl().crd_served_versions("nope.example.com") == []


# ---- diff-based change detection (§14.1) ------------------------------------

def test_diff_kustomize_maps_exit_codes(fake):
    fake({"diff -k": Result(("kubectl",), 0, "", "", 0.1)})
    assert Kubectl().diff_kustomize("overlays/test")[0] == 0

    fake({"diff -k": Result(("kubectl",), 1, "- replicas: 1\n+ replicas: 2", "", 0.1)})
    rc, text = Kubectl().diff_kustomize("overlays/test")
    assert rc == 1 and "replicas" in text

    fake({"diff -k": Result(("kubectl",), 5, "", "server error", 0.1)})
    assert Kubectl().diff_kustomize("overlays/test")[0] == 5


def test_diff_kustomize_reports_error_when_kubectl_is_missing(fake):
    fake({"diff -k": Result(("kubectl",), 127, "", "not found", 0.0, launched=False)})
    rc, _ = Kubectl().diff_kustomize("overlays/test")
    assert rc == 2, "a kubectl that never ran must not look like 'no differences'"


# ---- writes -----------------------------------------------------------------

def test_ensure_namespace_applies_a_namespace_manifest(fake):
    fk = fake({"apply -f -": Result(("kubectl",), 0, "namespace/kev1 created", "", 0.1)})
    assert Kubectl().ensure_namespace("kev1").ok
    assert any("apply" in " ".join(c) for c in fk.calls)


def test_annotate_pause_uses_overwrite(fake):
    fk = fake({"annotate": Result(("kubectl",), 0, "annotated", "", 0.1)})
    Kubectl(namespace="kev1").annotate(
        "scaledobject", "eventrunner", "autoscaling.keda.sh/paused-replicas=0")
    argv = fk.calls[-1]
    assert "--overwrite" in argv
    assert "autoscaling.keda.sh/paused-replicas=0" in argv


def test_scale_builds_the_replicas_flag(fake):
    fk = fake({"scale": Result(("kubectl",), 0, "scaled", "", 0.1)})
    Kubectl(namespace="kev1").scale("deploy/eventbridge", 0)
    assert "--replicas=0" in fk.calls[-1]


# ---- predicates -------------------------------------------------------------

def test_scaledobject_active_predicate(fake):
    fake({"get scaledobject eventrunner": SCALEDOBJECT_IDLE})
    k = Kubectl(namespace="kev1")
    assert k8slib.scaledobject_active(k, "eventrunner", False)() is True
    assert k8slib.scaledobject_active(k, "eventrunner", True)() is False


def test_deploy_replicas_is_predicate(fake):
    fake({"get deploy eventrunner": DEPLOY_SCALED_TO_ZERO})
    k = Kubectl(namespace="kev1")
    assert k8slib.deploy_replicas_is(k, "eventrunner", 0)() is True
    assert k8slib.deploy_replicas_is(k, "eventrunner", 1)() is False
