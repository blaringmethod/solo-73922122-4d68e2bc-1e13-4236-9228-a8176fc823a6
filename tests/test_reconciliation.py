"""Scan receipts and single-batch reconciliation."""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone

from conftest import batch, ingest
from fastapi.testclient import TestClient

from app.main import create_app
from app.store import Store

# B1 ledger: lab -> hub, departed 00:00, arrived 01:00.
# B2 ledger: hub -> ref, departed 02:00, arrived 03:00.


def seed(client):
    ingest(
        client,
        sources=["S1"],
        samples=[
            {"sample_id": "S1", "batch_nos": ["B1"]},
            {"sample_id": "S2", "batch_nos": ["B2"]},
        ],
        batches=[
            batch("B1", "lab", "hub", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
            batch("B2", "hub", "ref", "2026-01-01T02:00:00Z", "2026-01-01T03:00:00Z"),
        ],
    )


def scan(scan_id, batch_no="B1", direction="departure", site="lab",
         at="2026-01-01T00:00:00Z"):
    return {
        "scan_id": scan_id,
        "batch_no": batch_no,
        "direction": direction,
        "site": site,
        "scanned_at": at,
    }


def matching_pair(prefix="SC"):
    """Receipts that exactly match B1's ledger record."""
    return [
        scan(f"{prefix}1"),
        scan(f"{prefix}2", direction="arrival", site="hub",
             at="2026-01-01T01:00:00Z"),
    ]


def post_scans(client, scans):
    return client.post("/scans", json={"scans": scans})


def reconcile(client, batch_no="B1"):
    return client.post(f"/batches/{batch_no}/reconciliations")


# ------------------------------------------------------------------ receipts


def test_record_and_list_scans(client):
    seed(client)
    response = post_scans(client, matching_pair())
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["recorded"] == 2
    assert body["duplicates"] == 0
    assert [s["outcome"] for s in body["scans"]] == ["recorded", "recorded"]
    assert body["invalidated_reconciliation_ids"] == []

    listed = client.get("/batches/B1/scans")
    assert listed.status_code == 200
    assert [s["scan_id"] for s in listed.json()] == ["SC1", "SC2"]
    assert listed.json()[0]["scanned_at"] == "2026-01-01T00:00:00Z"
    assert client.get("/batches/NOPE/scans").status_code == 404


def test_identical_rescan_is_idempotent(client):
    seed(client)
    post_scans(client, matching_pair())
    again = post_scans(client, matching_pair())
    assert again.status_code == 201
    body = again.json()
    assert body["recorded"] == 0
    assert body["duplicates"] == 2
    assert [s["outcome"] for s in body["scans"]] == ["duplicate", "duplicate"]
    assert len(client.get("/batches/B1/scans").json()) == 2


def test_conflicting_rescan_rejected_and_request_is_atomic(client):
    seed(client)
    post_scans(client, matching_pair())
    response = post_scans(
        client,
        [
            scan("SC9"),  # new and valid, must be rolled back too
            scan("SC1", site="WRONG"),  # same id, different content
        ],
    )
    assert response.status_code == 409
    assert "different content" in response.json()["message"]
    assert [s["scan_id"] for s in client.get("/batches/B1/scans").json()] == [
        "SC1",
        "SC2",
    ]


def test_scan_for_unknown_batch_rejected(client):
    seed(client)
    response = post_scans(client, [scan("SC1", batch_no="NOPE")])
    assert response.status_code == 422
    assert "unknown batch" in response.json()["message"]


def test_scans_require_dataset(client):
    assert post_scans(client, [scan("SC1")]).status_code == 404


def test_duplicate_scan_id_within_payload_rejected(client):
    seed(client)
    response = post_scans(client, [scan("SC1"), scan("SC1")])
    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"


def test_bad_direction_rejected(client):
    seed(client)
    response = post_scans(client, [scan("SC1", direction="sideways")])
    assert response.status_code == 422


def test_naive_scan_timestamp_treated_as_utc(client):
    seed(client)
    response = post_scans(client, [scan("SC1", at="2026-01-01T00:00:00")])
    assert response.status_code == 201
    stored = client.get("/batches/B1/scans").json()[0]
    assert stored["scanned_at"] == "2026-01-01T00:00:00Z"


# -------------------------------------------------------------- reconciling


def test_clean_reconciliation_is_matched(client):
    seed(client)
    post_scans(client, matching_pair())
    response = reconcile(client)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "complete"
    assert body["matched"] is True
    assert body["findings"] == []
    assert body["finding_count"] == 0
    assert body["stale"] is False
    # The report pins the exact inputs it used.
    assert body["ledger"]["batch_no"] == "B1"
    assert body["ledger"]["version"] == 1
    assert [s["scan_id"] for s in body["scans"]] == ["SC1", "SC2"]


def test_missing_receipts(client):
    seed(client)
    body = reconcile(client).json()
    assert body["matched"] is False
    kinds = [(f["kind"], f["direction"]) for f in body["findings"]]
    assert kinds == [
        ("missing_receipt", "departure"),
        ("missing_receipt", "arrival"),
    ]
    arrival = body["findings"][1]
    assert arrival["expected_site"] == "hub"
    assert arrival["expected_at"] == "2026-01-01T01:00:00Z"


def test_missing_arrival_only(client):
    seed(client)
    post_scans(client, [scan("SC1")])
    body = reconcile(client).json()
    kinds = [(f["kind"], f["direction"]) for f in body["findings"]]
    assert kinds == [("missing_receipt", "arrival")]


def test_duplicate_direction(client):
    seed(client)
    post_scans(
        client,
        matching_pair() + [scan("SC3")],  # second departure scan
    )
    body = reconcile(client).json()
    assert [(f["kind"], f["direction"]) for f in body["findings"]] == [
        ("duplicate_direction", "departure")
    ]
    assert body["findings"][0]["scan_ids"] == ["SC1", "SC3"]


def test_site_mismatch(client):
    seed(client)
    post_scans(
        client,
        [
            scan("SC1", site="other-lab"),
            scan("SC2", direction="arrival", site="hub",
                 at="2026-01-01T01:00:00Z"),
        ],
    )
    body = reconcile(client).json()
    assert [(f["kind"], f["direction"]) for f in body["findings"]] == [
        ("site_mismatch", "departure")
    ]
    finding = body["findings"][0]
    assert finding["expected_site"] == "lab"
    assert finding["actual_site"] == "other-lab"
    assert finding["scan_ids"] == ["SC1"]


def test_time_mismatch(client):
    seed(client)
    post_scans(
        client,
        [
            scan("SC1"),
            scan("SC2", direction="arrival", site="hub",
                 at="2026-01-01T01:05:00Z"),
        ],
    )
    body = reconcile(client).json()
    assert [(f["kind"], f["direction"]) for f in body["findings"]] == [
        ("time_mismatch", "arrival")
    ]
    finding = body["findings"][0]
    assert finding["expected_at"] == "2026-01-01T01:00:00Z"
    assert finding["actual_at"] == "2026-01-01T01:05:00Z"


def test_multiple_discrepancies_have_deterministic_order(client):
    seed(client)
    post_scans(
        client,
        [
            scan("SC2", site="wrong-site"),  # departure: site + time ok? no:
            scan("SC1", at="2026-01-01T00:30:00Z"),  # departure: time mismatch
            # no arrival scan at all
        ],
    )
    body = reconcile(client).json()
    assert [(f["kind"], f["direction"]) for f in body["findings"]] == [
        ("missing_receipt", "arrival"),
        ("duplicate_direction", "departure"),
        ("site_mismatch", "departure"),
        ("time_mismatch", "departure"),
    ]


def test_reconcile_unknown_batch_is_404(client):
    seed(client)
    assert reconcile(client, "NOPE").status_code == 404


def test_reconcile_requires_dataset(client):
    assert reconcile(client).status_code == 404


# ------------------------------------------- publication atomicity / races


def test_scan_during_computation_blocks_publish(client, monkeypatch):
    seed(client)
    post_scans(client, [scan("SC1")])
    from app import reconcile as reconcile_module

    real = reconcile_module.reconcile

    def scan_lands_mid_compute(**kwargs):
        response = post_scans(
            client,
            [scan("SC2", direction="arrival", site="hub",
                  at="2026-01-01T01:00:00Z")],
        )
        assert response.status_code == 201
        return real(**kwargs)

    monkeypatch.setattr(reconcile_module, "reconcile", scan_lands_mid_compute)
    response = reconcile(client)
    assert response.status_code == 409
    detail = response.json()
    assert detail["code"] == "reconciliation_changed"
    recon_id = detail["details"]["reconciliation_id"]

    # The reconciliation is recorded as failed with zero findings.
    rows = client.get("/reconciliations").json()["reconciliations"]
    failed = next(r for r in rows if r["reconciliation_id"] == recon_id)
    assert failed["status"] == "failed"
    assert failed["finding_count"] == 0
    read_back = client.get(f"/reconciliations/{recon_id}")
    assert read_back.status_code == 500
    assert read_back.json()["code"] == "reconciliation_failed"

    # A retry sees the full receipt set and publishes.
    monkeypatch.undo()
    assert reconcile(client).json()["matched"] is True


def test_correction_during_computation_blocks_publish(client, monkeypatch):
    seed(client)
    post_scans(client, matching_pair())
    from app import reconcile as reconcile_module

    real = reconcile_module.reconcile

    def correction_lands_mid_compute(**kwargs):
        response = client.post(
            "/batches/B1/corrections",
            json={
                "seen_version": 1,
                "origin": "lab",
                "destination": "hub",
                "departed_at": "2026-01-01T00:30:00Z",
                "arrived_at": "2026-01-01T01:00:00Z",
            },
        )
        assert response.status_code == 200
        return real(**kwargs)

    monkeypatch.setattr(
        reconcile_module, "reconcile", correction_lands_mid_compute
    )
    response = reconcile(client)
    assert response.status_code == 409
    assert response.json()["code"] == "reconciliation_changed"
    assert "version" in response.json()["message"]


def test_failed_reconciliation_publishes_no_partial_findings(
    client, monkeypatch
):
    seed(client)
    post_scans(client, matching_pair())
    from app import reconcile as reconcile_module

    def boom(**kwargs):
        raise RuntimeError("simulated computation failure")

    monkeypatch.setattr(reconcile_module, "reconcile", boom)
    response = reconcile(client)
    assert response.status_code == 500
    detail = response.json()
    assert detail["code"] == "reconciliation_failed"
    recon_id = detail["details"]["reconciliation_id"]

    rows = client.get("/reconciliations").json()["reconciliations"]
    failed = next(r for r in rows if r["reconciliation_id"] == recon_id)
    assert failed["status"] == "failed"
    assert failed["finding_count"] == 0
    assert client.get(f"/reconciliations/{recon_id}").status_code == 500

    monkeypatch.undo()
    assert reconcile(client).status_code == 201


def test_concurrent_scan_and_reconciliation_never_half_published(client):
    """A scan landing mid-reconciliation must either be included, block
    publication, or mark the report stale — never produce a half result."""
    seed(client)
    post_scans(client, matching_pair())
    errors: list[BaseException] = []

    def record():
        try:
            post_scans(client, [scan("SC3", direction="arrival", site="hub",
                                     at="2026-01-01T01:00:00Z")])
        except BaseException as exc:  # pragma: no cover - diagnostic
            errors.append(exc)

    thread = threading.Thread(target=record)
    thread.start()
    response = reconcile(client)
    thread.join()
    assert not errors

    if response.status_code == 201:
        # The response was rendered at some point during the race; the
        # persisted state after both commits must be coherent: either the
        # report pinned SC3, or it was marked stale by SC3's arrival.
        body = client.get(
            f"/reconciliations/{response.json()['reconciliation_id']}"
        ).json()
        if body["stale"]:
            assert body["invalidations"]
        else:
            assert "SC3" in {s["scan_id"] for s in body["scans"]}
    else:
        # Publication was refused; the failed report holds no findings.
        assert response.status_code == 409
        recon_id = response.json()["details"]["reconciliation_id"]
        rows = client.get("/reconciliations").json()["reconciliations"]
        failed = next(r for r in rows if r["reconciliation_id"] == recon_id)
        assert failed["status"] == "failed"
        assert failed["finding_count"] == 0


# ---------------------------------------------------------------- staleness


def test_new_scan_marks_report_stale(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()
    assert report["matched"] is True

    response = post_scans(
        client,
        [scan("SC3", direction="arrival", site="hub",
              at="2026-01-01T01:00:00Z")],
    )
    assert response.json()["invalidated_reconciliation_ids"] == [
        report["reconciliation_id"]
    ]

    # The stale report stays readable: findings plus staleness status.
    stale = client.get(f"/reconciliations/{report['reconciliation_id']}")
    assert stale.status_code == 200
    body = stale.json()
    assert body["stale"] is True
    assert body["matched"] is True  # findings reflect the pinned inputs
    assert len(body["invalidations"]) == 1
    assert "SC3" in body["invalidations"][0]["reason"]

    listing = client.get("/reconciliations").json()["reconciliations"]
    summary = next(
        r for r in listing
        if r["reconciliation_id"] == report["reconciliation_id"]
    )
    assert summary["stale"] is True


def test_identical_rescan_does_not_stale_report(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()

    again = post_scans(client, matching_pair())
    assert again.json()["duplicates"] == 2
    assert again.json()["invalidated_reconciliation_ids"] == []
    body = client.get(f"/reconciliations/{report['reconciliation_id']}").json()
    assert body["stale"] is False


def test_other_batch_scan_does_not_stale_report(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()

    response = post_scans(client, [scan("SC9", batch_no="B2")])
    assert response.json()["invalidated_reconciliation_ids"] == []
    body = client.get(f"/reconciliations/{report['reconciliation_id']}").json()
    assert body["stale"] is False


def test_correction_marks_report_stale(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()

    response = client.post(
        "/batches/B1/corrections",
        json={
            "seen_version": 1,
            "origin": "lab",
            "destination": "hub",
            "departed_at": "2026-01-01T00:30:00Z",
            "arrived_at": "2026-01-01T01:00:00Z",
        },
    )
    assert response.status_code == 200
    assert response.json()["invalidated_reconciliation_ids"] == [
        report["reconciliation_id"]
    ]

    body = client.get(f"/reconciliations/{report['reconciliation_id']}").json()
    assert body["stale"] is True
    assert "corrected from version 1 to version 2" in (
        body["invalidations"][0]["reason"]
    )
    # The pinned ledger record remains the one the report used.
    assert body["ledger"]["version"] == 1
    assert body["ledger"]["departed_at"] == "2026-01-01T00:00:00Z"


def test_other_batch_correction_does_not_stale_report(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()

    response = client.post(
        "/batches/B2/corrections",
        json={
            "seen_version": 1,
            "origin": "hub",
            "destination": "ref",
            "departed_at": "2026-01-01T02:30:00Z",
            "arrived_at": "2026-01-01T03:00:00Z",
        },
    )
    assert response.status_code == 200
    assert response.json()["invalidated_reconciliation_ids"] == []
    body = client.get(f"/reconciliations/{report['reconciliation_id']}").json()
    assert body["stale"] is False


def test_fresh_reconciliation_after_stale_reflects_new_state(client):
    seed(client)
    post_scans(client, matching_pair())
    old = reconcile(client).json()
    post_scans(
        client,
        [scan("SC3", direction="arrival", site="hub",
              at="2026-01-01T01:00:00Z")],
    )
    assert client.get(
        f"/reconciliations/{old['reconciliation_id']}"
    ).json()["stale"] is True

    fresh = reconcile(client).json()
    assert fresh["stale"] is False
    assert fresh["matched"] is False
    assert [(f["kind"], f["direction"]) for f in fresh["findings"]] == [
        ("duplicate_direction", "arrival")
    ]
    assert [s["scan_id"] for s in fresh["scans"]] == ["SC1", "SC2", "SC3"]


def test_reconciliation_list_and_unknown_report(client):
    seed(client)
    post_scans(client, matching_pair())
    report = reconcile(client).json()
    listing = client.get("/reconciliations").json()["reconciliations"]
    assert len(listing) == 1
    assert listing[0]["reconciliation_id"] == report["reconciliation_id"]
    assert listing[0]["batch_no"] == "B1"
    assert listing[0]["status"] == "complete"
    assert listing[0]["finding_count"] == 0
    assert listing[0]["stale"] is False
    assert client.get("/reconciliations/9999").status_code == 404


def test_pending_reconciliation_from_crash_is_marked_failed(tmp_path):
    path = str(tmp_path / "crash-recon.db")
    store = Store(path)
    store.ingest(
        samples=[{"sample_id": "S1", "contaminated": True, "batch_nos": ["B1"]}],
        batches=[
            {
                "batch_no": "B1",
                "origin": "lab",
                "destination": "hub",
                "departed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "arrived_at": datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
            }
        ],
        source_sample_ids=[],
    )
    recon_id, _batch, _scans = store.accept_reconciliation(batch_no="B1")
    # Simulate a process dying before publication (startup recovery ignores
    # very recent pending rows, so age this one past the grace period).
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE reconciliations"
            " SET accepted_at='2000-01-01T00:00:00.000000+00:00' WHERE id=?",
            (recon_id,),
        )
    assert store.mark_interrupted() == 1

    with TestClient(create_app(path)) as client:
        row = next(
            r
            for r in client.get("/reconciliations").json()["reconciliations"]
            if r["reconciliation_id"] == recon_id
        )
        assert row["status"] == "failed"
        assert row["finding_count"] == 0
