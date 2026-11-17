"""Reconciliation of site scan receipts against the transfer ledger.

A *scan receipt* is a site-level event: a batch was scanned out of storage
(``departure``) or into storage (``arrival``) at a site and UTC instant. The
ledger (the registered batch record) states where and when the batch was
supposed to move. Reconciliation compares one pinned batch record against the
pinned set of its receipts and reports every discrepancy:

* ``missing_receipt``     no scan in a direction the ledger expects;
* ``duplicate_direction`` more than one scan in the same direction;
* ``site_mismatch``       a scan's site differs from the ledger endpoint;
* ``time_mismatch``       a scan's UTC instant differs from the ledger time.

The comparison is pure: it receives exactly the pinned rows and returns a
deterministically ordered list of findings, so a published report can always
be re-derived from its recorded inputs.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

#: Canonical direction order used for deterministic output.
DIRECTIONS = ("departure", "arrival")

_KIND_ORDER = {
    "missing_receipt": 0,
    "duplicate_direction": 1,
    "site_mismatch": 2,
    "time_mismatch": 3,
}


class ReconcileFailure(Exception):
    """The pinned inputs are inconsistent and no report can be produced."""


def _parse(value: str | datetime) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(value)


def reconcile(
    *,
    batch: dict[str, Any],
    scans: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Compare one pinned batch record with its pinned scan receipts.

    Args:
        batch: mapping with ``origin``, ``destination``, ``departed_at`` and
            ``arrived_at`` (datetimes or ISO strings).
        scans: mappings with ``scan_id``, ``direction``, ``site`` and
            ``scanned_at`` (datetimes or ISO strings).

    Returns:
        ``{"findings": [...]}`` sorted by (kind, direction, scan_id). An empty
        list means the receipts match the ledger exactly.

    Raises:
        ReconcileFailure: malformed pinned input.
    """
    try:
        expected = {
            "departure": (batch["origin"], _parse(batch["departed_at"])),
            "arrival": (batch["destination"], _parse(batch["arrived_at"])),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ReconcileFailure(
            f"pinned batch record is unusable: {exc}"
        ) from exc

    by_direction: dict[str, list[dict[str, Any]]] = {d: [] for d in DIRECTIONS}
    for scan in scans:
        direction = scan.get("direction")
        if direction not in by_direction:
            raise ReconcileFailure(
                f"scan {scan.get('scan_id')!r} has unknown direction"
                f" {direction!r}"
            )
        by_direction[direction].append(scan)

    findings: list[dict[str, Any]] = []
    for direction in DIRECTIONS:
        expected_site, expected_at = expected[direction]
        try:
            group = sorted(by_direction[direction], key=lambda s: s["scan_id"])
        except KeyError as exc:
            raise ReconcileFailure(f"scan is missing {exc}") from exc

        if not group:
            findings.append(
                {
                    "kind": "missing_receipt",
                    "direction": direction,
                    "scan_ids": [],
                    "expected_site": expected_site,
                    "expected_at": expected_at,
                    "message": (
                        f"no {direction} scan recorded; the ledger expects a"
                        f" {direction} at {expected_site}"
                    ),
                }
            )
        if len(group) > 1:
            findings.append(
                {
                    "kind": "duplicate_direction",
                    "direction": direction,
                    "scan_ids": [s["scan_id"] for s in group],
                    "message": (
                        f"{len(group)} {direction} scans recorded for one"
                        f" {direction} in the ledger"
                    ),
                }
            )
        for scan in group:
            scan_id = scan["scan_id"]
            if scan["site"] != expected_site:
                findings.append(
                    {
                        "kind": "site_mismatch",
                        "direction": direction,
                        "scan_ids": [scan_id],
                        "expected_site": expected_site,
                        "actual_site": scan["site"],
                        "message": (
                            f"scan {scan_id!r} reports site {scan['site']!r};"
                            f" the ledger expects {expected_site!r}"
                        ),
                    }
                )
            scanned_at = _parse(scan["scanned_at"])
            if scanned_at != expected_at:
                findings.append(
                    {
                        "kind": "time_mismatch",
                        "direction": direction,
                        "scan_ids": [scan_id],
                        "expected_at": expected_at,
                        "actual_at": scanned_at,
                        "message": (
                            f"scan {scan_id!r} reports {scanned_at.isoformat()};"
                            f" the ledger expects {expected_at.isoformat()}"
                        ),
                    }
                )

    findings.sort(
        key=lambda f: (
            _KIND_ORDER[f["kind"]],
            DIRECTIONS.index(f["direction"]),
            f["scan_ids"][0] if f["scan_ids"] else "",
        )
    )
    return {"findings": findings}
