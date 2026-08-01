from __future__ import annotations

from dataclasses import dataclass
import json
import re


COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
PINNED_IMAGE = re.compile(
    r"^ghcr\.io/[a-z0-9_.-]+/[a-z0-9_.-]+@sha256:[0-9a-f]{64}$"
)


@dataclass(frozen=True, slots=True)
class ReleaseManifest:
    commit: str
    image: str
    alembic: str


def verify_release_manifest(
    raw: str,
    *,
    expected_commit: str,
    compatible_revisions: frozenset[str],
) -> ReleaseManifest:
    """Validate the immutable artifact/schema tuple before deployment."""
    decoded = json.loads(raw)
    if not isinstance(decoded, dict) or set(decoded) != {"commit", "image", "alembic"}:
        raise ValueError("unexpected release manifest fields")
    if not COMMIT_SHA.fullmatch(expected_commit):
        raise ValueError("expected commit must be a full Git SHA")
    if decoded["commit"] != expected_commit:
        raise ValueError("release manifest belongs to another commit")
    if not isinstance(decoded["image"], str) or not PINNED_IMAGE.fullmatch(decoded["image"]):
        raise ValueError("image must be pinned by a sha256 digest")
    if decoded["alembic"] not in compatible_revisions:
        raise ValueError("database revision is outside the compatibility window")
    return ReleaseManifest(**decoded)
