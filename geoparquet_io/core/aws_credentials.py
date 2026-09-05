"""Resolve AWS credentials through botocore's standard credential chain.

gpio's DuckDB reads already authenticate with ``PROVIDER credential_chain``, so a
user signed in with ``aws sso login`` can read ``s3://`` fine. The object-store
writes -- ``gpio publish upload`` and the byte-for-byte copy of an already-correct
remote input (#849) -- used to resolve credentials by hand: environment keys plus a
configparser read of ``~/.aws/credentials``. That covered static keys and nothing
else, so the same command could succeed on the DuckDB branch and fail Access
Denied on the copy branch (#865).

This module is the single place that turns "profile + ambient environment" into
credentials, so both paths see the same chain: environment variables, the shared
credentials file, ``--aws-profile``, SSO, ``role_arn`` assume-role,
``credential_process``, and instance/container metadata.
"""

from __future__ import annotations

import botocore.session
from botocore.exceptions import BotoCoreError, ClientError

from geoparquet_io.core.logging_config import debug

#: Human-readable list of what the chain now covers, for credential hints.
CREDENTIAL_CHAIN_SOURCES = (
    "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (plus AWS_SESSION_TOKEN)",
    "~/.aws/credentials, the profile named by --aws-profile or 'default'",
    "an SSO session (aws sso login)",
    "an assume-role profile (role_arn with source_profile or web identity)",
    "credential_process",
    "EC2 instance / ECS task / EKS pod identity metadata",
)


def resolve_aws_credentials(profile: str | None = None) -> dict[str, str] | None:
    """Resolve AWS credentials and freeze them into obstore ``S3Store`` kwargs.

    Args:
        profile: AWS profile name, or None to let botocore choose (environment
            variables first, then the ``default`` profile). Naming a profile
            makes botocore skip the environment provider, which preserves gpio's
            long-standing precedence: ``--aws-profile`` wins over env keys.

    Returns:
        A dict with ``access_key_id`` and ``secret_access_key``, plus
        ``session_token`` when the chain produced a temporary session -- ready to
        splat into ``S3Store(...)``. None when botocore found no credentials at
        all, or the profile does not exist, or a provider failed; callers then
        pass no credential kwargs and obstore falls back to its own environment
        or anonymous resolution.

    Bound worth knowing: the returned credentials are *frozen*. Credentials from
    SSO, assume-role or ``credential_process`` expire (typically in an hour or
    more) and this snapshot does not refresh itself. A single upload or copy runs
    inside one session lifetime, so that is fine for gpio's use; a process that
    held a store open for longer than the credential lifetime would need to
    re-resolve.
    """
    try:
        credentials = botocore.session.Session(profile=profile).get_credentials()
    except (BotoCoreError, ClientError) as exc:
        debug(f"botocore could not resolve AWS credentials: {exc}")
        return None

    if credentials is None:
        return None

    frozen = credentials.get_frozen_credentials()
    if not (frozen.access_key and frozen.secret_key):
        return None

    resolved = {
        "access_key_id": frozen.access_key,
        "secret_access_key": frozen.secret_key,
    }
    if frozen.token:
        resolved["session_token"] = frozen.token
    return resolved


def resolve_aws_region(profile: str | None = None) -> str | None:
    """Resolve the AWS region botocore would use for ``profile``.

    Reads the profile's ``region`` from ``~/.aws/config`` (and
    ``AWS_DEFAULT_REGION``). Callers check ``AWS_REGION`` themselves, since
    botocore's ``region`` session variable does not.

    Args:
        profile: AWS profile name, or None for botocore's default

    Returns:
        The region string, or None when the profile is unknown or sets none.
    """
    try:
        region = botocore.session.Session(profile=profile).get_config_variable("region")
    except (BotoCoreError, ClientError) as exc:
        debug(f"botocore could not resolve an AWS region: {exc}")
        return None
    return str(region) if region else None
