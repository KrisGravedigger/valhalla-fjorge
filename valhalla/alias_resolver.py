"""
Wallet alias resolver.

Applies wallet_aliases.json to positions.csv:
- target_wallet  -> canonical alias (or original if no alias defined)
- original_wallet -> immutable original wallet ID (set once, never overwritten)

Idempotent: safe to run multiple times. Uses original_wallet as lookup key
so re-running after config changes works correctly.
"""
import json
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def apply_aliases(csv_path: Path, aliases_path: Path) -> None:
    """Apply wallet aliases to positions CSV in-place.

    Adds/updates original_wallet column and rewrites target_wallet
    with canonical alias names where configured.
    """
    if not csv_path.exists():
        return
    if not aliases_path.exists():
        logger.debug("No wallet_aliases.json found, skipping alias resolution")
        return

    with open(aliases_path, encoding='utf-8') as f:
        raw = json.load(f)

    # Build flat map: wallet_identifier -> canonical_name
    # Keys can be short IDs (YYYYMMDD_XXXX) or full Solana addresses (44-char base58)
    # Skip keys starting with "_" (comments/examples)
    alias_map: dict[str, str] = {}
    for canonical, members in raw.items():
        if canonical.startswith('_'):
            continue
        if not isinstance(members, list):
            continue
        for member in members:
            alias_map[member] = canonical

    if not alias_map:
        logger.debug("wallet_aliases.json has no aliases defined, skipping")
        return

    df = pd.read_csv(csv_path)

    if 'original_wallet' not in df.columns:
        df['original_wallet'] = ''

    # Ensure string type, fill NaN
    df['original_wallet'] = df['original_wallet'].fillna('').astype(str)
    df['target_wallet'] = df['target_wallet'].fillna('').astype(str)
    if 'target_wallet_address' in df.columns:
        df['target_wallet_address'] = df['target_wallet_address'].fillna('').astype(str)
    else:
        df['target_wallet_address'] = ''

    # First pass: set original_wallet where not yet set (idempotency guard)
    mask_empty = df['original_wallet'] == ''
    df.loc[mask_empty, 'original_wallet'] = df.loc[mask_empty, 'target_wallet']

    # Second pass: apply alias
    # Try lookup by short ID (original_wallet) first, then by full Solana address
    def resolve(row) -> str:
        ow = row['original_wallet']
        if ow in alias_map:
            return alias_map[ow]
        full_addr = row['target_wallet_address']
        if full_addr and full_addr in alias_map:
            return alias_map[full_addr]
        return ow

    df['target_wallet'] = df.apply(resolve, axis=1)

    applied = (
        df['original_wallet'].isin(alias_map) |
        df['target_wallet_address'].isin(alias_map)
    ).sum()
    if applied > 0:
        logger.info(f"Wallet aliases applied: {applied} positions remapped")

    df.to_csv(csv_path, index=False)
