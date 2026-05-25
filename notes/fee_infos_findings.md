# fee_infos audit findings

Date: 2026-05-23

## IDL verification

Source requested: `https://raw.githubusercontent.com/meteora-ag/dlmm-sdk/main/idls/lb_clmm.json`.

The local shell could not fetch GitHub because outbound sockets are blocked in this sandbox, so I verified the same Meteora `lb_clmm` IDL content through indexed web access and a mirrored IDL page for program `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.

`FeeInfo` is:

| Field | Type | Size | Offset within FeeInfo |
|---|---:|---:|---:|
| `fee_x_per_token_complete` | `u128` | 16 | 0 |
| `fee_y_per_token_complete` | `u128` | 16 | 16 |
| `fee_x_pending` | `u64` | 8 | 32 |
| `fee_y_pending` | `u64` | 8 | 40 |

Total `FeeInfo` size is 48 bytes. This matches the spike assumption exactly:

`u128(16) + u128(16) + u64(8) + u64(8) = 48B`, with `fee_x_pending` at `+32` and `fee_y_pending` at `+40`.

The IDL `PositionV2` field order is:

`lb_pair`, `owner`, `liquidity_shares`, `reward_infos`, `fee_infos`, then `lower_bin_id`, `upper_bin_id`, and trailing fields.

For the spike's fixed 70-bin interpretation, `fee_infos` begins at offset `4552`, and `[4552:7912]` is exactly `70 * 48 = 3360` bytes. That matches the current spike constants.

## Script run

Created `tools/audit_fee_infos.py` and ran:

```powershell
python tools/audit_fee_infos.py *> notes/fee_infos_audit.txt
```

The script did not reach RPC data. The captured output in `notes/fee_infos_audit.txt` shows:

`urllib.error.URLError: <urlopen error [WinError 10013] ... access to socket forbidden by access permissions>`

This is an environment/network permission failure, not a decode failure. Because of that, `notes/fee_infos_audit.txt` does not contain live account hex dumps in this run.

## Are fee_infos bytes genuinely zero?

Not conclusively re-verified by the new audit script, because Helius RPC was blocked before account data was returned.

Existing repo evidence in `notes/internal_nav_feasibility.md` says the earlier spike work verified `fee_infos` at offset `4552` as zero bytes and concluded LP fees were already claimed or zero. That prior result is consistent with the IDL layout being correct, but it is not a substitute for the requested fresh RPC hex dump.

## Is there a layout/decode bug?

For `FeeInfo` itself: no. The pending-fee offsets in `spike_internal_nav.py` match the IDL:

- `fee_x_pending`: `FEE_DATA_OFF + slot * 48 + 32`
- `fee_y_pending`: `FEE_DATA_OFF + slot * 48 + 40`

One separate query-filter nuance: the requested audit query used wallet memcmp offset `8`, but the IDL places `lb_pair` at account offset `8` and `owner` at account offset `40`. The spike uses offset `40`, which matches the IDL for PositionV2 owner filtering.

## Potential missed fees

No live non-zero fee bytes were fetched in this run, so there is no defensible SOL estimate from the new script output.

Using the IDL-matching pending offsets, the rough missed-fee estimate remains `0 SOL` unless a successful RPC dump shows non-zero `fee_x_pending` or `fee_y_pending` values.

## Recommendation

Do not change `tools/spike_internal_nav.py` for `FeeInfo` offsets. The layout matches the IDL.

To complete the byte-level audit, rerun `python tools/audit_fee_infos.py` in an environment where outbound HTTPS to `mainnet.helius-rpc.com` is allowed. The script is ready to print the requested `[4552:4600]`, `[7860:7920]`, and `[4552:7912]` byte counts once RPC access works.
