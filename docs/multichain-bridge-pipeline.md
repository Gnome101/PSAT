# Multichain Bridge Pipeline

PSAT's existing pipeline answers: which principals can control which contract
functions on one chain. The multichain bridge layer should preserve that model
and add the missing chain-boundary view:

- which contracts are deployed on which chains;
- which functions send messages or value across chains;
- which functions receive remote messages and apply local effects;
- which principals can change peers, trusted remotes, endpoints, routers, or
  bridge adapters;
- which source-chain effect corresponds to a destination-chain effect.

## Discovery

Inventory discovery should retain both shapes:

- grouped protocol view for people: one contract name with a `deployments`
  array across chains;
- analysis view for workers: one concrete `(address, chain)` row per
  deployment.

The discovery worker flattens grouped deployments before writing `contracts`
rows, so the selector can rank and queue each deployment independently. Source
fetching must be chain-aware; an Arbitrum row should fetch verified source with
Arbitrum's Etherscan chain id, not Ethereum mainnet.

## Static Analysis

Static analysis adds bridge-oriented effect labels beside the existing control
and value labels:

- `cross_chain_message`: sends or handles a bridge/message payload;
- `bridge_transfer`: combines a cross-chain message with token/accounting
  movement;
- `bridge_receive`: receives and applies a remote message;
- `bridge_config_update`: changes bridge routing, peers, remotes, endpoints, or
  related configuration.
- `bridge_security_config`: changes bridge verification/security configuration
  such as LayerZero DVNs/ULN/message libraries/executors/options or Hyperlane
  ISMs.

Protocol standards such as `LayerZero`, `CCIP`, `Wormhole`, `Hyperlane`,
`Axelar`, and `Connext` are surfaced as contract standards when the code shape
matches their send/receive/config patterns.

Static analysis emits a normalized bridge-signal block inside
`contract_analysis`. The company API exposes those unresolved signals as
`bridge_static_context`; the user-facing `bridge_context` name is reserved for
active runtime context with resolved routes. The static block groups send,
receive, config, security-config, and upgrade functions, then attaches the
existing upgradeability result:

- `upgrade_context.code_has_upgrade_path`: the code exposes an implementation
  update path or proxy shell shape;
- `upgrade_context.admin_paths`: functions that can change implementation or
  upgrade-control state;
- `upgrade_context.can_change_bridge_logic`: true when bridge behavior can
  change through an implementation update path detected in the analyzed code.

This is intentionally separate from decoded route state. It says "this bridge
path/config can be changed by upgrade" before the resolver has decoded the
remote peer, DVN set, ISM, or token-pool mapping.

## Resolution

Resolution should turn bridge config state into explicit cross-chain edges.
Examples:

- LayerZero `trustedRemoteLookup`, `setPeer`, `_lzSend`, `lzReceive`;
- LayerZero DVN/ULN/message-library/executor/enforced-options configuration;
- CCIP router and `chainSelector` mappings;
- Hyperlane mailbox/domain mappings;
- Hyperlane interchain security modules (ISMs), default ISMs, validator
  announce, and routing/aggregation ISM configuration;
- Wormhole emitter or VAA verification paths;
- Axelar gateway/gas-service executable paths.

For LayerZero V2, PSAT keeps the candidate endpoint IDs in
`data/layerzero_eids.json`. That file is generated from LayerZero's npm SDK
package instead of a hand-maintained shortlist:

```bash
node scripts/refresh_layerzero_eids.mjs
```

The runtime resolver uses those SDK-derived EIDs to batch-probe selected OApps
for configured peers, then performs the authoritative onchain reads for message
libraries, executor limits, and ULN/DVN config. Only that resolved output should
be returned as `bridge_context`. The SDK tells PSAT which EIDs exist; the OApp
and Endpoint contracts tell PSAT which routes are actually configured.

The important output is not just "external call"; it is:

`source chain contract/function -> bridge protocol -> remote chain peer -> remote effect`

## Policy And Surface

Policy should attach principals to cross-chain powers:

- who can send or relay;
- who can receive and mutate local state;
- who can change the remote peer or bridge endpoint;
- who can pause, upgrade, or redirect the bridge adapter.

The protocol surface can then show bridge contracts as cross-chain routers
instead of isolated utility contracts. A source-chain value-out action and a
destination-chain mint/unlock/credit action should read as one connected system
when the configured peer can be resolved.
