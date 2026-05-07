// Shared API response fixtures for vitest render tests. Shapes mirror
// what api.py returns and what the e2e specs in site/e2e/*.spec.js use,
// so tests stay self-consistent with the real frontend contracts.

export const ANALYSIS_LIST = [
  {
    job_id: "a",
    company: "etherfi",
    address: "0x1111111111111111111111111111111111111111",
    contract_name: "Weeth",
    risk_level: "low",
    is_proxy: true,
    upgrade_count: 2,
  },
  {
    job_id: "b",
    company: "etherfi",
    address: "0x2222222222222222222222222222222222222222",
    contract_name: "LiquidityPool",
    risk_level: "medium",
    upgrade_count: 0,
  },
  {
    job_id: "c",
    company: "lido",
    address: "0x3333333333333333333333333333333333333333",
    contract_name: "stETH",
    risk_level: "low",
  },
];

export const ETHERFI_COMPANY = {
  contracts: [
    {
      address: "0x1111111111111111111111111111111111111111",
      name: "Weeth",
      risk_level: "low",
      is_proxy: true,
      proxy_type: "ERC1967",
      upgrade_count: 2,
      control_model: "timelock",
      controllers: { owner: "0xMultiSig" },
      functions: [],
    },
    {
      address: "0x2222222222222222222222222222222222222222",
      name: "LiquidityPool",
      risk_level: "medium",
      upgrade_count: 0,
      controllers: {},
      functions: [],
    },
  ],
  ownership_hierarchy: [
    {
      owner: "0x9999999999999999999999999999999999999999",
      owner_name: "Treasury",
      owner_is_contract: true,
      contracts: [
        { address: "0x1111111111111111111111111111111111111111", name: "Weeth" },
        { address: "0x2222222222222222222222222222222222222222", name: "LiquidityPool" },
      ],
    },
  ],
  all_addresses: [],
};

export const COVERAGE_FIXTURE = {
  audit_count: 3,
  coverage: [
    {
      address: "0x1111111111111111111111111111111111111111",
      audit_count: 2,
      audits: [],
      last_audit: { auditor: "ABC", date: "2024-06" },
    },
    {
      address: "0x2222222222222222222222222222222222222222",
      audit_count: 0,
      audits: [],
    },
  ],
};

export const ANALYSIS_DETAIL = {
  job_id: "a",
  company: "etherfi",
  run_name: "Weeth",
  contract_name: "Weeth",
  address: "0x1111111111111111111111111111111111111111",
  contract_id: 1,
  source_verified: true,
  is_proxy: true,
  proxy_type: "ERC1967",
  upgrade_count: 2,
  risk_level: "low",
  controllers: { owner: "0xMultiSig" },
  functions: [],
  upgrade_history: { proxies: {}, total_upgrades: 0 },
};

export const PIPELINE_FIXTURE = {
  groups: [],
  recent_completed: [],
};

export const ADDRESS_LABELS = { labels: {} };
