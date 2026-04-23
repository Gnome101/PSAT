// Block-explorer URL helper — shared across any component that renders
// a contract address. Defaults to Etherscan when the chain tag is
// missing, since 95% of PSAT's traffic is Ethereum mainnet.

const EXPLORER_BASE = {
  ethereum: "https://etherscan.io",
  mainnet: "https://etherscan.io",
  arbitrum: "https://arbiscan.io",
  optimism: "https://optimistic.etherscan.io",
  base: "https://basescan.org",
  polygon: "https://polygonscan.com",
  bsc: "https://bscscan.com",
};

export function explorerAddressUrl(address, chain) {
  if (!address) return null;
  const base = EXPLORER_BASE[(chain || "ethereum").toLowerCase()] || EXPLORER_BASE.ethereum;
  return `${base}/address/${address}`;
}
