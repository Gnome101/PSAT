const ADDRESS_EXPLORERS = {
  ethereum: "https://etherscan.io/address/",
  mainnet: "https://etherscan.io/address/",
  base: "https://basescan.org/address/",
  arbitrum: "https://arbiscan.io/address/",
  optimism: "https://optimistic.etherscan.io/address/",
  polygon: "https://polygonscan.com/address/",
};

export function blockExplorerAddressUrl(address, chain = "ethereum") {
  if (!address) return null;
  const base = ADDRESS_EXPLORERS[String(chain || "ethereum").toLowerCase()] || ADDRESS_EXPLORERS.ethereum;
  return `${base}${address}`;
}
