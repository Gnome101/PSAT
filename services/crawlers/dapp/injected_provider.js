// This script is injected into every page to spoof window.ethereum.
// It intercepts wallet RPC calls, spoofs balances, auto-signs auth messages,
// and captures transaction/approval requests without broadcasting them.

(function () {
  // Mutable so we can update chainId on wallet_switchEthereumChain
  let currentChainId = "__CHAIN_ID__";

  const CONFIG = {
    get chainId() { return currentChainId; },
    accounts: ["__ACCOUNT_ADDRESS__"],
    ethBalance: "__ETH_BALANCE__",
    tokenBalance: "__TOKEN_BALANCE__",
  };

  // ERC20 balanceOf(address) selector
  const BALANCE_OF_SELECTOR = "0x70a08231";

  // Track captured interactions for extraction
  const captured = [];

  function captureInteraction(type, data) {
    const entry = {
      type,
      timestamp: Date.now(),
      url: window.location.href,
      ...data,
    };
    captured.push(entry);
    // Post to page context so Playwright can pick it up
    window.postMessage({ source: "dapp-crawler", entry }, "*");
    console.log("[dapp-crawler] captured:", JSON.stringify(entry));
  }

  // Determine if an eth_signTypedData payload is a token permit
  function isPermitSignature(params) {
    try {
      const data = typeof params[1] === "string" ? JSON.parse(params[1]) : params[1];
      const primaryType = data.primaryType || "";
      return primaryType.toLowerCase().includes("permit");
    } catch {
      return false;
    }
  }

  const provider = {
    isMetaMask: true,
    isConnected: () => true,
    get chainId() { return CONFIG.chainId; },
    get networkVersion() { return String(parseInt(CONFIG.chainId, 16)); },
    selectedAddress: CONFIG.accounts[0],

    _events: {},

    on(event, handler) {
      if (!this._events[event]) this._events[event] = [];
      this._events[event].push(handler);
      return this;
    },

    removeListener(event, handler) {
      if (this._events[event]) {
        this._events[event] = this._events[event].filter((h) => h !== handler);
      }
      return this;
    },

    removeAllListeners(event) {
      if (event) {
        delete this._events[event];
      } else {
        this._events = {};
      }
      return this;
    },

    emit(event, ...args) {
      if (this._events[event]) {
        this._events[event].forEach((h) => h(...args));
      }
    },

    async request({ method, params }) {
      switch (method) {
        // -- Connection methods --
        case "eth_requestAccounts":
        case "eth_accounts":
          return CONFIG.accounts;

        case "eth_chainId":
          return CONFIG.chainId;

        case "net_version":
          return String(parseInt(CONFIG.chainId, 16));

        case "wallet_switchEthereumChain": {
          const requestedChainId = params?.[0]?.chainId;
          if (requestedChainId) {
            currentChainId = requestedChainId;
            console.log("[dapp-crawler] Switched chain to", requestedChainId);
            captureInteraction("switchChain", { chainId: requestedChainId });
            // Emit chainChanged event
            provider.emit("chainChanged", requestedChainId);
          }
          return null;
        }

        // -- Balance spoofing --
        case "eth_getBalance":
          return CONFIG.ethBalance;

        case "eth_call": {
          const callData = params?.[0]?.data || "";
          // Intercept balanceOf calls and return spoofed token balance
          if (callData.startsWith(BALANCE_OF_SELECTOR)) {
            const paddedBalance = CONFIG.tokenBalance.replace("0x", "").padStart(64, "0");
            return "0x" + paddedBalance;
          }
          // For other eth_call, relay to real RPC (handled by Playwright)
          window.postMessage(
            { source: "dapp-crawler-rpc-relay", method, params },
            "*"
          );
          // Return zero as fallback (Playwright will handle real relay)
          return "0x" + "0".repeat(64);
        }

        // -- Auth signatures (auto-sign) --
        case "personal_sign": {
          captureInteraction("personal_sign", {
            message: params[0],
            account: params[1],
          });
          // Signal Playwright to sign this message
          return new Promise((resolve) => {
            const id = "sig_" + Date.now();
            window.__dappCrawlerPendingSign = { id, method, params, resolve };
            window.postMessage(
              { source: "dapp-crawler-sign", id, method, params },
              "*"
            );
          });
        }

        case "eth_sign": {
          captureInteraction("eth_sign", {
            account: params[0],
            message: params[1],
          });
          return new Promise((resolve) => {
            const id = "sig_" + Date.now();
            window.__dappCrawlerPendingSign = { id, method, params, resolve };
            window.postMessage(
              { source: "dapp-crawler-sign", id, method, params },
              "*"
            );
          });
        }

        // -- Typed data signatures (capture permits, auto-sign auth) --
        case "eth_signTypedData":
        case "eth_signTypedData_v3":
        case "eth_signTypedData_v4": {
          const isPermit = isPermitSignature(params);
          captureInteraction("signTypedData", {
            method,
            isPermit,
            account: params[0],
            typedData: params[1],
          });

          if (isPermit) {
            // Permit signatures can authorize token spending.
            // Log but still sign (wallet has no real tokens anyway).
            console.log("[dapp-crawler] PERMIT signature detected!");
          }

          return new Promise((resolve) => {
            const id = "sig_" + Date.now();
            window.__dappCrawlerPendingSign = { id, method, params, resolve };
            window.postMessage(
              { source: "dapp-crawler-sign", id, method, params },
              "*"
            );
          });
        }

        // -- Transaction capture (NEVER broadcast) --
        case "eth_sendTransaction": {
          const tx = params[0];
          captureInteraction("sendTransaction", {
            to: tx.to,
            value: tx.value || "0x0",
            data: tx.data || "0x",
            gas: tx.gas,
          });
          // Return a fake tx hash so the DApp thinks it went through
          return "0x" + "0".repeat(63) + "1";
        }

        case "eth_sendRawTransaction": {
          captureInteraction("sendRawTransaction", {
            rawTx: params[0],
          });
          return "0x" + "0".repeat(63) + "1";
        }

        // -- Stubs for common methods --
        case "eth_blockNumber":
          return "0x" + Math.floor(Date.now() / 12000).toString(16);

        case "eth_gasPrice":
          return "0x" + (30e9).toString(16); // 30 gwei

        case "eth_estimateGas":
          return "0x5208"; // 21000

        case "eth_getTransactionReceipt":
          return null;

        case "wallet_addEthereumChain": {
          // DApps call this to add+switch to a new chain (e.g. Scroll, Arbitrum)
          const newChainId = params?.[0]?.chainId;
          if (newChainId) {
            currentChainId = newChainId;
            console.log("[dapp-crawler] Added and switched to chain", newChainId);
            captureInteraction("addChain", {
              chainId: newChainId,
              chainName: params[0].chainName || "unknown",
              rpcUrls: params[0].rpcUrls || [],
            });
            provider.emit("chainChanged", newChainId);
          }
          return null;
        }

        case "wallet_watchAsset":
          return true;

        default:
          console.log("[dapp-crawler] unhandled method:", method, params);
          return null;
      }
    },

    // Legacy send method (some DApps use this)
    send(methodOrPayload, callbackOrParams) {
      if (typeof methodOrPayload === "string") {
        return this.request({ method: methodOrPayload, params: callbackOrParams });
      }
      // JSON-RPC payload
      return this.request({
        method: methodOrPayload.method,
        params: methodOrPayload.params,
      }).then((result) => {
        if (typeof callbackOrParams === "function") {
          callbackOrParams(null, { id: methodOrPayload.id, jsonrpc: "2.0", result });
        }
        return result;
      });
    },

    sendAsync(payload, callback) {
      this.request({ method: payload.method, params: payload.params })
        .then((result) => callback(null, { id: payload.id, jsonrpc: "2.0", result }))
        .catch((err) => callback(err));
    },

    enable() {
      return this.request({ method: "eth_requestAccounts" });
    },
  };

  // Define as non-configurable so sites can't overwrite it
  Object.defineProperty(window, "ethereum", {
    value: provider,
    writable: false,
    configurable: false,
  });

  // Some DApps check for window.web3
  window.web3 = { currentProvider: provider };

  // --- EIP-6963: Multi Injected Provider Discovery ---
  // Modern DApps (Reown, RainbowKit, etc.) use this instead of window.ethereum
  const providerInfo = {
    uuid: "350670db-19fa-4704-a166-e52e178e6e97",
    name: "MetaMask",
    icon: "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyMTIiIGhlaWdodD0iMTg5Ij48cGF0aCBkPSJNMTg2LjYuNmwtNzYuNCA1Ni43IDEzLjktMzMuNHoiIGZpbGw9IiNFMjc2MUIiLz48L3N2Zz4=",
    rdns: "io.metamask",
  };

  const providerDetail = Object.freeze({
    info: Object.freeze(providerInfo),
    provider: provider,
  });

  // Announce provider when requested
  function announceProvider() {
    window.dispatchEvent(
      new CustomEvent("eip6963:announceProvider", {
        detail: providerDetail,
      })
    );
  }

  // Listen for DApp requests
  window.addEventListener("eip6963:requestProvider", () => {
    announceProvider();
  });

  // Also announce proactively on load
  announceProvider();

  console.log("[dapp-crawler] Honeypot provider injected for", CONFIG.accounts[0]);
  console.log("[dapp-crawler] EIP-6963 provider announced as MetaMask");
})();
