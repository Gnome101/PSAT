import { UpgradesPanel } from "../surface/inspector/UpgradesPanel.jsx";

export default function UpgradesTab({ detail }) {
  return (
    <UpgradesPanel
      upgradeHistory={detail?.upgrade_history}
      contractId={detail?.contract_id ?? null}
      companyName={detail?.company || null}
      contractAddress={detail?.address}
      contractName={detail?.run_name || detail?.contract_name}
      dependencies={detail?.dependencies?.dependencies || {}}
    />
  );
}
