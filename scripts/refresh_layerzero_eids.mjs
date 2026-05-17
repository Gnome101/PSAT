#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const PACKAGE_SPEC = process.env.LAYERZERO_DEFINITIONS_PACKAGE || "@layerzerolabs/lz-definitions@3.1.2";
const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const outputPath = resolve(repoRoot, "data", "layerzero_eids.json");

async function loadLayerZeroDefinitions() {
  const workspace = await mkdtemp(join(tmpdir(), "psat-lz-defs-"));
  try {
    await writeFile(join(workspace, "package.json"), '{"private":true,"type":"module"}\n');
    execFileSync(
      "npm",
      ["install", PACKAGE_SPEC, "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund", "--silent"],
      {
        cwd: workspace,
        stdio: ["ignore", "ignore", "inherit"],
      },
    );
    const packageDir = join(workspace, "node_modules", "@layerzerolabs", "lz-definitions");
    const definitions = await import(pathToFileURL(join(packageDir, "dist", "index.mjs")).href);
    const packageJson = JSON.parse(await readFile(join(packageDir, "package.json"), "utf8"));
    return { definitions, packageJson, cleanup: () => rm(workspace, { recursive: true, force: true }) };
  } catch (error) {
    await rm(workspace, { recursive: true, force: true });
    throw error;
  }
}

function endpointIdToChainType(definitions, eid) {
  try {
    return definitions.endpointIdToChainType(eid);
  } catch {
    return null;
  }
}

function endpointRows(definitions) {
  const seen = new Set();
  const rows = [];
  for (const network of definitions.getNetworksForStage(definitions.Stage.MAINNET)) {
    try {
      const eid = definitions.networkToEndpointId(network, definitions.EndpointVersion.V2);
      if (!eid || seen.has(eid)) {
        continue;
      }
      seen.add(eid);
      rows.push({
        eid,
        network,
        chain: definitions.endpointIdToChain(eid),
        chain_type: endpointIdToChainType(definitions, eid),
      });
    } catch {
      // Some networks in the SDK are not deployed on every endpoint version.
    }
  }
  return rows.sort((a, b) => a.eid - b.eid);
}

const { definitions, packageJson, cleanup } = await loadLayerZeroDefinitions();
try {
  const payload = {
    source: packageJson.name,
    source_version: packageJson.version,
    package_spec: PACKAGE_SPEC,
    stage: "mainnet",
    endpoint_version: "v2",
    eids: endpointRows(definitions),
  };
  await writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`);
  console.log(`Wrote ${payload.eids.length} LayerZero endpoint IDs to ${outputPath}`);
} finally {
  await cleanup();
}
