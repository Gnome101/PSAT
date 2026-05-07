import React, { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";

import {
  ADDRESS_GRAPH_COLUMNS,
  PRINCIPAL_COLUMNS,
  buildVisualAddressGraph,
  buildVisualPermissionGraph,
  layoutVisualAddressGraph,
  layoutVisualPermissionGraph,
  prettyFunctionName,
  shortenAddress,
  wrapText,
} from "./graph.js";
// Heavy graph + audit components are deferred so the home page (`/`)
// doesn't pay their bundle cost on first paint. The pre-split bundle
// was ~1.9 MB; each lazy chunk cuts a slice that only loads when its
// route or modal opens.
const DependencyGraphTab = lazy(() => import("./DependencyGraphTab.jsx"));
const ProtocolGraph = lazy(() => import("./ProtocolGraph.jsx"));
const RiskSurface = lazy(() => import("./RiskSurface.jsx"));
const ProtocolSurface = lazy(() => import("./ProtocolSurface.jsx"));
const AuditsTab = lazy(() => import("./AuditsTab.jsx"));
const AuditExtractionShelf = lazy(() => import("./AuditExtractionShelf.jsx"));
const AddressesModal = lazy(() => import("./AddressesModal.jsx"));
const AuditsAdminModal = lazy(() => import("./AuditsAdminModal.jsx"));
import { api } from "./api/client.js";
import { getPipeline as getAuditPipeline } from "./api/audits.js";
import ProductHero from "./ProductHero.jsx";
// Shelved assembly-line hero — kept on disk, not rendered.
// import SplashHero from "./SplashHero.jsx";
// import AssemblyLine from "./AssemblyLine.jsx";
import ProtocolLogo from "./ProtocolLogo.jsx";
import ProtocolRadar from "./ProtocolRadar.jsx";
import { computeProtocolScore } from "./protocolScore.js";
import { bytecodeVerifiedAudits } from "./auditCoverage.js";
import { StatCard } from "./ui/StatCard.jsx";
import { UpgradesPanel } from "./surface/inspector/UpgradesPanel.jsx";
import ErrorBoundary from "./ErrorBoundary.jsx";
import HamburgerMenu from "./HamburgerMenu.jsx";
import {
  TABS,
  buildLocationPath,
  formatJson,
  isAddress,
  normalizeTab,
  parseLocationPath,
} from "./router.js";
import { displayName } from "./displayName.js";
import SummaryTab from "./tabs/SummaryTab.jsx";
import PermissionsTab from "./tabs/PermissionsTab.jsx";
import PrincipalsTab from "./tabs/PrincipalsTab.jsx";
import UpgradesTab from "./tabs/UpgradesTab.jsx";
import RawTab from "./tabs/RawTab.jsx";
import GraphTab from "./tabs/GraphTab.jsx";
import ProxyWatcherPage from "./pages/ProxyWatcherPage.jsx";
import PipelineDashboard, { PIPELINE_STAGES } from "./pages/PipelineDashboard.jsx";
import ProtocolMonitoringPage from "./pages/ProtocolMonitoringPage.jsx";
import CompanyOverview from "./pages/CompanyOverview.jsx";
import LoadingFallback from "./LoadingFallback.jsx";

// SurfacePreview was a static SVG mini-map; we now embed the real
// ProtocolSurface component inline. File kept for possible reuse.
// import SurfacePreview from "./SurfacePreview.jsx";

// TODO: replace this with a real sign-in page + session-based auth. Options
// that fit our Fly deployment: (a) an identity-aware proxy sidecar such as
// oauth2-proxy or Pomerium that authenticates real users (Google/GitHub SSO)
// and injects X-PSAT-Admin-Key server-side so the key never touches a browser,
// or (b) an app-level user system with per-user login + roles (fastapi-users,
// a managed provider like WorkOS/Clerk, etc.). The window.prompt +
// localStorage pattern in api/client.js is a stopgap so admins can click
// buttons during local dev and early prod — a shared-secret bearer token
// sitting in every admin's browser, with no per-user audit log and no
// revocation story beyond rotating the key and logging everyone out.

// ---------------------------------------------------------------------------
// Company overview
// ---------------------------------------------------------------------------

// computeProtocolScore moved to ./protocolScore.js so the Surface sidebar
// can render the same score + radar in its empty Detail state.


function mergeProxyImpl(analyses) {
  const implByProxy = new Map();
  const mergedProxies = new Set();

  for (const a of analyses) {
    if (a.proxy_address) implByProxy.set(a.proxy_address.toLowerCase(), a);
  }

  const merged = [];
  for (const a of analyses) {
    if (a.proxy_address) continue; // skip standalone impl entries — they'll be merged into their proxy
    if (a.is_proxy && a.implementation_address) {
      const impl = implByProxy.get(a.address?.toLowerCase());
      if (impl) {
        merged.push({
          ...impl,
          proxy_address_display: a.address,
          proxy_type_display: a.proxy_type,
          display_name: displayName(a) || displayName(impl),
          rank_score: a.rank_score ?? impl.rank_score,
          company: a.company || impl.company,
        });
        mergedProxies.add(a.address?.toLowerCase());
        continue;
      }
    }
    merged.push(a);
  }
  // Add impl entries whose proxy wasn't in the list
  for (const a of analyses) {
    if (a.proxy_address && !mergedProxies.has(a.proxy_address.toLowerCase())) {
      merged.push(a);
    }
  }
  return merged;
}

// ---------------------------------------------------------------------------
// Protocol Monitoring
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Proxy Watcher (WIP)
// ---------------------------------------------------------------------------


function monitorJobScope(job) {
  const request = job?.request && typeof job.request === "object" ? job.request : {};

  if (job.stage === "dapp_crawl") {
    const urls = Array.isArray(request.dapp_urls) ? request.dapp_urls.filter(Boolean) : [];
    if (urls.length) {
      try {
        return new URL(urls[0]).host;
      } catch {
        return String(urls[0]);
      }
    }
  }

  if (job.stage === "defillama_scan" && request.defillama_protocol) {
    return `protocol ${request.defillama_protocol}`;
  }

  if (job.stage === "selection" && job.company) {
    return `ranking ${job.company}`;
  }

  if (job.company) return job.company;
  if (job.address) return shortenAddress(job.address);
  return job.job_id?.slice(0, 8) || "job";
}


// ---------------------------------------------------------------------------
// Runs list page
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

function RunsPage({ analyses, activeJobs, onSelect, onDiscoverMore, onSelectCompany }) {
  const [search, setSearch] = useState("");
  const protocolSectionRef = useRef(null);

  const { companies, standalone } = useMemo(() => {
    const map = new Map();
    const solo = [];
    for (const a of analyses) {
      const co = a.company;
      if (!co) { solo.push(a); continue; }
      if (!map.has(co)) map.set(co, { company: co, contracts: 0 });
      map.get(co).contracts++;
    }
    return { companies: [...map.values()].sort((a, b) => b.contracts - a.contracts), standalone: solo };
  }, [analyses]);

  const filtered = useMemo(() => {
    if (!search.trim()) return companies;
    const q = search.toLowerCase();
    return companies.filter((c) => c.company.toLowerCase().includes(q));
  }, [companies, search]);

  const contractCount = analyses.length;

  function scrollToProtocols() {
    protocolSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <div>
      {activeJobs.length > 0 && (
        <div className="active-jobs-bar" style={{ maxWidth: 1400, margin: "0 auto", padding: "0 24px" }}>
          {activeJobs.slice(0, 8).map((j) => {
            const stageIdx = PIPELINE_STAGES.indexOf(j.stage);
            const isDone = j.stage === "done" || j.status === "completed";
            const isFailed = j.status === "failed";
            return (
              <div key={j.job_id} className={`active-job-chip ${isDone ? "done" : ""} ${isFailed ? "err" : ""}`}>
                <span className="active-job-name">{j.name || j.company || j.address || "Job"}</span>
                <span className="active-job-stage">{j.stage}</span>
                <div className="mini-bar">
                  {PIPELINE_STAGES.map((s, i) => (
                    <div key={s} className={`mini-step ${isDone || i < stageIdx ? "done" : i === stageIdx ? "current" : ""}`} />
                  ))}
                </div>
              </div>
            );
          })}
          {activeJobs.length > 8 && <div className="active-job-chip" style={{ opacity: 0.6 }}>+{activeJobs.length - 8} more</div>}
        </div>
      )}

      <section ref={protocolSectionRef} id="protocols" className="home-protocol-section">
        <div className="home-protocol-header">
          <div>
            <p className="eyebrow" style={{ margin: 0 }}>Analyzed Protocols</p>
            <h2>All protocols</h2>
          </div>
          <div className="home-protocol-search">
            <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search protocols..." />
          </div>
        </div>

        {filtered.length > 0 ? (
          <div className="home-protocol-list">
            {filtered.map((c) => (
              <button key={c.company} className="home-protocol-row" onClick={() => onSelectCompany(c.company)}>
                <ProtocolLogo name={c.company} />
                <span className="home-protocol-row-name">{c.company}</span>
                <span className="home-protocol-row-count">{c.contracts} contracts</span>
                <span className="home-protocol-row-arrow" aria-hidden="true">→</span>
              </button>
            ))}
          </div>
        ) : (
          <p className="empty">{search ? "No protocols match your search." : "No analyses yet. Submit a protocol to get started."}</p>
        )}

        {standalone.length > 0 && (
          <section className="panel" style={{ marginTop: 32 }}>
            <h3 style={{ marginBottom: 12 }}>Standalone analyses</h3>
            <div className="runs-table">
              <div className="runs-table-header">
                <span style={{ flex: 2 }}>Contract</span>
                <span style={{ flex: 3 }}>Address</span>
              </div>
              {standalone.map((a) => (
                <button key={a.job_id || a.run_name} className="runs-table-row" onClick={() => onSelect(a.job_id)}>
                  <span className="runs-cell-name" style={{ flex: 2 }}>{a.contract_name || a.run_name || "Unknown"}</span>
                  <span className="mono runs-cell-addr" style={{ flex: 3 }}>{a.address || ""}</span>
                </button>
              ))}
            </div>
          </section>
        )}
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

export default function App() {
  const [analyses, setAnalyses] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [viewMode, setViewMode] = useState(() => parseLocationPath(window.location.pathname).mode);
  const [companyName, setCompanyName] = useState(() => { const r = parseLocationPath(window.location.pathname); return r.mode === "company" ? r.value : null; });
  const [companyTab, setCompanyTab] = useState(() => parseLocationPath(window.location.pathname).companyTab || "overview");
  const [menuOpen, setMenuOpen] = useState(false);
  // Initialize activeTab from the URL so /address/<addr>/upgrades loads
  // the upgrades tab directly on refresh — otherwise activeTab starts as
  // "summary" and only flips once loadAnalysis resolves, which means
  // UpgradesTab briefly doesn't mount and any URL-dependent tab content
  // can race with loadAnalysis' state batch.
  const [activeTab, setActiveTab] = useState(() => parseLocationPath(window.location.pathname).tab);
  const [job, setJob] = useState(null);
  const [activeJobs, setActiveJobs] = useState([]);
  const [form, setForm] = useState({ target: "", name: "", chain: "", analyzeLimit: "5" });
  const [formOpen, setFormOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const analysesRef = useRef([]);
  const activeTabRef = useRef(parseLocationPath(window.location.pathname).tab);
  const doneTimerRef = useRef(null);

  useEffect(() => { analysesRef.current = analyses; }, [analyses]);
  useEffect(() => { activeTabRef.current = activeTab; }, [activeTab]);
  useEffect(() => {
    function handleKey(e) { if (e.key === "Escape" && menuOpen) setMenuOpen(false); }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [menuOpen]);

  function navigate(path, mode) {
    const m = mode || parseLocationPath(path).mode;
    setViewMode(m);
    if (m !== "company") setCompanyName(null);
    window.history.pushState({}, "", path);
  }

  function openCompany(name) {
    setCompanyName(name);
    setCompanyTab("overview");
    setViewMode("company");
    window.history.pushState({}, "", `/company/${encodeURIComponent(name)}`);
  }

  function navigateCompanyTab(tab, params = {}) {
    setCompanyTab(tab);
    const suffix = tab === "overview" ? "" : `/${tab}`;
    const query = new URLSearchParams();
    if (params.focus) query.set("focus", params.focus);
    if (params.fn) query.set("fn", params.fn);
    if (params.score) query.set("score", params.score);
    const search = query.toString();
    window.history.pushState({}, "", `/company/${encodeURIComponent(companyName)}${suffix}${search ? `?${search}` : ""}`);
  }

  async function loadAnalysis(runId, options = {}) {
    try {
      const payload = await api(`/api/analyses/${encodeURIComponent(runId)}`);
      const nextTab = normalizeTab(options.tab ?? activeTabRef.current);
      setSelectedRun(runId);
      setSelectedDetail(payload);
      setActiveTab(nextTab);
      setViewMode("run");
      const address = payload?.address || payload?.contract_analysis?.subject?.address;
      const path = buildLocationPath(runId, address, nextTab);
      window.history[options.history === "replace" ? "replaceState" : "pushState"]({}, "", path);
      return payload;
    } catch (err) {
      console.error("Failed to load analysis:", runId, err);
      return null;
    }
  }

  async function refreshAnalyses() {
    const payload = await api("/api/analyses");
    const filtered = payload.filter((a) => a.address);
    setAnalyses(filtered);
    return filtered;
  }

  // Initial load
  useEffect(() => {
    function handlePopState() {
      const route = parseLocationPath(window.location.pathname);
      setViewMode(route.mode);
      if (route.mode === "company") {
        setCompanyName(route.value);
        setCompanyTab(route.companyTab || "overview");
      } else if (route.mode === "run" || route.mode === "address") {
        setCompanyName(null);
        // For /address/<x> we pass the address directly: /api/analyses/<name>
        // falls back to a by-address lookup and returns the run whose primary
        // address is <x>. This bypasses the merged /api/analyses list — which
        // hides the proxy run behind the impl run and would otherwise cause
        // /address/<proxy>/upgrades to load the impl's detail (where the
        // impl run's upgrade_history doesn't include its own proxy chain).
        loadAnalysis(route.value, { tab: route.tab, history: "replace" });
      } else {
        setCompanyName(null);
      }
    }

    refreshAnalyses().catch(() => null);
    const route = parseLocationPath(window.location.pathname);
    if (route.mode === "company") {
      setCompanyName(route.value);
      setCompanyTab(route.companyTab || "overview");
    } else if (route.mode === "run" || route.mode === "address") {
      loadAnalysis(route.value, { tab: route.tab, history: "replace" });
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  // Job polling — scoped to the current submission's job tree
  useEffect(() => {
    if (!job?.job_id) return undefined;
    let stopped = false;
    let timer;

    // Collect all job IDs belonging to this submission's tree
    function getJobTree(allJobs, rootId) {
      const ids = new Set([rootId]);
      let changed = true;
      while (changed) {
        changed = false;
        for (const j of allJobs) {
          if (ids.has(j.job_id)) continue;
          if (ids.has(j.request?.parent_job_id) || ids.has(j.request?.root_job_id)) {
            ids.add(j.job_id);
            changed = true;
          }
        }
      }
      return ids;
    }

    async function poll() {
      if (stopped) return;
      try {
        const allJobs = await api("/api/jobs");
        if (stopped) return;
        const now = new Date();
        const treeIds = getJobTree(allJobs, job.job_id);
        const treeJobs = allJobs.filter((j) => treeIds.has(j.job_id));
        const visible = treeJobs.filter((j) =>
          j.status === "queued" || j.status === "processing" ||
          ((j.status === "completed" || j.status === "failed") && j.updated_at && now - new Date(j.updated_at) < 30000)
        );
        setActiveJobs(visible);
        const parent = allJobs.find((j) => j.job_id === job.job_id);
        if (parent) setJob(parent);
        const stillRunning = treeJobs.some((j) => j.status === "queued" || j.status === "processing");
        if (!stillRunning && !doneTimerRef.current) {
          doneTimerRef.current = setTimeout(async () => {
            stopped = true; clearInterval(timer); setActiveJobs([]); doneTimerRef.current = null;
            await refreshAnalyses();
          }, 5000);
        }
      } catch {}
    }

    poll();
    timer = setInterval(poll, 2000);
    return () => { stopped = true; clearInterval(timer); if (doneTimerRef.current) { clearTimeout(doneTimerRef.current); doneTimerRef.current = null; } };
  }, [job?.job_id]);

  async function submit(event) {
    event.preventDefault();
    if (!form.target) return;
    setLoading(true);
    try {
      const target = form.target.trim();
      const payload = isAddress(target)
        ? { address: target, name: form.name.trim() || null }
        : {
            company: target,
            chain: form.chain.trim() || null,
            analyze_limit: Number.parseInt(form.analyzeLimit, 10) || 5,
          };
      const nextJob = await api("/api/analyze", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      setJob(nextJob);
      setFormOpen(false);
      navigate("/monitor", "monitor");
    } finally { setLoading(false); }
  }

  async function discoverMore(company) {
    try {
      const nextJob = await api("/api/analyze", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ company, analyze_limit: 5 }) });
      setJob(nextJob);
    } catch (err) { console.error("Failed to start discovery:", err); }
  }

  function handleTabChange(tab) {
    const nextTab = normalizeTab(tab);
    setActiveTab(nextTab);
    const address = selectedDetail?.address || selectedDetail?.contract_analysis?.subject?.address;
    const path = buildLocationPath(selectedRun, address, nextTab);
    window.history.pushState({}, "", path);
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  const isDetail = viewMode === "run" || viewMode === "address";
  const isMonitor = viewMode === "monitor";
  const isCompany = viewMode === "company";
  const isProxies = viewMode === "proxies";

  const detailContent = selectedDetail ? {
    summary: <SummaryTab detail={selectedDetail} />,
    permissions: <PermissionsTab detail={selectedDetail} />,
    principals: <PrincipalsTab detail={selectedDetail} />,
    graph: <GraphTab detail={selectedDetail} />,
    dependencies: (
      <Suspense fallback={<LoadingFallback label="Loading graph..." />}>
        <DependencyGraphTab data={selectedDetail?.dependency_graph_viz} runName={selectedRun} />
      </Suspense>
    ),
    upgrades: <UpgradesTab detail={selectedDetail} />,
    raw: <RawTab detail={selectedDetail} />,
  } : {};

  return (
    <ErrorBoundary>
      {/* Top nav */}
      <nav className={`top-nav ${isCompany && companyTab === "surface" ? "top-nav-dark" : ""}`}>
        <div className="top-nav-left">
          <button className="hamburger-btn" onClick={() => setMenuOpen(!menuOpen)} aria-label="Menu">
            <span className="hamburger-icon" />
          </button>
          <button className="top-nav-brand" onClick={() => { navigate("/", "default"); refreshAnalyses(); }}>PSAT</button>
          {companyName && <span className="top-nav-context">{companyName}</span>}
        </div>
        <div className="top-nav-right">
          <button className="top-nav-submit-btn" onClick={() => setFormOpen(!formOpen)}>
            {formOpen ? "Close" : "+ New Analysis"}
          </button>
        </div>
      </nav>

      {/* Hamburger drawer */}
      {menuOpen && (
        <HamburgerMenu
          onClose={() => setMenuOpen(false)}
          viewMode={viewMode}
          companyName={companyName}
          companyTab={companyTab}
          onNavigate={(path, mode) => { navigate(path, mode); refreshAnalyses(); }}
          onNavigateCompanyTab={navigateCompanyTab}
        />
      )}

      {/* Submit form dropdown */}
      {formOpen && (
        <div className="submit-dropdown">
          <form className="submit-form" onSubmit={submit}>
            <label><span>Address or company</span><input value={form.target} onChange={(e) => setForm((c) => ({ ...c, target: e.target.value }))} placeholder="0x... or etherfi" required /></label>
            <label><span>Run name</span><input value={form.name} onChange={(e) => setForm((c) => ({ ...c, name: e.target.value }))} placeholder="Optional" /></label>
            <label><span>Chain</span><input value={form.chain} onChange={(e) => setForm((c) => ({ ...c, chain: e.target.value }))} placeholder="Optional" /></label>
            <label><span>Analyze limit</span><input type="number" min="1" max="200" value={form.analyzeLimit} onChange={(e) => setForm((c) => ({ ...c, analyzeLimit: e.target.value }))} /></label>
            <button type="submit" disabled={loading}>{loading ? "Starting..." : "Run"}</button>
          </form>
        </div>
      )}

      {/* Page content */}
      {isMonitor && <PipelineDashboard />}
      {isProxies && <ProxyWatcherPage />}

      {isDetail && selectedDetail && (
        <div className="page">
          {/* Proxy banner */}
          {(selectedDetail.proxy_address_display || selectedDetail.proxy_address) && (
            <div className="proxy-banner">
              Proxy at <span className="mono">{shortenAddress(selectedDetail.proxy_address_display || selectedDetail.proxy_address)}</span>
              {selectedDetail.proxy_type_display && <span className="chip alt" style={{ marginLeft: 8, padding: "2px 8px", fontSize: 10 }}>{selectedDetail.proxy_type_display}</span>}
              <span style={{ margin: "0 6px" }}>&rarr;</span>
              Implementation at <span className="mono">{shortenAddress(selectedDetail.address)}</span>
            </div>
          )}
          <section className="panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Contract Analysis</p>
                <h2>{displayName(selectedDetail) || selectedRun || "Unknown"}</h2>
              </div>
              <div className="meta-stack">
                <div className="mono">{selectedDetail.proxy_address_display || selectedDetail.address || ""}</div>
                <div>{selectedDetail.summary?.control_model || selectedDetail.contract_analysis?.summary?.control_model || ""}</div>
              </div>
            </div>
            <div className="tabs">
              {TABS.map((tab) => (
                <button key={tab} className={`tab ${activeTab === tab ? "active" : ""}`} onClick={() => handleTabChange(tab)}>
                  {tab === "raw" ? "Raw JSON" : tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>
            <div className="tab-panel active">{detailContent[activeTab]}</div>
          </section>
        </div>
      )}

      {isCompany && companyName && companyTab === "overview" && (
        <CompanyOverview
          companyName={companyName}
          onSelectContract={(jobId) => loadAnalysis(jobId, { history: "push" })}
          onNavigateToSurface={(params) => navigateCompanyTab("surface", params)}
        />
      )}
      {isCompany && companyName && companyTab === "surface" && (
        <div className="fullscreen-surface">
          <Suspense fallback={<LoadingFallback label="Loading control surface..." />}>
            <ProtocolSurface companyName={companyName} />
          </Suspense>
        </div>
      )}
      {isCompany && companyName && companyTab === "graph" && (
        <div className="page" style={{ height: "calc(100vh - 52px)", display: "flex", flexDirection: "column" }}>
          <div className="protocol-graph-wrapper" style={{ flex: 1, minHeight: 0 }}>
            <Suspense fallback={<LoadingFallback label="Loading graph..." />}>
              <ProtocolGraph companyName={companyName} />
            </Suspense>
          </div>
        </div>
      )}
      {isCompany && companyName && companyTab === "risk" && (
        <div className="page">
          <Suspense fallback={<LoadingFallback label="Loading risk matrix..." />}>
            <RiskSurface companyName={companyName} />
          </Suspense>
        </div>
      )}
      {isCompany && companyName && companyTab === "monitoring" && (
        <ProtocolMonitoringPage companyName={companyName} />
      )}
      {isCompany && companyName && companyTab === "audits" && (
        <Suspense fallback={<LoadingFallback label="Loading audits..." />}>
          <AuditsTab
            companyName={companyName}
            focusAuditId={new URLSearchParams(window.location.search).get("audit")}
          />
        </Suspense>
      )}

      {!isDetail && !isMonitor && !isCompany && !isProxies && (
        <>
          <ProductHero form={form} setForm={setForm} onSubmit={submit} loading={loading} />
          <RunsPage
            analyses={analyses}
            activeJobs={activeJobs}
            onSelect={(runId) => loadAnalysis(runId, { history: "push" })}
            onDiscoverMore={discoverMore}
            onSelectCompany={openCompany}
          />
        </>
      )}
    </ErrorBoundary>
  );
}
