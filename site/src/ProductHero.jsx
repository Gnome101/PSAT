import React from "react";
import HeroMesh from "./HeroMesh.jsx";
// Shelved alternatives:
// import HeroFlow from "./HeroFlow.jsx";
// import HeroSurface from "./HeroSurface.jsx";

export default function ProductHero({ form, setForm, onSubmit, loading }) {
  return (
    <section className="product-hero" aria-labelledby="product-hero-title">
      <div className="product-hero-inner">
        <div className="product-hero-copy">
          <p className="product-hero-eyebrow">Protocol Security Assessment Tool</p>
          <h1 id="product-hero-title" className="product-hero-title">
            Every privileged path in a protocol.
            <br />
            <span className="product-hero-accent">One surface.</span>
          </h1>
          <p className="product-hero-sub">
            Drop an address. PSAT discovers every contract, resolves proxy chains,
            maps owners and upgrade paths, scores risk, and shows which audits
            cover which functions — in minutes, not weeks.
          </p>

          <form className="product-hero-form" onSubmit={onSubmit}>
            <label className="product-hero-field product-hero-field-main">
              <span>Address or protocol</span>
              <input
                value={form.target}
                onChange={(e) => setForm((c) => ({ ...c, target: e.target.value }))}
                placeholder="0x... or etherfi"
                required
              />
            </label>
            <label className="product-hero-field">
              <span>Chain</span>
              <input
                value={form.chain}
                onChange={(e) => setForm((c) => ({ ...c, chain: e.target.value }))}
                placeholder="Optional"
              />
            </label>
            <label className="product-hero-field product-hero-field-narrow">
              <span>Limit</span>
              <input
                type="number"
                min="1"
                max="200"
                value={form.analyzeLimit}
                onChange={(e) => setForm((c) => ({ ...c, analyzeLimit: e.target.value }))}
              />
            </label>
            <button
              type="submit"
              className="product-hero-submit"
              disabled={loading || !form.target}
            >
              {loading ? "Starting..." : "Run analysis →"}
            </button>
          </form>
        </div>

        <div className="product-hero-preview" aria-hidden="true">
          <HeroMesh />
        </div>
      </div>
    </section>
  );
}
