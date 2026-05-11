import React from "react";
// import EyeConveyor from "./EyeConveyor.jsx";

export default function HomeHero({ onScrollToProtocols, protocolCount, contractCount }) {
  return (
    <section className="home-hero">
      <div className="home-hero-copy">
        <p className="home-hero-eyebrow">Protocol Security Assessment Tool</p>
        <h1 className="home-hero-title">
          Map the control <span className="accent">surface</span>
          <br />
          of any DeFi protocol.
        </h1>
        <p className="home-hero-lede">
          PSAT discovers every contract, privileged role and upgrade path for a protocol and
          lays them out in one navigable surface. Start from an address or a name — get audit
          coverage, control flow and risk posture in minutes.
        </p>
        <div className="home-hero-actions">
          <button className="home-hero-cta" onClick={onScrollToProtocols}>
            Browse analyzed protocols ↓
          </button>
        </div>
        <div className="home-hero-stats">
          <div className="home-hero-stat">
            <span className="home-hero-stat-value">{protocolCount ?? 0}</span>
            <span className="home-hero-stat-label">Protocols</span>
          </div>
          <div className="home-hero-stat">
            <span className="home-hero-stat-value">{contractCount ?? 0}</span>
            <span className="home-hero-stat-label">Contracts mapped</span>
          </div>
        </div>
      </div>

      {/* Pipeline animation disabled for now — re-enable by uncommenting the
          import above and the <EyeConveyor /> below. */}
      {/*
      <div className="home-hero-visual" aria-hidden="true">
        <EyeConveyor />
      </div>
      */}
    </section>
  );
}
