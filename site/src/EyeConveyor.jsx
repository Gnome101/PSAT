import React, { useEffect, useRef, useState } from "react";
import { animate } from "animejs";

// ─── Canvas ────────────────────────────────────────────────────────────────
const VIEW_W = 1080;
const VIEW_H = 520;

// ─── Belt ──────────────────────────────────────────────────────────────────
const BELT_Y = 330;
const BELT_LEFT = 40;
const BELT_RIGHT = VIEW_W - 40;
const CONTRACT_W = 84;
const CONTRACT_H = 68;
const CONTRACT_CY = BELT_Y + CONTRACT_H / 2;

// ─── Station positions (x) ────────────────────────────────────────────────
const STATION1_X = 210;
const STATION2_X = 540;
const STATION3_X = 870;
const ENTRY_X = -CONTRACT_W - 20;
const EXIT_X = VIEW_W + 60;

// ─── Station 1: red x-ray turret ──────────────────────────────────────────
const S1_CEILING_Y = 46;
const S1_POD_CY = 122;
const S1_POD_W = 68;
const S1_POD_H = 44;
const S1_LENS_Y = S1_POD_CY + 22;
const S1_BEAM_HALF_WIDTH_DEG = 11;
const S1_BEAM_BASE_Y = BELT_Y + CONTRACT_H + 14;
const S1_BEAM_BASE_HALF_WIDTH_PX =
  Math.tan((S1_BEAM_HALF_WIDTH_DEG * Math.PI) / 180) * (S1_BEAM_BASE_Y - S1_LENS_Y);

// ─── Station 2: magnifying glass + audit document ─────────────────────────
const AUDIT_DOC_X = STATION2_X - 105;
const AUDIT_DOC_Y = 220;
const AUDIT_DOC_W = 58;
const AUDIT_DOC_H = 72;
const GLASS_IDLE_X = STATION2_X - 80;
const GLASS_IDLE_Y = -100;
const GLASS_AUDIT_X = AUDIT_DOC_X;
const GLASS_AUDIT_Y = AUDIT_DOC_Y;
const GLASS_CONTRACT_X = STATION2_X;
const GLASS_CONTRACT_Y = CONTRACT_CY - 6;

// ─── Station 3: green stamp + trap door ───────────────────────────────────
const S3_CEILING_Y = 46;
const S3_STAMP_IDLE_Y = 100; // top of stamp head at rest
const S3_STAMP_W = 96;
const S3_STAMP_H = 60;
// Descent needed to press stamp onto contract top (+ a bit of overlap)
const S3_ARM_DESCENT = BELT_Y - (S3_STAMP_IDLE_Y + S3_STAMP_H) + 6;

const S3_TRAP_Y = BELT_Y + CONTRACT_H + 4;
const S3_TRAP_W = CONTRACT_W + 28;
const S3_TRAP_LEFT = STATION3_X - S3_TRAP_W / 2;

// ─── Timings (ms) ─────────────────────────────────────────────────────────
const SLIDE_MS = 1200;

const S1_REVEAL_MS = 850;
const S1_TAIL_MS = 500;

const S2_APPROACH_MS = 420;
const S2_AUDIT_HOVER_MS = 380;
const S2_MOVE_TO_CONTRACT_MS = 480;
const S2_CONTRACT_HOVER_MS = 340;
const S2_MOVE_BACK_AUDIT_MS = 420;
const S2_AUDIT_RECHECK_MS = 240;
const S2_MOVE_FINAL_MS = 420;
const S2_MARK_HOLD_MS = 320;
const S2_RETRACT_MS = 420;
const S2_TAIL_MS = 200;

const S3_DOWN_MS = 320;
const S3_HOLD_MS = 360; // stamp presses on contract this long
const S3_UP_MS = 400;
const S3_TAIL_MS = 200;
const S3_TRAP_OPEN_MS = 300;
const S3_DROP_MS = 800;
const S3_TRAP_CLOSE_MS = 340;

const SPAWN_INTERVAL_MS = 3800;
const BROKEN_RATE = 0.4;

const NAMES = [
  "Vault", "Pool", "Oracle", "Router", "Minter", "Lending",
  "Treasury", "Registry", "Factory", "Bridge", "Staking", "Rewards",
  "Strategy", "Token", "Guard", "Proxy",
];

let seq = 1;
function makeContract() {
  return {
    id: seq++,
    name: NAMES[Math.floor(Math.random() * NAMES.length)],
    hash: Math.floor(Math.random() * 0xffffff).toString(16).padStart(6, "0"),
    broken: Math.random() < BROKEN_RATE,
    phase: "entering",
    audit: null, // "audited"
    stamp: null, // "approved" | "reject"
    state: { x: ENTRY_X, y: BELT_Y },
  };
}

function animateAsync(target, params) {
  return new Promise((resolve) => {
    animate(target, { ...params, onComplete: resolve });
  });
}

function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export default function EyeConveyor() {
  const svgRef = useRef(null);
  const contractElsRef = useRef(new Map());
  const contractsRef = useRef([]);
  const counters = useRef({ flagged: 0, cleared: 0 });
  const [flagged, setFlagged] = useState(0);
  const [cleared, setCleared] = useState(0);
  const [, setTick] = useState(0);

  // Station refs
  const s1BeamRef = useRef(null);
  const s2GlassRef = useRef(null);
  const s2AuditGlowRef = useRef(null);
  const s3StampRef = useRef(null);
  const s3TrapRef = useRef(null);

  useEffect(() => {
    let mounted = true;
    let spawnerId;

    function updateContractEl(c) {
      const el = contractElsRef.current.get(c.id);
      if (el) el.setAttribute("transform", `translate(${c.state.x.toFixed(1)}, ${c.state.y.toFixed(1)})`);
    }

    function forceRender() {
      setTick((t) => t + 1);
    }

    async function runStation1(c) {
      c.phase = c.broken ? "s1-reveal-broken" : "s1-reveal-safe";
      if (c.broken) {
        counters.current.flagged += 1;
        setFlagged(counters.current.flagged);
      } else {
        counters.current.cleared += 1;
        setCleared(counters.current.cleared);
      }
      forceRender();

      const beam = s1BeamRef.current;
      if (beam) {
        beam.style.transition = "opacity 140ms ease-out";
        beam.style.opacity = "0.95";
        setTimeout(() => {
          if (beam) {
            beam.style.transition = "opacity 220ms ease-in";
            beam.style.opacity = "0";
          }
        }, S1_REVEAL_MS);
      }

      await wait(S1_REVEAL_MS);
      c.phase = c.broken ? "s1-settled-broken" : "s1-settled-safe";
      forceRender();
      await wait(S1_TAIL_MS);
    }

    async function runStation2(c) {
      const glass = s2GlassRef.current;
      const auditGlow = s2AuditGlowRef.current;
      if (!glass) return;

      const moveGlassTo = (x, y, duration, ease) =>
        animateAsync(glass, { translateX: x, translateY: y, duration, ease });

      // Glass appears from above, descends to hover over the audit document
      glass.style.opacity = "1";
      await moveGlassTo(GLASS_AUDIT_X, GLASS_AUDIT_Y, S2_APPROACH_MS, "outCubic");

      if (auditGlow) auditGlow.style.opacity = "1";
      await wait(S2_AUDIT_HOVER_MS);
      if (auditGlow) auditGlow.style.opacity = "0";

      // Carry the reference over to the contract
      await moveGlassTo(GLASS_CONTRACT_X, GLASS_CONTRACT_Y, S2_MOVE_TO_CONTRACT_MS, "inOutSine");
      await wait(S2_CONTRACT_HOVER_MS);

      // Cross-check: back to the audit document
      await moveGlassTo(GLASS_AUDIT_X, GLASS_AUDIT_Y, S2_MOVE_BACK_AUDIT_MS, "inOutSine");
      if (auditGlow) auditGlow.style.opacity = "1";
      await wait(S2_AUDIT_RECHECK_MS);
      if (auditGlow) auditGlow.style.opacity = "0";

      // Final return to the contract — mark it
      await moveGlassTo(GLASS_CONTRACT_X, GLASS_CONTRACT_Y, S2_MOVE_FINAL_MS, "inOutSine");
      c.audit = "audited";
      forceRender();
      await wait(S2_MARK_HOLD_MS);

      // Retract up and fade out
      await animateAsync(glass, {
        translateX: GLASS_IDLE_X,
        translateY: GLASS_IDLE_Y,
        duration: S2_RETRACT_MS,
        ease: "inCubic",
      });
      glass.style.opacity = "0";

      await wait(S2_TAIL_MS);
    }

    async function runStation3(c) {
      const stamp = s3StampRef.current;

      if (stamp) {
        await animateAsync(stamp, {
          translateY: S3_ARM_DESCENT,
          duration: S3_DOWN_MS,
          ease: "inQuad",
        });
      }
      // On contact: the stamp mark appears on the contract and the stamp holds.
      c.stamp = c.broken ? "reject" : "approved";
      forceRender();

      await wait(S3_HOLD_MS);

      if (stamp) {
        await animateAsync(stamp, {
          translateY: 0,
          duration: S3_UP_MS,
          ease: "outCubic",
        });
      }
      await wait(S3_TAIL_MS);
    }

    async function runTrapDoorDrop(c) {
      const trap = s3TrapRef.current;
      if (trap) {
        await animateAsync(trap, {
          rotate: 95,
          duration: S3_TRAP_OPEN_MS,
          ease: "inQuad",
        });
      }
      await animateAsync(c.state, {
        y: VIEW_H + 60,
        duration: S3_DROP_MS,
        ease: "inQuad",
        onUpdate: () => updateContractEl(c),
      });
      if (trap) {
        await animateAsync(trap, {
          rotate: 0,
          duration: S3_TRAP_CLOSE_MS,
          ease: "outCubic",
        });
      }
    }

    async function spawnContract() {
      if (!mounted) return;

      const c = makeContract();
      contractsRef.current = [...contractsRef.current, c];
      forceRender();

      try {
        await animateAsync(c.state, {
          x: STATION1_X - CONTRACT_W / 2,
          duration: SLIDE_MS,
          ease: "inOutSine",
          onUpdate: () => updateContractEl(c),
        });
        if (!mounted) return;

        await runStation1(c);
        if (!mounted) return;

        await animateAsync(c.state, {
          x: STATION2_X - CONTRACT_W / 2,
          duration: SLIDE_MS,
          ease: "inOutSine",
          onUpdate: () => updateContractEl(c),
        });
        if (!mounted) return;

        await runStation2(c);
        if (!mounted) return;

        await animateAsync(c.state, {
          x: STATION3_X - CONTRACT_W / 2,
          duration: SLIDE_MS,
          ease: "inOutSine",
          onUpdate: () => updateContractEl(c),
        });
        if (!mounted) return;

        await runStation3(c);
        if (!mounted) return;

        if (c.broken) {
          c.phase = "rejected";
          forceRender();
          await runTrapDoorDrop(c);
        } else {
          c.phase = "approved";
          forceRender();
          await animateAsync(c.state, {
            x: EXIT_X,
            duration: SLIDE_MS * 1.25,
            ease: "inOutSine",
            onUpdate: () => updateContractEl(c),
          });
        }
      } finally {
        if (!mounted) return;
        contractsRef.current = contractsRef.current.filter((x) => x.id !== c.id);
        contractElsRef.current.delete(c.id);
        forceRender();
      }
    }

    spawnContract();
    spawnerId = setInterval(spawnContract, SPAWN_INTERVAL_MS);

    return () => {
      mounted = false;
      clearInterval(spawnerId);
    };
  }, []);

  const renderIcons = () => (
    <>
      <g className="rc-icon rc-icon-unknown">
        <circle cx={CONTRACT_W / 2} cy={CONTRACT_H / 2} r={3.5} fill="rgba(148, 163, 184, 0.45)" />
      </g>
      <g transform={`translate(${CONTRACT_W / 2}, ${CONTRACT_H / 2})`}>
        <g className="rc-icon rc-icon-safe">
          <text
            x={0}
            y={0}
            dominantBaseline="central"
            textAnchor="middle"
            fill="#22c55e"
            fontSize="44"
            fontWeight="900"
            fontFamily="Space Grotesk, system-ui, sans-serif"
            style={{ filter: "drop-shadow(0 0 6px rgba(34, 197, 94, 0.55))" }}
          >
            $
          </text>
        </g>
      </g>
      <g transform={`translate(${CONTRACT_W / 2}, ${CONTRACT_H / 2})`}>
        <g transform="scale(1.35)">
          <g className="rc-icon rc-icon-broken">
            <g transform="rotate(45)">
              <rect x={-19} y={-2} width={38} height={4} rx={2} fill="#f1f5f9" />
              <circle cx={-19} cy={0} r={3} fill="#f1f5f9" />
              <circle cx={19} cy={0} r={3} fill="#f1f5f9" />
            </g>
            <g transform="rotate(-45)">
              <rect x={-19} y={-2} width={38} height={4} rx={2} fill="#f1f5f9" />
              <circle cx={-19} cy={0} r={3} fill="#f1f5f9" />
              <circle cx={19} cy={0} r={3} fill="#f1f5f9" />
            </g>
            <circle cx={0} cy={-3} r={11} fill="#f1f5f9" />
            <rect x={-5.5} y={6.5} width={11} height={7.5} rx={2.2} fill="#f1f5f9" />
            <ellipse cx={-4} cy={-3} rx={2.4} ry={3.2} fill="#0b0f16" />
            <ellipse cx={4} cy={-3} rx={2.4} ry={3.2} fill="#0b0f16" />
            <path d="M -1 2.5 L 0 5 L 1 2.5 Z" fill="#0b0f16" />
          </g>
        </g>
      </g>
    </>
  );

  const renderContract = (c) => (
    <g
      key={c.id}
      ref={(el) => {
        if (el) contractElsRef.current.set(c.id, el);
        else contractElsRef.current.delete(c.id);
      }}
      className="rc-contract"
      data-status={mapPhaseToStatus(c.phase)}
      data-audit={c.audit || ""}
      data-stamp={c.stamp || ""}
      transform={`translate(${c.state.x}, ${c.state.y})`}
    >
      <rect
        className="rc-box"
        width={CONTRACT_W}
        height={CONTRACT_H}
        rx="10"
        fill="#1b2230"
        stroke="rgba(148, 163, 184, 0.22)"
        strokeWidth="1.2"
      />
      {renderIcons()}

      {/* Audit mark (after station 2): small blue circle with a checkmark */}
      <g className="audit-badge">
        <circle cx={CONTRACT_W / 2} cy={-14} r={10} fill="rgba(59, 130, 246, 0.22)" stroke="#60a5fa" strokeWidth="1.6" />
        <path
          d={`M ${CONTRACT_W / 2 - 4} -14 L ${CONTRACT_W / 2 - 1} -10 L ${CONTRACT_W / 2 + 5} -18`}
          stroke="#60a5fa"
          strokeWidth="2"
          fill="none"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </g>

      {/* Stamp mark (after station 3) */}
      <g
        className="stamp-mark-wrap"
        transform={`translate(${CONTRACT_W / 2}, ${CONTRACT_H / 2}) rotate(-14)`}
      >
        <g className="stamp-mark">
          <rect className="stamp-frame" x={-34} y={-11} width={68} height={22} rx={3} fill="none" strokeWidth={2.2} />
          <text
            className="stamp-text stamp-text-approved"
            x={0}
            y={5}
            textAnchor="middle"
            fontSize="11"
            fontWeight="900"
            letterSpacing="0.14em"
            fontFamily="JetBrains Mono, monospace"
          >
            APPROVED
          </text>
          <text
            className="stamp-text stamp-text-reject"
            x={0}
            y={5}
            textAnchor="middle"
            fontSize="11"
            fontWeight="900"
            letterSpacing="0.14em"
            fontFamily="JetBrains Mono, monospace"
          >
            REJECT
          </text>
        </g>
      </g>
    </g>
  );

  return (
    <div className="eye-scanner">
      <div className="eye-meta">
        <span className="eye-meta-dot" />
        <span className="eye-meta-text">Pipeline online</span>
        <span className="eye-meta-sep">·</span>
        <span className="eye-meta-flag">{flagged} flagged</span>
        <span className="eye-meta-sep">·</span>
        <span className="eye-meta-scanning">{cleared} cleared</span>
      </div>

      <svg
        ref={svgRef}
        className="eye-svg"
        viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="Three-stage audit pipeline"
      >
        <defs>
          <linearGradient id="v-beam" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="rgba(254, 202, 202, 0.95)" />
            <stop offset="40%" stopColor="rgba(248, 113, 113, 0.48)" />
            <stop offset="80%" stopColor="rgba(239, 68, 68, 0.34)" />
            <stop offset="100%" stopColor="rgba(127, 29, 29, 0.1)" />
          </linearGradient>
          <radialGradient id="v-lens" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#fecaca" />
            <stop offset="50%" stopColor="#ef4444" />
            <stop offset="100%" stopColor="#7f1d1d" />
          </radialGradient>
          <filter id="v-glow" x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur stdDeviation="3.5" result="b" />
            <feMerge>
              <feMergeNode in="b" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="#0a0e15" />

        <line
          x1={60}
          x2={VIEW_W - 60}
          y1={S1_CEILING_Y - 6}
          y2={S1_CEILING_Y - 6}
          stroke="rgba(148, 163, 184, 0.16)"
          strokeWidth="1.2"
        />

        {/* ─── STATION 1: red x-ray turret ─── */}
        <g>
          <rect x={STATION1_X - 22} y={S1_CEILING_Y} width={44} height={14} rx={2} fill="#14070a" stroke="#7f1d1d" strokeWidth={1.5} />
          <circle cx={STATION1_X - 14} cy={S1_CEILING_Y + 7} r={1.6} fill="#7f1d1d" />
          <circle cx={STATION1_X + 14} cy={S1_CEILING_Y + 7} r={1.6} fill="#7f1d1d" />
          <line x1={STATION1_X} y1={S1_CEILING_Y + 14} x2={STATION1_X} y2={S1_POD_CY - 22} stroke="#7f1d1d" strokeWidth="3" />
          <rect x={STATION1_X - S1_POD_W / 2} y={S1_POD_CY - S1_POD_H / 2} width={S1_POD_W} height={S1_POD_H} rx="8" fill="#1a0e0f" stroke="#dc2626" strokeWidth="2" />
          <line x1={STATION1_X - 18} x2={STATION1_X + 18} y1={S1_POD_CY - S1_POD_H / 2 + 6} y2={S1_POD_CY - S1_POD_H / 2 + 6} stroke="#7f1d1d" strokeWidth="1.4" />
          <circle cx={STATION1_X - 22} cy={S1_POD_CY - S1_POD_H / 2 + 6} r={2.5} fill="#ef4444">
            <animate attributeName="opacity" values="1;0.25;1" dur="0.9s" repeatCount="indefinite" />
          </circle>
          <rect x={STATION1_X - 14} y={S1_POD_CY + 8} width={28} height={12} rx={3} fill="#0b0f16" stroke="#dc2626" strokeWidth={1.5} />
          <circle cx={STATION1_X} cy={S1_LENS_Y} r={10} fill="url(#v-lens)" filter="url(#v-glow)" />
          <circle cx={STATION1_X - 2} cy={S1_LENS_Y - 3} r={2} fill="#fff1f2" opacity="0.95" />
        </g>

        {/* ─── Conveyor slab ─── */}
        <rect
          x={BELT_LEFT - 8}
          y={BELT_Y - 6}
          width={BELT_RIGHT - BELT_LEFT + 16}
          height={CONTRACT_H + 12}
          rx="10"
          fill="#10151f"
          stroke="rgba(148, 163, 184, 0.16)"
        />
        <line
          x1={BELT_LEFT - 8}
          x2={BELT_RIGHT + 8}
          y1={BELT_Y + CONTRACT_H + 6}
          y2={BELT_Y + CONTRACT_H + 6}
          stroke="rgba(148, 163, 184, 0.18)"
          strokeDasharray="4 6"
        />

        {/* Station 1 beam — over the belt */}
        <path
          ref={s1BeamRef}
          d={`M ${STATION1_X} ${S1_LENS_Y}
              L ${STATION1_X - S1_BEAM_BASE_HALF_WIDTH_PX} ${S1_BEAM_BASE_Y}
              L ${STATION1_X + S1_BEAM_BASE_HALF_WIDTH_PX} ${S1_BEAM_BASE_Y} Z`}
          fill="url(#v-beam)"
          filter="url(#v-glow)"
          style={{ opacity: 0, pointerEvents: "none" }}
        />

        {/* ─── STATION 2: audit document + magnifying glass ─── */}
        {/* Mount tab on the ceiling */}
        <rect x={AUDIT_DOC_X - 6} y={S1_CEILING_Y} width={12} height={10} rx={2} fill="#0b1324" stroke="#1e3a8a" strokeWidth={1.2} />
        <line x1={AUDIT_DOC_X} y1={S1_CEILING_Y + 10} x2={AUDIT_DOC_X} y2={AUDIT_DOC_Y - AUDIT_DOC_H / 2 - 4} stroke="#1e3a8a" strokeWidth={1.2} strokeDasharray="3 4" />

        {/* Audit document (reference material the glass keeps returning to) */}
        <g>
          {/* glow that pulses in while the glass is over it */}
          <rect
            ref={s2AuditGlowRef}
            x={AUDIT_DOC_X - AUDIT_DOC_W / 2 - 6}
            y={AUDIT_DOC_Y - AUDIT_DOC_H / 2 - 6}
            width={AUDIT_DOC_W + 12}
            height={AUDIT_DOC_H + 12}
            rx={6}
            fill="rgba(59, 130, 246, 0.12)"
            stroke="#60a5fa"
            strokeWidth={1.4}
            strokeDasharray="3 3"
            style={{ opacity: 0, transition: "opacity 160ms" }}
          />
          {/* paper */}
          <rect
            x={AUDIT_DOC_X - AUDIT_DOC_W / 2}
            y={AUDIT_DOC_Y - AUDIT_DOC_H / 2}
            width={AUDIT_DOC_W}
            height={AUDIT_DOC_H}
            rx={3}
            fill="#0b1324"
            stroke="#3b82f6"
            strokeWidth={1.6}
          />
          {/* folded-corner */}
          <path
            d={`M ${AUDIT_DOC_X + AUDIT_DOC_W / 2 - 10} ${AUDIT_DOC_Y - AUDIT_DOC_H / 2}
                L ${AUDIT_DOC_X + AUDIT_DOC_W / 2} ${AUDIT_DOC_Y - AUDIT_DOC_H / 2 + 10}
                L ${AUDIT_DOC_X + AUDIT_DOC_W / 2 - 10} ${AUDIT_DOC_Y - AUDIT_DOC_H / 2 + 10} Z`}
            fill="#1e3a8a"
          />
          {/* header bar */}
          <rect
            x={AUDIT_DOC_X - AUDIT_DOC_W / 2 + 4}
            y={AUDIT_DOC_Y - AUDIT_DOC_H / 2 + 14}
            width={AUDIT_DOC_W - 8}
            height={9}
            rx={1.5}
            fill="#1e3a8a"
          />
          <text
            x={AUDIT_DOC_X}
            y={AUDIT_DOC_Y - AUDIT_DOC_H / 2 + 20.5}
            textAnchor="middle"
            fill="#bfdbfe"
            fontSize={6}
            fontWeight={800}
            letterSpacing="0.18em"
            fontFamily="JetBrains Mono, monospace"
          >
            AUDIT
          </text>
          {/* content lines */}
          {[0, 1, 2, 3, 4].map((i) => {
            const y = AUDIT_DOC_Y - AUDIT_DOC_H / 2 + 30 + i * 5.5;
            const w = [AUDIT_DOC_W - 14, AUDIT_DOC_W - 20, AUDIT_DOC_W - 14, AUDIT_DOC_W - 26, AUDIT_DOC_W - 18][i];
            return (
              <line
                key={`al-${i}`}
                x1={AUDIT_DOC_X - AUDIT_DOC_W / 2 + 6}
                x2={AUDIT_DOC_X - AUDIT_DOC_W / 2 + 6 + w}
                y1={y}
                y2={y}
                stroke="#3b82f6"
                strokeOpacity={0.55}
                strokeWidth={1}
              />
            );
          })}
          {/* audit seal */}
          <circle cx={AUDIT_DOC_X + 10} cy={AUDIT_DOC_Y + AUDIT_DOC_H / 2 - 10} r={6} fill="none" stroke="#60a5fa" strokeWidth={1.2} />
          <path
            d={`M ${AUDIT_DOC_X + 7} ${AUDIT_DOC_Y + AUDIT_DOC_H / 2 - 10} L ${AUDIT_DOC_X + 9} ${AUDIT_DOC_Y + AUDIT_DOC_H / 2 - 8} L ${AUDIT_DOC_X + 13} ${AUDIT_DOC_Y + AUDIT_DOC_H / 2 - 12}`}
            stroke="#60a5fa"
            strokeWidth={1.5}
            fill="none"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </g>

        {/* Magnifying glass — floats down from above and sweeps between audit doc and contract */}
        <g
          ref={s2GlassRef}
          style={{
            transform: `translate(${GLASS_IDLE_X}px, ${GLASS_IDLE_Y}px)`,
            opacity: 0,
            transition: "opacity 220ms",
          }}
        >
          <circle cx={0} cy={0} r={24} fill="rgba(147, 197, 253, 0.1)" stroke="#60a5fa" strokeWidth={3.8} />
          <line x1={17} y1={17} x2={36} y2={36} stroke="#60a5fa" strokeWidth={5} strokeLinecap="round" />
          <rect
            x={32}
            y={30}
            width={12}
            height={6}
            rx={2}
            fill="#1e3a8a"
            transform="rotate(45 38 33)"
          />
          <circle cx={-7} cy={-7} r={5.5} fill="rgba(219, 234, 254, 0.45)" />
        </g>

        {/* ─── STATION 3: green stamp + trap door ─── */}
        <g>
          <rect x={STATION3_X - 22} y={S3_CEILING_Y} width={44} height={14} rx={2} fill="#052e14" stroke="#14532d" strokeWidth={1.5} />
          <circle cx={STATION3_X - 14} cy={S3_CEILING_Y + 7} r={1.6} fill="#14532d" />
          <circle cx={STATION3_X + 14} cy={S3_CEILING_Y + 7} r={1.6} fill="#14532d" />
        </g>

        {/* Long fixed stem — the stamp slides along this */}
        <rect
          x={STATION3_X - 3}
          y={S3_CEILING_Y + 14}
          width={6}
          height={BELT_Y - (S3_CEILING_Y + 14) - 2}
          fill="#14532d"
          stroke="#0b0f16"
          strokeWidth={1}
        />

        {/* Movable stamp head */}
        <g ref={s3StampRef} style={{ transform: "translate3d(0,0,0)" }}>
          <rect
            x={STATION3_X - S3_STAMP_W / 2}
            y={S3_STAMP_IDLE_Y}
            width={S3_STAMP_W}
            height={S3_STAMP_H}
            rx={8}
            fill="#052e14"
            stroke="#22c55e"
            strokeWidth="2.2"
          />
          <rect
            x={STATION3_X - S3_STAMP_W / 2 + 10}
            y={S3_STAMP_IDLE_Y + 10}
            width={S3_STAMP_W - 20}
            height={S3_STAMP_H - 20}
            rx={4}
            fill="none"
            stroke="#22c55e"
            strokeWidth="1.2"
            strokeDasharray="4 3"
          />
          <text
            x={STATION3_X}
            y={S3_STAMP_IDLE_Y + S3_STAMP_H / 2 + 5}
            textAnchor="middle"
            fill="#86efac"
            fontSize="11"
            fontWeight="900"
            letterSpacing="0.12em"
            fontFamily="JetBrains Mono, monospace"
          >
            STAMP
          </text>
        </g>

        {/* Trap door */}
        <g transform={`translate(${S3_TRAP_LEFT}, ${S3_TRAP_Y})`}>
          <g
            ref={s3TrapRef}
            style={{ transformBox: "fill-box", transformOrigin: "0% 50%", transform: "rotate(0deg)" }}
          >
            <rect x={0} y={-5} width={S3_TRAP_W} height={10} rx={2} fill="#1f2937" stroke="#0b0f16" strokeWidth={1.5} />
            <line x1={2} x2={S3_TRAP_W - 2} y1={-2} y2={-2} stroke="rgba(148, 163, 184, 0.25)" strokeDasharray="3 4" />
          </g>
        </g>
        <rect
          x={S3_TRAP_LEFT + 6}
          y={S3_TRAP_Y + 28}
          width={S3_TRAP_W - 12}
          height={14}
          rx={3}
          fill="none"
          stroke="rgba(239, 68, 68, 0.35)"
          strokeDasharray="2 3"
        />

        {/* Contracts */}
        <g>{contractsRef.current.map(renderContract)}</g>
      </svg>
    </div>
  );
}

function mapPhaseToStatus(phase) {
  switch (phase) {
    case "s1-reveal-safe":
      return "safe-reveal";
    case "s1-reveal-broken":
      return "broken-reveal";
    case "s1-settled-safe":
    case "approved":
      return "safe-settled";
    case "s1-settled-broken":
    case "rejected":
      return "broken-settled";
    default:
      return "unknown";
  }
}
