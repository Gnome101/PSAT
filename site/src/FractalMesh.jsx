import React, { useEffect, useRef } from "react";

/*
 * FractalMesh — continuous ASCII minesweeper background that doubles as a
 * playable game on click.
 *
 * Pattern mode (default):
 *   The auto-clicker is replaced with a pattern cycle. Carved-out shapes
 *   (BITCOIN / DEFI / SATOSHI / a low-res world map) are drawn into the
 *   field of `0`s by setting selected cells to a transparent "carved"
 *   state. Each pattern: draws → holds → fades back → next pattern.
 *
 * Play mode (after user clicks):
 *   Pattern cycle stops. Left-click reveals (and cascades), right-click
 *   toggles a flag. Click a mine and it explodes — all mines reveal, the
 *   board resets, and pattern mode resumes after a short hold. After 25s
 *   of inactivity in play mode, pattern mode resumes too.
 */

const COLS = 200;
const ROWS = 52;
const CELL_W = 20;
const CELL_H = 26;
const MINE_DENSITY = 0.13;

const REVEAL_STAGGER_MS = 28;
const IDLE_TIMEOUT_MS = 25000;
const GAME_OVER_HOLD_MS = 2600;

// Pattern cycle pacing
const PATTERN_DRAW_STAGGER_MS = 6;   // ms between pattern cell reveals
const PATTERN_HOLD_MS = 4500;         // how long the fully-drawn pattern stays
const PATTERN_FADE_STAGGER_MS = 3;    // re-cover speed

// 5x7 pixel font — uppercase + a few specials. `#` = on, `.` = off.
const FONT_5x7 = {
  A: [".###.", "#...#", "#...#", "#####", "#...#", "#...#", "#...#"],
  B: ["####.", "#...#", "#...#", "####.", "#...#", "#...#", "####."],
  C: [".###.", "#...#", "#....", "#....", "#....", "#...#", ".###."],
  D: ["####.", "#...#", "#...#", "#...#", "#...#", "#...#", "####."],
  E: ["#####", "#....", "#....", "####.", "#....", "#....", "#####"],
  F: ["#####", "#....", "#....", "####.", "#....", "#....", "#...."],
  H: ["#...#", "#...#", "#...#", "#####", "#...#", "#...#", "#...#"],
  I: [".###.", "..#..", "..#..", "..#..", "..#..", "..#..", ".###."],
  N: ["#...#", "##..#", "#.#.#", "#.#.#", "#.#.#", "#..##", "#...#"],
  O: [".###.", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."],
  S: [".####", "#....", "#....", ".###.", "....#", "....#", "####."],
  T: ["#####", "..#..", "..#..", "..#..", "..#..", "..#..", "..#.."],
  " ": [".....", ".....", ".....", ".....", ".....", ".....", "....."],
};

function scaleBitmap(rows, scale) {
  const out = [];
  for (const row of rows) {
    const expanded = row.split("").map((c) => c.repeat(scale)).join("");
    for (let i = 0; i < scale; i++) out.push(expanded);
  }
  return out;
}

function renderText(text, scale) {
  const baseRows = 7;
  const rows = Array.from({ length: baseRows }, () => "");
  for (let i = 0; i < text.length; i++) {
    const ch = text[i].toUpperCase();
    const glyph = FONT_5x7[ch] || FONT_5x7[" "];
    for (let r = 0; r < baseRows; r++) {
      rows[r] += glyph[r];
      if (i < text.length - 1) rows[r] += ".";
    }
  }
  return scaleBitmap(rows, scale);
}

// Hand-designed world map silhouette — Americas, Europe+Africa, Asia+Australia.
const WORLD_MAP = [
  "................................................................",
  "...........#####.......#####...##......######...........###....",
  "..........########..############.....###########.........####..",
  "........############################.############.........###..",
  "........#############################.###########..............",
  ".........#############################...#######...............",
  "..........###########################.......##.................",
  "...........########################...........................",
  "............######################................###..........",
  ".............###################.................#####.........",
  "...............##############.....................###..........",
  "................##########.........................##..........",
  ".................######............................##..........",
  "..................####............................##...........",
  "..................###..............................##..........",
  "...................##........................................",
  "................................................................",
];

// Pattern cycle definitions
const PATTERNS = [
  { name: "BITCOIN", bitmap: renderText("BITCOIN", 2) },
  { name: "DEFI",    bitmap: renderText("DEFI", 3) },
  { name: "SATOSHI", bitmap: renderText("SATOSHI", 2) },
  { name: "WORLD",   bitmap: WORLD_MAP },
];

function rng(seed) {
  let s = seed | 0;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function makeEmptyCells() {
  const cells = [];
  for (let y = 0; y < ROWS; y++) {
    for (let x = 0; x < COLS; x++) {
      cells.push({ x, y, mine: false, count: 0 });
    }
  }
  return cells;
}

function fillBoard(cells, seed) {
  const r = rng(seed);
  for (const c of cells) {
    c.mine = r() < MINE_DENSITY;
    c.count = 0;
  }
  const get = (x, y) =>
    x < 0 || x >= COLS || y < 0 || y >= ROWS ? null : cells[y * COLS + x];
  for (const c of cells) {
    if (c.mine) continue;
    let count = 0;
    for (let dy = -1; dy <= 1; dy++) {
      for (let dx = -1; dx <= 1; dx++) {
        if (!dx && !dy) continue;
        const nb = get(c.x + dx, c.y + dy);
        if (nb && nb.mine) count++;
      }
    }
    c.count = count;
  }
  return get;
}

function cascadeReveal(start, get) {
  if (!start || start.mine) return [];
  const order = [];
  const seen = new Set();
  const queue = [start];
  while (queue.length) {
    const c = queue.shift();
    const k = `${c.x},${c.y}`;
    if (seen.has(k) || c.mine) continue;
    seen.add(k);
    order.push(c);
    if (c.count === 0) {
      for (let dy = -1; dy <= 1; dy++) {
        for (let dx = -1; dx <= 1; dx++) {
          if (!dx && !dy) continue;
          const nb = get(c.x + dx, c.y + dy);
          if (nb) queue.push(nb);
        }
      }
    }
  }
  return order;
}

// Compute the grid-cell indices that make up a given pattern, centered.
function patternCellIndices(bitmap) {
  const h = bitmap.length;
  const w = bitmap[0].length;
  const ox = Math.floor((COLS - w) / 2);
  const oy = Math.floor((ROWS - h) / 2);
  const cells = [];
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      if (bitmap[y][x] === "#") {
        const gx = ox + x;
        const gy = oy + y;
        if (gx >= 0 && gx < COLS && gy >= 0 && gy < ROWS) {
          cells.push(gy * COLS + gx);
        }
      }
    }
  }
  return cells;
}

// Shuffle in place (Fisher-Yates) so the reveal order looks organic
// rather than line-by-line.
function shuffle(arr, r) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(r() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

const CELL_DATA = makeEmptyCells();

export default function FractalMesh() {
  const containerRef = useRef(null);

  useEffect(() => {
    const root = containerRef.current;
    if (!root) return;

    const cellEls = Array.from(root.querySelectorAll(".ms-c"));
    let getCell = fillBoard(CELL_DATA, Math.floor(Math.random() * 1e9));

    const state = CELL_DATA.map((cell, i) => ({
      cell,
      el: cellEls[i],
      status: "covered",
      revealedAt: 0,
    }));

    const ctx = {
      mode: "pattern", // "pattern" | "playing" | "gameover"
      lastUserActivity: 0,
      pendingTimers: [],
    };

    const patternCtx = {
      idx: 0,
      phase: "drawing", // "drawing" | "holding" | "fading"
      cells: [],
      cellPos: 0,
      phaseStart: performance.now(),
      shuffleRng: rng(987654321),
    };

    function setCell(idx, status, now) {
      const s = state[idx];
      s.status = status;
      s.revealedAt = now;
      const el = s.el;
      if (status === "covered") {
        el.textContent = "0";
        el.className = "ms-c covered";
      } else if (status === "carved") {
        el.textContent = " ";
        el.className = "ms-c carved";
      } else if (status === "revealed") {
        if (s.cell.mine) return;
        if (s.cell.count === 0) {
          el.textContent = "·";
          el.className = "ms-c empty";
        } else {
          el.textContent = String(s.cell.count);
          el.className = `ms-c num n${s.cell.count}`;
        }
      } else if (status === "flagged") {
        el.textContent = "⚑";
        el.className = "ms-c flag";
      } else if (status === "mine-revealed") {
        el.textContent = "✸";
        el.className = "ms-c mine-revealed";
      } else if (status === "mine-explosion") {
        el.textContent = "✸";
        el.className = "ms-c mine-revealed mine-explosion";
      }
    }

    function recoverAll(now) {
      for (let i = 0; i < state.length; i++) {
        if (state[i].status !== "covered") setCell(i, "covered", now);
      }
    }

    function startPattern(idx) {
      const p = PATTERNS[idx];
      patternCtx.cells = shuffle(patternCellIndices(p.bitmap), patternCtx.shuffleRng);
      patternCtx.cellPos = 0;
      patternCtx.phase = "drawing";
      patternCtx.phaseStart = performance.now();
    }

    function patternTick(now) {
      if (patternCtx.phase === "drawing") {
        const elapsed = now - patternCtx.phaseStart;
        const target = Math.floor(elapsed / PATTERN_DRAW_STAGGER_MS);
        while (patternCtx.cellPos < patternCtx.cells.length && patternCtx.cellPos < target) {
          const idx = patternCtx.cells[patternCtx.cellPos];
          if (state[idx].status === "covered") setCell(idx, "carved", now);
          patternCtx.cellPos++;
        }
        if (patternCtx.cellPos >= patternCtx.cells.length) {
          patternCtx.phase = "holding";
          patternCtx.phaseStart = now;
        }
      } else if (patternCtx.phase === "holding") {
        if (now - patternCtx.phaseStart > PATTERN_HOLD_MS) {
          patternCtx.phase = "fading";
          patternCtx.phaseStart = now;
          patternCtx.cellPos = 0;
        }
      } else if (patternCtx.phase === "fading") {
        const elapsed = now - patternCtx.phaseStart;
        const target = Math.floor(elapsed / PATTERN_FADE_STAGGER_MS);
        while (patternCtx.cellPos < patternCtx.cells.length && patternCtx.cellPos < target) {
          const idx = patternCtx.cells[patternCtx.cellPos];
          if (state[idx].status === "carved") setCell(idx, "covered", now);
          patternCtx.cellPos++;
        }
        if (patternCtx.cellPos >= patternCtx.cells.length) {
          patternCtx.idx = (patternCtx.idx + 1) % PATTERNS.length;
          startPattern(patternCtx.idx);
        }
      }
    }

    startPattern(0);

    function startCascade(idx, autoFlag) {
      const cell = state[idx].cell;
      const targetEl = state[idx].el;
      targetEl.classList.add("click-pulse");
      const pulseT = setTimeout(() => targetEl.classList.remove("click-pulse"), 220);
      ctx.pendingTimers.push(pulseT);

      const order = cascadeReveal(cell, getCell);
      order.forEach((c, i) => {
        const t = setTimeout(() => {
          const idx2 = c.y * COLS + c.x;
          if (state[idx2].status === "covered") {
            setCell(idx2, "revealed", performance.now());
          }
        }, i * REVEAL_STAGGER_MS);
        ctx.pendingTimers.push(t);
      });

      if (autoFlag) {
        const flagT = setTimeout(() => {
          const revealedSet = new Set(order.map((c) => `${c.x},${c.y}`));
          for (const c of CELL_DATA) {
            if (!c.mine) continue;
            let touches = false;
            for (let dy = -1; dy <= 1 && !touches; dy++) {
              for (let dx = -1; dx <= 1 && !touches; dx++) {
                if (!dx && !dy) continue;
                const nb = getCell(c.x + dx, c.y + dy);
                if (nb && revealedSet.has(`${nb.x},${nb.y}`)) touches = true;
              }
            }
            if (touches) {
              const idx2 = c.y * COLS + c.x;
              if (state[idx2].status === "covered") {
                setCell(idx2, "flagged", performance.now());
              }
            }
          }
        }, order.length * REVEAL_STAGGER_MS + 80);
        ctx.pendingTimers.push(flagT);
      }
    }

    function gameOver(triggeredIdx) {
      ctx.mode = "gameover";
      const now = performance.now();
      for (let i = 0; i < state.length; i++) {
        if (state[i].cell.mine) {
          setCell(i, i === triggeredIdx ? "mine-explosion" : "mine-revealed", now);
        }
      }
      const t = setTimeout(() => {
        getCell = fillBoard(CELL_DATA, Math.floor(Math.random() * 1e9));
        const resetNow = performance.now();
        for (let i = 0; i < state.length; i++) {
          setCell(i, "covered", resetNow);
        }
        ctx.mode = "pattern";
        startPattern(patternCtx.idx);
      }, GAME_OVER_HOLD_MS);
      ctx.pendingTimers.push(t);
    }

    function noteUserActivity() {
      ctx.lastUserActivity = performance.now();
      if (ctx.mode === "pattern") {
        // Switching out of pattern mode — clear any carved cells so the
        // game starts on a clean covered board.
        recoverAll(performance.now());
        ctx.mode = "playing";
      }
    }

    function handleClick(e) {
      const cellEl = e.target.closest && e.target.closest(".ms-c");
      if (!cellEl) return;
      if (ctx.mode === "gameover") return;
      const idx = parseInt(cellEl.dataset.idx, 10);
      noteUserActivity();
      const s = state[idx];
      if (s.status === "flagged") return;
      if (s.status !== "covered") return;
      if (s.cell.mine) {
        gameOver(idx);
        return;
      }
      startCascade(idx, false);
    }

    function handleContextMenu(e) {
      const cellEl = e.target.closest && e.target.closest(".ms-c");
      if (!cellEl) return;
      e.preventDefault();
      if (ctx.mode === "gameover") return;
      const idx = parseInt(cellEl.dataset.idx, 10);
      noteUserActivity();
      const s = state[idx];
      const now = performance.now();
      if (s.status === "covered") setCell(idx, "flagged", now);
      else if (s.status === "flagged") setCell(idx, "covered", now);
    }

    root.addEventListener("click", handleClick);
    root.addEventListener("contextmenu", handleContextMenu);

    let raf;
    function tick(now) {
      if (ctx.mode === "pattern") {
        patternTick(now);
      } else if (ctx.mode === "playing" && now - ctx.lastUserActivity > IDLE_TIMEOUT_MS) {
        // Idle timeout — clear board and resume pattern cycle
        recoverAll(now);
        ctx.mode = "pattern";
        startPattern(patternCtx.idx);
      }
      raf = requestAnimationFrame(tick);
    }
    raf = requestAnimationFrame(tick);

    return () => {
      root.removeEventListener("click", handleClick);
      root.removeEventListener("contextmenu", handleContextMenu);
      cancelAnimationFrame(raf);
      ctx.pendingTimers.forEach(clearTimeout);
    };
  }, []);

  // Render grid (once). JS mutates textContent + className via refs.
  const cells = [];
  for (let y = 0; y < ROWS; y++) {
    for (let x = 0; x < COLS; x++) {
      const idx = y * COLS + x;
      cells.push(
        <span
          key={idx}
          className="ms-c covered"
          data-idx={idx}
          style={{
            position: "absolute",
            left: x * CELL_W,
            top: y * CELL_H,
            width: CELL_W,
            height: CELL_H,
            lineHeight: `${CELL_H}px`,
          }}
        >
          0
        </span>
      );
    }
  }

  return (
    <div className="fractal-stage ms-stage">
      <div
        className="ms-grid"
        ref={containerRef}
        style={{
          width: `${COLS * CELL_W}px`,
          height: `${ROWS * CELL_H}px`,
        }}
      >
        {cells}
      </div>
    </div>
  );
}
