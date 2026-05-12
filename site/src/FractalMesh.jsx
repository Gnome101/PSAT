import React, { useEffect, useRef } from "react";

/*
 * FractalMesh — minesweeper auto-play with engineered boards that hide
 * recognizable shapes in their no-mine regions.
 *
 * Each board is generated so a chosen pattern (BITCOIN / DEFI / SATOSHI /
 * world map) lives in the no-mine zone, surrounded by a forced wall of
 * mines. When the auto-clicker (or the user) clicks anywhere inside the
 * pattern, the cascade reveals the entire pattern as empty cells fenced
 * by numbered border cells — minesweeper naturally drawing the shape.
 *
 * Pattern cycles every PATTERN_CYCLE_MS, swapping in a fresh board.
 * User click still drops into game mode; idle timeout resumes auto-play.
 */

const COLS = 200;
const ROWS = 52;
const CELL_W = 20;
const CELL_H = 26;
const MINE_DENSITY = 0.13;

// Auto-play timing — 2× faster than the previous baseline
const CLICK_INTERVAL_MS = 750;
const AUTO_CLICKER_COUNT = 2;
const INITIAL_BURST_COUNT = 5;
const REVEAL_STAGGER_MS = 14;
const RECOVER_MIN_AGE_MS = 14000;
const RECOVER_PROB_PER_TICK = 0.025;
const RECOVER_TICK_MS = 280;
const IDLE_TIMEOUT_MS = 25000;
const GAME_OVER_HOLD_MS = 2600;

// Each board lives for this long before the pattern rotates
const PATTERN_CYCLE_MS = 28000;
// Pattern is centered vertically around this grid row (upper portion)
const PATTERN_Y_ROW = 14;

// 5x7 pixel font — `#` = on
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

// Hand-designed world map silhouette
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

// Cells covered by the pattern bitmap (the `#` marks), centered horizontally
// and shifted into the upper portion of the grid.
function patternCellSet(bitmap) {
  const h = bitmap.length;
  const w = bitmap[0].length;
  const ox = Math.floor((COLS - w) / 2);
  const oy = PATTERN_Y_ROW - Math.floor(h / 2);
  const set = new Set();
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      if (bitmap[y][x] === "#") {
        const gx = ox + x;
        const gy = oy + y;
        if (gx >= 0 && gx < COLS && gy >= 0 && gy < ROWS) {
          set.add(gy * COLS + gx);
        }
      }
    }
  }
  return set;
}

// 1-cell buffer ring around any cell in the input set (still no-mine zone)
function bufferAround(cellSet) {
  const buf = new Set(cellSet);
  for (const idx of cellSet) {
    const x = idx % COLS;
    const y = Math.floor(idx / COLS);
    for (let dy = -1; dy <= 1; dy++) {
      for (let dx = -1; dx <= 1; dx++) {
        const gx = x + dx;
        const gy = y + dy;
        if (gx >= 0 && gx < COLS && gy >= 0 && gy < ROWS) {
          buf.add(gy * COLS + gx);
        }
      }
    }
  }
  return buf;
}

// Cells in the immediate ring outside the buffer — forced mines so the
// cascade halts cleanly at the pattern's outline.
function wallAround(bufferSet) {
  const wall = new Set();
  for (const idx of bufferSet) {
    const x = idx % COLS;
    const y = Math.floor(idx / COLS);
    for (let dy = -1; dy <= 1; dy++) {
      for (let dx = -1; dx <= 1; dx++) {
        if (!dx && !dy) continue;
        const gx = x + dx;
        const gy = y + dy;
        if (gx >= 0 && gx < COLS && gy >= 0 && gy < ROWS) {
          const widx = gy * COLS + gx;
          if (!bufferSet.has(widx)) wall.add(widx);
        }
      }
    }
  }
  return wall;
}

function fillBoardWithPattern(cells, seed, patternBitmap) {
  const r = rng(seed);
  const patternMask = patternCellSet(patternBitmap);
  const bufferMask = bufferAround(patternMask);
  const wallMask = wallAround(bufferMask);

  for (const c of cells) {
    const idx = c.y * COLS + c.x;
    if (bufferMask.has(idx)) {
      c.mine = false; // pattern + buffer = guaranteed empty cascade region
    } else if (wallMask.has(idx)) {
      c.mine = true; // forced ring so the cascade halts at the outline
    } else {
      c.mine = r() < MINE_DENSITY;
    }
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
  return { get, patternMask };
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

const CELL_DATA = makeEmptyCells();

export default function FractalMesh() {
  const containerRef = useRef(null);

  useEffect(() => {
    const root = containerRef.current;
    if (!root) return;

    const cellEls = Array.from(root.querySelectorAll(".ms-c"));
    let getCell;
    let currentPatternMask = new Set();

    const state = CELL_DATA.map((cell, i) => ({
      cell,
      el: cellEls[i],
      status: "covered",
      revealedAt: 0,
    }));

    const ctx = {
      mode: "auto", // "auto" | "playing" | "gameover"
      lastUserActivity: 0,
      pendingTimers: [],
      patternIdx: 0,
      lastPatternSwitch: performance.now(),
    };

    function setCell(idx, status, now) {
      const s = state[idx];
      s.status = status;
      s.revealedAt = now;
      const el = s.el;
      if (status === "covered") {
        el.textContent = "0";
        el.className = "ms-c covered";
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

    function autoTick() {
      if (ctx.mode !== "auto") return;
      const candidates = [];
      for (let i = 0; i < state.length; i++) {
        const s = state[i];
        if (s.status === "covered" && !s.cell.mine) candidates.push(i);
      }
      if (!candidates.length) return;
      const idx = candidates[Math.floor(Math.random() * candidates.length)];
      startCascade(idx, true);
    }

    function pickCoveredInRegion(xMin, xMax, yMin, yMax) {
      const candidates = [];
      for (let y = yMin; y < yMax; y++) {
        for (let x = xMin; x < xMax; x++) {
          const i = y * COLS + x;
          const s = state[i];
          if (s && s.status === "covered" && !s.cell.mine) candidates.push(i);
        }
      }
      if (!candidates.length) return -1;
      return candidates[Math.floor(Math.random() * candidates.length)];
    }

    function pickFromSet(set) {
      const arr = [];
      for (const idx of set) {
        const s = state[idx];
        if (s && s.status === "covered" && !s.cell.mine) arr.push(idx);
      }
      if (!arr.length) return -1;
      return arr[Math.floor(Math.random() * arr.length)];
    }

    function initialBurst() {
      // First click is guaranteed inside the current pattern so the shape
      // surfaces immediately rather than waiting for random hits.
      const patternIdx = pickFromSet(currentPatternMask);
      if (patternIdx >= 0) startCascade(patternIdx, true);

      // Plus random clicks across regions for additional activity
      const cols = Math.ceil(Math.sqrt(INITIAL_BURST_COUNT));
      const rowsBurst = Math.ceil(INITIAL_BURST_COUNT / cols);
      for (let ry = 0; ry < rowsBurst; ry++) {
        for (let rx = 0; rx < cols; rx++) {
          const idx = pickCoveredInRegion(
            Math.floor((rx / cols) * COLS),
            Math.floor(((rx + 1) / cols) * COLS),
            Math.floor((ry / rowsBurst) * ROWS),
            Math.floor(((ry + 1) / rowsBurst) * ROWS)
          );
          if (idx >= 0) startCascade(idx, true);
        }
      }
    }

    function regenerateBoard(useNextPattern) {
      if (useNextPattern) ctx.patternIdx = (ctx.patternIdx + 1) % PATTERNS.length;
      const pattern = PATTERNS[ctx.patternIdx];
      const result = fillBoardWithPattern(
        CELL_DATA,
        Math.floor(Math.random() * 1e9),
        pattern.bitmap
      );
      getCell = result.get;
      currentPatternMask = result.patternMask;
      recoverAll(performance.now());
      ctx.lastPatternSwitch = performance.now();
      initialBurst();
    }

    regenerateBoard(false);

    function gameOver(triggeredIdx) {
      ctx.mode = "gameover";
      const now = performance.now();
      for (let i = 0; i < state.length; i++) {
        if (state[i].cell.mine) {
          setCell(i, i === triggeredIdx ? "mine-explosion" : "mine-revealed", now);
        }
      }
      const t = setTimeout(() => {
        regenerateBoard(true);
        ctx.mode = "auto";
      }, GAME_OVER_HOLD_MS);
      ctx.pendingTimers.push(t);
    }

    function noteUserActivity() {
      ctx.lastUserActivity = performance.now();
      if (ctx.mode === "auto") ctx.mode = "playing";
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

    // Multiple concurrent auto-clickers, staggered start
    const autoTimers = [];
    for (let i = 0; i < AUTO_CLICKER_COUNT; i++) {
      const offset = (i * CLICK_INTERVAL_MS) / AUTO_CLICKER_COUNT;
      const t = setTimeout(() => {
        autoTick();
        const interval = setInterval(autoTick, CLICK_INTERVAL_MS);
        autoTimers.push(interval);
      }, offset);
      autoTimers.push(t);
    }

    let raf;
    let lastRecoverTick = 0;
    function tick(now) {
      // Re-cover (auto only)
      if (ctx.mode === "auto" && now - lastRecoverTick > RECOVER_TICK_MS) {
        lastRecoverTick = now;
        for (let i = 0; i < state.length; i++) {
          const s = state[i];
          if (s.status === "covered" || s.status === "mine-revealed" || s.status === "mine-explosion") continue;
          const age = now - s.revealedAt;
          if (age > RECOVER_MIN_AGE_MS && Math.random() < RECOVER_PROB_PER_TICK) {
            setCell(i, "covered", now);
          }
        }
      }

      // Pattern cycle
      if (ctx.mode === "auto" && now - ctx.lastPatternSwitch > PATTERN_CYCLE_MS) {
        regenerateBoard(true);
      }

      // Idle timeout — return to auto-play
      if (ctx.mode === "playing" && now - ctx.lastUserActivity > IDLE_TIMEOUT_MS) {
        ctx.mode = "auto";
        regenerateBoard(false);
      }

      raf = requestAnimationFrame(tick);
    }
    raf = requestAnimationFrame(tick);

    return () => {
      root.removeEventListener("click", handleClick);
      root.removeEventListener("contextmenu", handleContextMenu);
      autoTimers.forEach((t) => {
        clearInterval(t);
        clearTimeout(t);
      });
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
