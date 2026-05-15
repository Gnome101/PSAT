import { useCallback, useRef, useState } from "react";

export function DraggableSidebar({ children, flyout = null }) {
  const [width, setWidth] = useState(380);
  // On phones, start the sidebar collapsed — it's a bottom sheet there
  // and would otherwise cover most of the screen on first load.
  const [collapsed, setCollapsed] = useState(() => {
    if (typeof window === "undefined") return false;
    return window.matchMedia("(max-width: 720px)").matches;
  });
  const [flyoutCollapsed, setFlyoutCollapsed] = useState(false);
  const dragging = useRef(false);
  const sidebarWidth = collapsed ? 44 : width;
  const showFlyout = !collapsed && flyout && !flyoutCollapsed;
  const showFlyoutRail = !collapsed && flyout && flyoutCollapsed;

  const onMouseDown = useCallback((e) => {
    if (collapsed) return;
    e.preventDefault();
    dragging.current = true;
    const startX = e.clientX;
    const startW = width;
    const onMove = (ev) => {
      if (!dragging.current) return;
      const newW = Math.max(280, Math.min(800, startW - (ev.clientX - startX)));
      setWidth(newW);
    };
    const onUp = () => {
      dragging.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [collapsed, width]);

  return (
    <>
      {showFlyout ? (
        <div className="ps-sidebar-flyout" style={{ right: sidebarWidth }}>
          <button
            type="button"
            className="ps-sidebar-flyout-toggle"
            onClick={() => setFlyoutCollapsed(true)}
            title="Minimize panel"
            aria-label="Minimize panel"
          >
            &lt;
          </button>
          {flyout}
        </div>
      ) : null}
      {showFlyoutRail ? (
        <button
          type="button"
          className="ps-sidebar-flyout-rail"
          style={{ right: sidebarWidth }}
          onClick={() => setFlyoutCollapsed(false)}
          title="Expand panel"
          aria-label="Expand panel"
        >
          &gt;
        </button>
      ) : null}
      <div
        className={`ps-sidebar${collapsed ? " ps-sidebar-collapsed" : ""}`}
        style={{
          width: sidebarWidth,
          minWidth: sidebarWidth,
          maxWidth: sidebarWidth,
          "--ps-sidebar-width": `${sidebarWidth}px`,
        }}
      >
        <div
          className="ps-sidebar-handle"
          onMouseDown={onMouseDown}
          onClick={(e) => {
            // On mobile (where the sidebar is a bottom sheet) tap the
            // handle to toggle. Desktop keeps drag-to-resize behavior.
            if (typeof window !== "undefined" && window.matchMedia("(max-width: 720px)").matches) {
              e.stopPropagation();
              setCollapsed((c) => !c);
            }
          }}
        >
          <button
            type="button"
            className="ps-sidebar-toggle ps-sidebar-toggle-collapse"
            onMouseDown={(e) => e.stopPropagation()}
            onClick={() => setCollapsed(true)}
            title="Minimize side panel"
            aria-label="Minimize side panel"
          >
            &gt;
          </button>
          <span className="ps-sidebar-mobile-label" aria-hidden="true">
            {collapsed ? "▴  View details" : "▾  Close"}
          </span>
        </div>
        <div className="ps-sidebar-content">{children}</div>
        <div className="ps-sidebar-rail">
          <button
            type="button"
            className="ps-sidebar-rail-button"
            onClick={() => setCollapsed(false)}
            title="Expand side panel"
            aria-label="Expand side panel"
          >
            &lt;
          </button>
        </div>
      </div>
    </>
  );
}
