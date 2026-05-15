import { useEffect, useRef } from "react";
import { useReactFlow } from "@xyflow/react";

export function FocusOnNode({ address, focusKey }) {
  const { setCenter, getNodes } = useReactFlow();
  const lastKey = useRef(null);
  useEffect(() => {
    if (!address || focusKey === lastKey.current) return;
    lastKey.current = focusKey;
    // Small delay to let ReactFlow finish rendering positions
    const timer = setTimeout(() => {
      const allNodes = getNodes();
      let node = allNodes.find((n) => n.id === address);
      if (!node) node = allNodes.find((n) => n.id?.toLowerCase() === address.toLowerCase());
      if (node) {
        const w = node.measured?.width || node.width || 220;
        const h = node.measured?.height || node.height || 120;
        const x = node.positionAbsolute?.x ?? node.position?.x ?? 0;
        const y = node.positionAbsolute?.y ?? node.position?.y ?? 0;
        setCenter(x + w / 2, y + h / 2, { zoom: 1.2, duration: 400 });
      }
    }, 100);
    return () => clearTimeout(timer);
  }, [address, focusKey, getNodes, setCenter]);
  return null;
}
