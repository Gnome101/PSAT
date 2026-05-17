import { useEffect, useRef } from "react";
import { useReactFlow } from "@xyflow/react";

export function FocusOnNode({ address, focusKey }) {
  const { setCenter, getInternalNode, getNodes } = useReactFlow();
  const lastKey = useRef(null);
  useEffect(() => {
    if (!address || focusKey === lastKey.current) return;
    lastKey.current = focusKey;
    // Small delay to let ReactFlow finish rendering positions
    const timer = setTimeout(() => {
      // node.position is RELATIVE to the parent group for nodes inside
      // a group container (every contract inside a Safe / EOA group on
      // this page). The resolved absolute position lives on the
      // internal-node representation, not the public node, so we look
      // it up via getInternalNode. Without this, focusing a grouped
      // contract centres the camera at the group's local origin
      // (sometimes thousands of px from where the card actually
      // renders, e.g. StakedTokenV1 inside the EOA group).
      const internal = getInternalNode(address) || getInternalNode(address.toLowerCase());
      const node = internal
        || getNodes().find((n) => n.id === address)
        || getNodes().find((n) => n.id?.toLowerCase() === address.toLowerCase());
      if (!node) return;
      const w = node.measured?.width || node.width || 220;
      const h = node.measured?.height || node.height || 120;
      const x = internal?.internals?.positionAbsolute?.x
        ?? node.positionAbsolute?.x
        ?? node.position?.x
        ?? 0;
      const y = internal?.internals?.positionAbsolute?.y
        ?? node.positionAbsolute?.y
        ?? node.position?.y
        ?? 0;
      setCenter(x + w / 2, y + h / 2, { zoom: 1.2, duration: 400 });
    }, 100);
    return () => clearTimeout(timer);
  }, [address, focusKey, getInternalNode, getNodes, setCenter]);
  return null;
}
