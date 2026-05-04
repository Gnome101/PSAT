// Isolated module so Vite/Rollup can split react-markdown + remark-gfm
// out of the main ProtocolSurface bundle. Loaded lazily by AgentPanel
// only after the user actually opens the Agent tab and a turn renders.
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

export default function MarkdownBubble({ text, components }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
      {text}
    </ReactMarkdown>
  );
}
