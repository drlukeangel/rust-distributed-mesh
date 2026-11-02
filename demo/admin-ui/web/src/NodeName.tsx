import type { PathName } from "./api";

/// A node shown by its two properties, its path and its name.
export function NodeName({ node }: { node: PathName }) {
  return (
    <span className="mono">
      <span className="muted">path</span> {node.path} <span className="muted">name</span> {node.name}
    </span>
  );
}
