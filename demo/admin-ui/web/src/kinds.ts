/// The old UI's palette: gateway blue, broker orange, compute green, node_admin gold (the bridge's colour).
export const KIND_COLOR: Record<string, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  node_admin: "#e3b341",
};
export const colourOfKind = (k: string) => KIND_COLOR[k] ?? "#8b949e";
