// These diagnostic fields are not replay commands. Keep local paths and worker
// hostnames out of public synthetic histories without changing payloads/events.
/** Remove identifying diagnostics while preserving recorded replay commands and payloads. */
export function sanitizeHistory(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sanitizeHistory);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key,
    key === "identity" && typeof entry === "string" ? "synthetic-worker"
      : key === "stackTrace" && typeof entry === "string" ? ""
      : sanitizeHistory(entry)
  ]));
}
