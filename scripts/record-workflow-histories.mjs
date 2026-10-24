import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const directory = "apps/worker/tests/fixtures/workflow-histories";
const patchIndex = process.argv.indexOf("--patch");
const patch = patchIndex < 0 ? undefined : process.argv[patchIndex + 1];
if (patch !== undefined && !/^[a-zA-Z0-9_-]+$/.test(patch)) throw new Error("Patch ID must be a filename-safe patched() identifier.");
const tracked = spawnSync("git", ["ls-files", "--", `${directory}/*.json`], { cwd: root, encoding: "utf8" });
if (tracked.status !== 0) throw new Error("Cannot determine whether histories are already tracked.");
if (tracked.stdout.trim()) {
  const changes = spawnSync("git", ["diff", "HEAD", "--", "apps/worker/src/workflows.ts",
    "apps/worker/src/memory-maintenance-workflows.ts", "apps/worker/src/memory-mutation-workflows.ts"], { cwd: root, encoding: "utf8" });
  if (changes.status !== 0) throw new Error("Cannot inspect the accompanying workflow patch.");
  if (!patch || !changes.stdout.split("\n").some(line => line.startsWith("+") && !line.startsWith("+++") && line.includes(`patched("${patch}")`))) {
    throw new Error("Frozen histories: add a new reviewed patched() change before recording. Pass --patch NEW_PATCH_ID before committing the change; never re-record to hide a replay failure.");
  }
}
const result = spawnSync(process.execPath, [path.join(root, "node_modules/vitest/vitest.mjs"), "run",
  "--config", "apps/worker/vitest.config.ts", "apps/worker/tests/workflow-real-sandbox.test.ts"], {
  cwd: root, stdio: "inherit", env: { ...process.env, SPACE_RECORD_TEMPORAL_HISTORIES: "1", SPACE_TEMPORAL_HISTORY_PATCH: patch ?? "" }
});
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
