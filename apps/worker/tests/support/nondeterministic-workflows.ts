// Deliberately incompatible TEST ONLY code. Never registered by a production worker.
import { sleep } from "@temporalio/workflow";
import { dummyTurnWorkflow as original, dotGoalWorkflow as originalGoal } from "../../src/workflows.js";
import type { DummyTurnInput } from "@space/contracts";

/** Intentionally change the first command so the dummy-turn replay gate must reject it. */
export async function dummyTurnWorkflow(input: DummyTurnInput) {
  await sleep("1 second");
  return original(input);
}

/** Intentionally add a timer before goal execution to prove incompatible replay fails. */
export async function dotGoalWorkflow(input: { goalId: string }) {
  await sleep("1 second");
  return originalGoal(input);
}
