import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { Context } from "@temporalio/activity";
import { ApplicationFailure, TimeoutFailure, TimeoutType, defaultPayloadConverter } from "@temporalio/common";
import { temporal } from "@temporalio/proto";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker, type WorkflowBundle } from "@temporalio/worker";
import { WorkflowContinuedAsNewError, WorkflowNotFoundError, type WorkflowHandle } from "@temporalio/client";
import { DeterminismViolationError } from "@temporalio/workflow";
import {
  dummyTurnInputSchema, DOT_GOAL_SIGNAL, ROOM_AGENT_ENQUEUE_SIGNAL, ROOM_AGENT_STOP_SIGNAL,
  roomAgentSupervisorQueueItemSchema, roomAgentSupervisorInputSchema, MEMORY_MUTATION_TASK_QUEUE
} from "@space/contracts";
import { MEMORY_MAINTENANCE_TASK_QUEUE } from "../src/memory-maintenance-config.js";
import { continuationFixture } from "./room-continuation-fixture.js";
import { sanitizeHistory } from "./support/sanitize-history.js";
import { discardSupervisorTurn, finishSupervisorTurn, prepareBackgroundContinuation, supervisorTurnState } from "../src/room-background-continuation.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const historiesDirectory = path.join(here, "fixtures/workflow-histories");
const recording = process.env.SPACE_RECORD_TEMPORAL_HISTORIES === "1";
const currentPath = path.join(here, "support/sandbox-workflows.ts");
const legacyPath = path.join(here, "fixtures/workflow-histories/legacy-workflows.ts");
type Implementation = (...args: unknown[]) => unknown | Promise<unknown>;
type Call = { name: string; input: unknown; attempt: number };

// Single sequential environment: no provider, database, Docker or production credentials.
describe.sequential("production workflows in the real Temporal sandbox", () => {
  let env: TestWorkflowEnvironment;
  let bundle: WorkflowBundle;
  let legacyBundle: WorkflowBundle;
  const workers: Worker[] = [];
  const runs: Promise<void>[] = [];
  const started = new Set<string>();
  let calls: Call[] = [];
  let implementations: Record<string, Implementation> = {};
  const taskQueue = `sandbox-regression-${randomUUID()}`;
  const legacyQueue = `${taskQueue}-legacy`;
  const completion = { status: "COMPLETED", message: "Synthetic successful turn",
    roomAgentOutcome: { status: "VERIFIED", executedActionCount: 1, statusReason: "Synthetic action verified" } };
  /** Build a schema-valid turn containing synthetic identifiers and prompt text. */
  const turn = (suffix = "one") => dummyTurnInputSchema.parse({
    roomId: "room:fixture", paneId: "pane:fixture", prompt: "Synthetic fixture only",
    agentRunId: `agent_run:${suffix}`, traceId: "trace:fixture"
  });
  /** Wrap a synthetic turn in a supervisor mission queue item. */
  const item = (suffix = "one") => roomAgentSupervisorQueueItemSchema.parse({
    missionId: "mission:fixture", turn: turn(suffix)
  });
  const activityNames = [
    "recordDummyTurnStarted", "recordDummyTurnCompleted", "runCodexAppServerTurn",
    "runOpenCodeAgentTurn", "runCliAgentTurn", "dotGoalStep", "dotGoalObserve",
    "checkRoomAgentSupervisorTurn", "discardRoomAgentSupervisorTurn", "finishRoomAgentSupervisorTurn",
    "prepareRoomAgentBackgroundContinuation", "settleRoomAgentBackgroundMission",
    "markRoomAgentMissionStarted", "markRoomAgentMissionFinished", "markRoomAgentMissionContinued",
    "runRoomAgentBackgroundAction", "failRoomAgentBackgroundAction", "refreshMemoryGraphSnapshot",
    "refreshAllMonthsMemoryGraphSnapshot", "reconcileStaleSpaceAgentSessions", "createScheduledMemoryAudit",
    "prepareMemoryConsolidation", "executePersistedMemoryChangeSet", "finalizeMemoryRepair"
  ];
  const activities = Object.fromEntries(activityNames.map(name => [name, async (...args: unknown[]) => {
    calls.push({ name, input: args[0], attempt: Context.current().info.attempt });
    return implementations[name]?.(...args);
  }]));

  beforeAll(async () => {
    env = await TestWorkflowEnvironment.createTimeSkipping();
    bundle = await bundleWorkflowCode({ workflowsPath: currentPath });
    legacyBundle = await bundleWorkflowCode({ workflowsPath: legacyPath });
    for (const queue of [taskQueue, MEMORY_MAINTENANCE_TASK_QUEUE, MEMORY_MUTATION_TASK_QUEUE, legacyQueue]) {
      const worker = await Worker.create({
        connection: env.nativeConnection, namespace: env.namespace,
        taskQueue: queue, workflowBundle: queue === legacyQueue ? legacyBundle : bundle,
        activities, shutdownGraceTime: "1 second",
        maxHeartbeatThrottleInterval: "10 milliseconds", defaultHeartbeatThrottleInterval: "10 milliseconds"
      });
      workers.push(worker);
      // Observe failures immediately, then propagate them in teardown.
      const run = worker.run();
      void run.catch(() => undefined);
      runs.push(run);
    }
  }, 60_000);

  beforeEach(() => {
    calls = [];
    implementations = {
      recordDummyTurnCompleted: () => completion,
      runCodexAppServerTurn: () => completion,
      runOpenCodeAgentTurn: () => completion,
      runCliAgentTurn: () => completion,
      dotGoalStep: () => ({ state: "DONE", runId: null, goalStatus: "COMPLETED" }),
      checkRoomAgentSupervisorTurn: () => "RUN",
      prepareRoomAgentBackgroundContinuation: () => ({ state: "DONE" }),
      settleRoomAgentBackgroundMission: () => true,
      runRoomAgentBackgroundAction: () => completion,
      refreshMemoryGraphSnapshot: () => ({ sourceHash: "synthetic-source", nodeCount: 1 }),
      createScheduledMemoryAudit: () => ({ id: "audit:fixture", workflowId: `audit-${randomUUID()}` }),
      prepareMemoryConsolidation: () => ({ changeSetId: null, run: { id: "audit:fixture" } }),
      executePersistedMemoryChangeSet: () => ({ status: "APPLIED" }),
      finalizeMemoryRepair: () => ({ id: "audit:fixture", status: "COMPLETED" })
    };
  });

  afterEach(async () => {
    for (const id of started) {
      await env.client.workflow.getHandle(id).terminate("Synthetic test cleanup").catch(() => undefined);
    }
    started.clear();
  });

  afterAll(async () => {
    for (const worker of workers) worker.shutdown();
    try { await Promise.all(runs); } finally { await env?.teardown(); }
  }, 20_000);

  /** Start the selected real workflow and pin its handle to this run for continuation checks. */
  async function start(name: string, args: unknown[], legacy = false, workflowId = `${name}-${randomUUID()}`) {
    const handle = await env.client.workflow.start(name, { workflowId, taskQueue: legacy ? legacyQueue : taskQueue, args });
    started.add(workflowId);
    return env.client.workflow.getHandle(workflowId, handle.firstExecutionRunId, { followRuns: false });
  }
  /** Select recorded activity invocations by name, retaining order and attempt numbers. */
  const seen = (name: string) => calls.filter(call => call.name === name);
  /** Await a bounded host-side observation without replacing durable workflow timers. */
  async function until(predicate: () => boolean) {
    // Time skipping advances durable workflow timers, not this bounded host-side observation.
    await expect.poll(predicate, { timeout: 10_000, interval: 10 }).toBe(true);
  }
  /** Append a sanitized recording when explicitly enabled; never overwrite a frozen history. */
  async function keepHistory(name: string, handle: WorkflowHandle) {
    if (!recording) return;
    await mkdir(historiesDirectory, { recursive: true });
    const history = await handle.fetchHistory();
    const json = temporal.api.history.v1.History.toObject(temporal.api.history.v1.History.fromObject(history), { bytes: String, longs: String, enums: String });
    const suffix = process.env.SPACE_TEMPORAL_HISTORY_PATCH;
    const destination = path.join(historiesDirectory, `${name}${suffix ? `--${suffix}` : ""}.json`);
    try {
      // Historical commands are append-only, including the initial untracked
      // baseline. A re-record must never repair a broken replay by replacing it.
      await writeFile(destination, JSON.stringify(sanitizeHistory(json), null, 2) + "\n", { flag: "wx" });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
  }
  /** Terminate an infinite synthetic workflow, then capture its original run for replay. */
  async function stopAndKeep(name: string, handle: WorkflowHandle) {
    try {
      await env.client.workflow.getHandle(handle.workflowId).terminate("Synthetic test complete");
    } catch (error) {
      // A terminal execution needs no second termination. Do not hide network,
      // authorization or other cleanup errors, and still fetch/replay its history.
      if (!(error instanceof WorkflowNotFoundError) || error.message !== "Completed workflow") throw error;
    }
    started.delete(handle.workflowId);
    await keepHistory(name, handle);
  }
  /** Connect activity stubs to the real in-memory continuation store for persistence assertions. */
  function storeDriver() {
    const fixture = continuationFixture();
    implementations.prepareRoomAgentBackgroundContinuation = input => prepareBackgroundContinuation(input as Parameters<typeof prepareBackgroundContinuation>[0], fixture.store);
    implementations.checkRoomAgentSupervisorTurn = input => supervisorTurnState(input as Parameters<typeof supervisorTurnState>[0], fixture.store);
    implementations.discardRoomAgentSupervisorTurn = input => {
      const row = input as { item: Parameters<typeof discardSupervisorTurn>[0]; reason: string; errorCode?: string };
      return discardSupervisorTurn(row.item, row.reason, fixture.store, row.errorCode);
    };
    implementations.finishRoomAgentSupervisorTurn = input => finishSupervisorTurn(input as Parameters<typeof finishSupervisorTurn>[0], fixture.store);
    implementations.runCodexAppServerTurn = input => {
      const row = input as ReturnType<typeof dummyTurnInputSchema.parse>;
      fixture.store.updateSpaceAgentRun(row.agentRunId!, { status: "COMPLETED" });
      return completion;
    };
    return fixture;
  }

  it.each([0, 50])("actual store preserves one mission across allocation, continuation and verification (processed=%s)", async processedCount => {
    const c = storeDriver();
    let executions = 0;
    implementations.runCodexAppServerTurn = input => {
      executions++;
      const row = input as ReturnType<typeof dummyTurnInputSchema.parse>;
      expect(row.prompt).toContain("Open CLI and Chat, then send a story");
      c.store.updateSpaceAgentRun(row.agentRunId!, { status: "COMPLETED" });
      if (processedCount === 0 && executions === 1) {
        c.store.createRoomAgentAction({ ...c.action, actionId: "action:send", idempotencyKey: "send-once", actionType: "SEND",
          evidence: { cli: "Synthetic story complete", chat: "Synthetic story complete" }, statusReason: "Synthetic tasks completed" });
        return { ...completion, roomAgentOutcome: { ...completion.roomAgentOutcome, pendingActionIds: ["action:send"] } };
      }
      return completion;
    };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: c.room.id, monitoringMissionIds: [c.initial.mission.id], processedCount }]);
    const advance = env.client.workflow.getHandle(h.workflowId).result().catch(() => undefined);
    try {
      await until(() => c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status === "COMPLETED");
      expect(executions).toBe(processedCount === 0 ? 2 : 1);
      expect(seen("finishRoomAgentSupervisorTurn")).toHaveLength(1);
      if (processedCount === 0) expect(c.store.listRoomAgentActions(c.initial.mission.id).filter(a => a.actionType === "SEND")).toHaveLength(1);
      else {
        const history = await h.fetchHistory();
        const payload = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes)
          ?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
        expect(defaultPayloadConverter.fromPayload(payload!)).toMatchObject({ pending: [expect.anything()], processedCount: 0 });
        expect(history.events?.some(e => e.startChildWorkflowExecutionInitiatedEventAttributes)).toBe(false);
      }
      await stopAndKeep(`roomAgentSupervisorWorkflow-store-${processedCount}`, h);
    } finally {
      await env.client.workflow.getHandle(h.workflowId).terminate("Synthetic store test cleanup").catch(() => undefined);
      await advance;
    }
  }, 20_000);

  it.each(["plain", "timeout", "typed"])("actual store persists a failed child (%s) on both run and mission", async kind => {
    const c = storeDriver();
    const code = kind === "timeout" ? "ROOM_AGENT_SUPERVISOR_TIMEOUT"
      : kind === "typed" ? "CODEX_APP_SERVER_TURN_TIMEOUT" : "ROOM_AGENT_SUPERVISOR_FAILED";
    implementations.runCodexAppServerTurn = () => {
      throw ApplicationFailure.create({ message: "Synthetic failure", type: kind === "typed" ? code : "SyntheticFailure",
        nonRetryable: true, ...(kind === "timeout" ? { cause: new TimeoutFailure("Synthetic upstream timeout", undefined, TimeoutType.START_TO_CLOSE) } : {}) });
    };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: c.room.id, monitoringMissionIds: [c.initial.mission.id] }]);
    const advance = h.result().catch(() => undefined);
    try {
      await until(() => c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status === "FAILED");
      expect(c.store.getRoomAgentTurn(c.initial.mission.id)?.run).toMatchObject({ status: "FAILED", errorCode: code });
      expect(c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)).toMatchObject({ status: "FAILED" });
      expect(seen("finishRoomAgentSupervisorTurn")).toHaveLength(1);
      expect(seen("finishRoomAgentSupervisorTurn")[0]?.input).toMatchObject({ status: "FAILED", errorCode: code });
      await stopAndKeep(`roomAgentSupervisorWorkflow-store-failure-${kind}`, h);
    } finally {
      await env.client.workflow.getHandle(h.workflowId).terminate("Synthetic store test cleanup").catch(() => undefined);
      await advance;
    }
  }, 20_000);

  it("dummy turn performs both activities in order and retries a transient failure", async () => {
    implementations.recordDummyTurnStarted = () => {
      if (Context.current().info.attempt === 1) throw Error("Synthetic transient failure");
    };
    const h = await start("dummyTurnWorkflow", [turn()]);
    expect(await h.result()).toEqual(completion);
    expect(calls.map(c => [c.name, c.attempt])).toEqual([
      ["recordDummyTurnStarted", 1], ["recordDummyTurnStarted", 2], ["recordDummyTurnCompleted", 1]
    ]);
    await keepHistory("dummyTurnWorkflow", h);
  }, 15_000);

  it.each([
    ["opencode", "runOpenCodeAgentTurn"], [null, "runCodexAppServerTurn"], ["cli:claude", "runCliAgentTurn"]
  ])("turn workflow routes %s through its actual activity proxy", async (providerId, activity) => {
    const h = await start("codexAppServerTurnWorkflow", [{ ...turn(), providerId }]);
    expect(await h.result()).toEqual(completion);
    expect(seen(activity)).toHaveLength(1);
    await keepHistory(`codexAppServerTurnWorkflow-${activity}`, h);
  }, 10_000);

  it("Codex turn retries a transient activity failure and returns the successful result", async () => {
    implementations.runCodexAppServerTurn = () => {
      if (Context.current().info.attempt === 1) throw Error("Synthetic transient turn failure");
      return completion;
    };
    const h = await start("codexAppServerTurnWorkflow", [turn()]);
    expect(await h.result()).toEqual(completion);
    expect(seen("runCodexAppServerTurn").map(call => call.attempt)).toEqual([1, 2]);
  }, 15_000);

  it("goal wake signal interrupts a durable one-hour pause", async () => {
    implementations.dotGoalStep = () => seen("dotGoalStep").length === 1
      ? { state: "PAUSED", runId: null, goalStatus: "ACTIVE" }
      : { state: "DONE", runId: null, goalStatus: "COMPLETED" };
    const h = await start("dotGoalWorkflow", [{ goalId: "goal:fixture" }]);
    // Seeing the host activity alone does not prove its completion reached the
    // sandbox. Signal only after the durable pause really exists.
    await expect.poll(async () => (await h.fetchHistory()).events?.some(event =>
      Number(event.timerStartedEventAttributes?.startToFireTimeout?.seconds) === 3600),
      { timeout: 10_000, interval: 10 }).toBe(true);
    await h.signal(DOT_GOAL_SIGNAL);
    expect(await h.result()).toEqual({ goalStatus: "COMPLETED" });
    expect(seen("dotGoalStep")).toHaveLength(2);
    const history = await h.fetchHistory();
    const pause = history.events?.find(event => Number(event.timerStartedEventAttributes?.startToFireTimeout?.seconds) === 3600);
    expect(history.events?.some(event => String(event.timerCanceledEventAttributes?.startedEventId) === String(pause?.eventId))).toBe(true);
    expect(history.events?.some(event => String(event.timerFiredEventAttributes?.startedEventId) === String(pause?.eventId))).toBe(false);
    await keepHistory("dotGoalWorkflow-wake", h);
  }, 15_000);

  it("goal timeout polls activity and continues as new after its bounded loop", async () => {
    let steps = 0;
    implementations.dotGoalStep = () => {
      steps++;
      return { state: steps > 300 ? "DONE" : "WAITING", runId: null, goalStatus: steps > 300 ? "COMPLETED" : "ACTIVE" };
    };
    const isolated = await TestWorkflowEnvironment.createTimeSkipping();
    const queue = `goal-bound-${randomUUID()}`;
    const worker = await Worker.create({ connection: isolated.nativeConnection, namespace: isolated.namespace,
      taskQueue: queue, workflowBundle: bundle, activities });
    const id = `goal-bound-${randomUUID()}`;
    try {
      await worker.runUntil(async () => {
        const started = await isolated.client.workflow.start("dotGoalWorkflow", {
          workflowId: id, taskQueue: queue, args: [{ goalId: "goal:continuing" }]
        });
        const h = isolated.client.workflow.getHandle(id, started.firstExecutionRunId, { followRuns: false });
        expect(await isolated.client.workflow.getHandle(id).result()).toEqual({ goalStatus: "COMPLETED" });
        const history = await h.fetchHistory();
        expect(history.events?.some(e => e.workflowExecutionContinuedAsNewEventAttributes)).toBe(true);
        const timers = history.events?.filter(e => Number(e.timerStartedEventAttributes?.startToFireTimeout?.seconds) === 30);
        expect(timers).toHaveLength(300);
        expect(history.events?.filter(e => e.timerFiredEventAttributes)).toHaveLength(300);
        const carried = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes)
          ?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
        expect(defaultPayloadConverter.fromPayload(carried!)).toEqual({ goalId: "goal:continuing" });
        expect(steps).toBe(301);
        await keepHistory("dotGoalWorkflow-continue", h);
      });
    } finally {
      await isolated.client.workflow.getHandle(id).terminate("Synthetic test cleanup").catch(() => undefined);
      await isolated.teardown();
    }
  }, 25_000);

  it.each([false, true])("supervisor ignores cross-room input, executes enqueued work once and finishes the mission (legacy=%s)", async legacy => {
    const finishActivity = legacy ? "markRoomAgentMissionFinished" : "finishRoomAgentSupervisorTurn";
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture" }], legacy);
    await h.signal(ROOM_AGENT_ENQUEUE_SIGNAL, { ...item("wrong"), turn: { ...turn("wrong"), roomId: "room:other" } });
    await h.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item());
    await until(() => seen(finishActivity).length === 1);
    expect(seen("runCodexAppServerTurn")).toHaveLength(1);
    expect(seen(finishActivity)[0]?.input).toMatchObject({ status: "COMPLETED" });
    await stopAndKeep(`roomAgentSupervisorWorkflow-enqueue${legacy ? "-legacy" : ""}`, h);
  }, 15_000);

  it.each(["stop", "follow-up"])("supervisor discards prepared continuation when %s arrives during its activity", async kind => {
    const c = storeDriver();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    implementations.prepareRoomAgentBackgroundContinuation = async input => {
      const prepared = await prepareBackgroundContinuation(input as Parameters<typeof prepareBackgroundContinuation>[0], c.store);
      await gate;
      return prepared;
    };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: c.room.id, monitoringMissionIds: [c.initial.mission.id] }]);
    await env.sleep("1 second");
    await until(() => c.store.getRoomAgentTurn(c.initial.mission.id)?.run.runId !== c.initial.run.runId);
    const preparedRunId = c.store.getRoomAgentTurn(c.initial.mission.id)!.run.runId;
    if (kind === "stop") {
      c.store.updateRoomAgentMission(c.initial.mission.id, { status: "INTERRUPTED" });
      await h.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Synthetic stop" });
    } else {
      const followUp = c.enqueue("follow-up", "Only inspect the room.");
      await h.signal(ROOM_AGENT_ENQUEUE_SIGNAL, followUp.queueItem);
    }
    release();
    await until(() => seen("discardRoomAgentSupervisorTurn").length >= 1);
    expect(seen("discardRoomAgentSupervisorTurn")[0]?.input).toMatchObject({ item: { turn: { agentRunId: preparedRunId } } });
    await until(() => c.store.getRoomAgentTurn(c.initial.mission.id, preparedRunId)?.run.status === "INTERRUPTED");
    if (kind === "stop") expect(seen("runCodexAppServerTurn")).toHaveLength(0);
    else {
      await until(() => seen("runCodexAppServerTurn").length === 1);
      expect(seen("runCodexAppServerTurn")[0]?.input).toMatchObject({ roomAgentOperatorRequest: "Only inspect the room." });
    }
    await stopAndKeep(`roomAgentSupervisorWorkflow-store-race-${kind}`, h);
  }, 15_000);

  it("supervisor carries a prepared continuation over the 50-item boundary without duplicating it", async () => {
    implementations.prepareRoomAgentBackgroundContinuation = () => ({ state: "READY", item: {
      ...item("carried"), turn: { ...turn("carried"), roomAgentContinuation: { sourceRunId: "agent_run:original", generation: 1 } }
    } });
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", processedCount: 50, monitoringMissionIds: ["mission:fixture"] }]);
    await env.sleep("1 second");
    await until(() => seen("finishRoomAgentSupervisorTurn").length === 1);
    const history = await h.fetchHistory();
    const event = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes);
    const payload = event?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
    expect(payload).toBeDefined();
    expect(defaultPayloadConverter.fromPayload(payload!)).toMatchObject({ processedCount: 0, pending: [{ turn: { agentRunId: "agent_run:carried" } }] });
    expect(seen("runCodexAppServerTurn")).toHaveLength(1);
    await stopAndKeep("roomAgentSupervisorWorkflow-continue", h);
  }, 15_000);

  it("supervisor records child failure and its safe nested application code", async () => {
    implementations.runCodexAppServerTurn = () => { throw ApplicationFailure.nonRetryable("Synthetic failure", "CODEX_APP_SERVER_TURN_TIMEOUT"); };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", pending: [item()] }]);
    await until(() => seen("finishRoomAgentSupervisorTurn").length === 1);
    expect(seen("finishRoomAgentSupervisorTurn")[0]?.input).toMatchObject({ status: "FAILED", errorCode: "CODEX_APP_SERVER_TURN_TIMEOUT" });
    await stopAndKeep("roomAgentSupervisorWorkflow-child-failure", h);
  }, 15_000);

  it.each([false, true])("supervisor cancels a running child and records INTERRUPTED instead of FAILED (legacy=%s)", async legacy => {
    const finishActivity = legacy ? "markRoomAgentMissionFinished" : "finishRoomAgentSupervisorTurn";
    implementations.runCodexAppServerTurn = async () => {
      const context = Context.current();
      const timer = setInterval(() => context.heartbeat(), 10);
      try { await context.cancelled; } finally { clearInterval(timer); }
    };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", pending: [item("interrupted")] }], legacy);
    await until(() => seen("runCodexAppServerTurn").length === 1);
    await h.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: "mission:fixture", reason: "Synthetic stop during child" });
    await until(() => seen(finishActivity).length === 1);
    expect(seen(finishActivity)[0]?.input).toMatchObject({ status: "INTERRUPTED", statusReason: "Synthetic stop during child" });
    await stopAndKeep(`roomAgentSupervisorWorkflow-interrupt${legacy ? "-legacy" : ""}`, h);
  }, 15_000);

  it("supervisor drains an oversized queue before carrying exactly its 100-item input bound", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let first = true;
    implementations.checkRoomAgentSupervisorTurn = async () => {
      if (first) { first = false; await gate; }
      return "SKIP";
    };
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", pending: [item("initial")], processedCount: 49 }]);
    await until(() => seen("checkRoomAgentSupervisorTurn").length === 1);
    for (let n = 1; n <= 105; n++) await h.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item(`queued-${n}`));
    release();
    await expect.poll(async () => (await h.describe()).status.name, { timeout: 10_000, interval: 10 }).toBe("CONTINUED_AS_NEW");
    const history = await h.fetchHistory();
    const payload = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes)
      ?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
    const carried = defaultPayloadConverter.fromPayload(payload!) as { pending: unknown[]; processedCount: number };
    expect(carried.pending).toHaveLength(100);
    expect(carried.processedCount).toBe(0);
    expect(() => roomAgentSupervisorInputSchema.parse(JSON.parse(JSON.stringify(carried)))).not.toThrow();
    const checks = history.events?.filter(e => e.activityTaskScheduledEventAttributes?.activityType?.name === "checkRoomAgentSupervisorTurn");
    expect(checks).toHaveLength(6); // one original plus five overflow items, none dropped
    await stopAndKeep("roomAgentSupervisorWorkflow-overflow", h);
  }, 20_000);

  it.each([false, true])("supervisor continues after 250 one-second monitoring polls (legacy=%s)", async legacy => {
    implementations.prepareRoomAgentBackgroundContinuation = () => ({ state: "WAITING" });
    implementations.settleRoomAgentBackgroundMission = () => false;
    // Skipping is server-global. Give long, deliberately infinite supervisors
    // their own server so terminated workflows from other cases cannot hold time.
    const isolated = await TestWorkflowEnvironment.createTimeSkipping();
    const queue = `poll-bound-${randomUUID()}`;
    const worker = await Worker.create({ connection: isolated.nativeConnection, namespace: isolated.namespace,
      taskQueue: queue, workflowBundle: legacy ? legacyBundle : bundle, activities });
    const id = `poll-bound-${randomUUID()}`;
    try {
      await worker.runUntil(async () => {
        const started = await isolated.client.workflow.start("roomAgentSupervisorWorkflow", {
          workflowId: id, taskQueue: queue, args: [{ roomId: "room:fixture", monitoringMissionIds: ["mission:fixture"] }]
        });
        const h = isolated.client.workflow.getHandle(id, started.firstExecutionRunId, { followRuns: false });
        await expect(h.result()).rejects.toBeInstanceOf(WorkflowContinuedAsNewError);
        expect((await h.describe()).status.name).toBe("CONTINUED_AS_NEW");
        const history = await h.fetchHistory();
        const activity = legacy ? "settleRoomAgentBackgroundMission" : "prepareRoomAgentBackgroundContinuation";
        expect(history.events?.filter(e => e.activityTaskScheduledEventAttributes?.activityType?.name === activity)).toHaveLength(250);
        expect(history.events?.filter(e => Number(e.timerStartedEventAttributes?.startToFireTimeout?.seconds) === 1)).toHaveLength(250);
        expect(history.events?.filter(e => e.timerFiredEventAttributes)).toHaveLength(250);
        const payload = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes)
          ?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
        expect(defaultPayloadConverter.fromPayload(payload!)).toMatchObject({ processedCount: 0, monitoringMissionIds: ["mission:fixture"] });
        await isolated.client.workflow.getHandle(id).terminate("Synthetic poll-bound test complete");
        await keepHistory(`roomAgentSupervisorWorkflow-polls-${legacy ? "legacy" : "current"}`, h);
      });
    } finally {
      await isolated.client.workflow.getHandle(id).terminate("Synthetic test cleanup").catch(() => undefined);
      await isolated.teardown();
    }
  }, 25_000);

  it("legacy supervisor carries its queue after 50 processed items", async () => {
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", pending: [item(), item("carried-legacy")], processedCount: 49 }], true);
    await until(() => seen("markRoomAgentMissionFinished").length >= 1);
    await expect.poll(async () => (await h.describe()).status.name, { timeout: 10_000, interval: 10 }).toBe("CONTINUED_AS_NEW");
    const history = await h.fetchHistory();
    const payload = history.events?.find(e => e.workflowExecutionContinuedAsNewEventAttributes)
      ?.workflowExecutionContinuedAsNewEventAttributes?.input?.payloads?.[0];
    expect(defaultPayloadConverter.fromPayload(payload!)).toMatchObject({ pending: [{ turn: { agentRunId: "agent_run:carried-legacy" } }], processedCount: 0 });
    await stopAndKeep("roomAgentSupervisorWorkflow-legacy-continue", h);
  }, 15_000);

  it("legacy supervisor records its boolean settle contract without a patch marker", async () => {
    const h = await start("roomAgentSupervisorWorkflow", [{ roomId: "room:fixture", monitoringMissionIds: ["mission:fixture"] }], true);
    await env.sleep("1 second");
    await until(() => seen("settleRoomAgentBackgroundMission").length === 1);
    expect(seen("prepareRoomAgentBackgroundContinuation")).toHaveLength(0);
    await stopAndKeep("roomAgentSupervisorWorkflow-pre-patch", h);
    const history = await h.fetchHistory();
    expect(history.events?.some(e => e.markerRecordedEventAttributes?.markerName === "core_patch")).toBe(false);
    // Replay old emitted commands through CURRENT patched() code, not the historical fixture.
    await Worker.runReplayHistory({ workflowBundle: bundle }, history);
  }, 15_000);

  it.each([true, false])("background action failure runs its non-cancellable failure activity (explicit action ID=%s)", async explicit => {
    implementations.runRoomAgentBackgroundAction = () => { throw ApplicationFailure.nonRetryable("Synthetic failure", "FIXTURE_FAILED"); };
    const h = await start("roomAgentActionWorkflow", [{ bridge: { roomId: "room:fixture", missionId: "mission:fixture" }, ...(explicit ? { actionId: "action:fixture" } : {}) }], false, explicit ? undefined : "space-room-action:action:legacy");
    await expect(h.result()).rejects.toThrow();
    expect(seen("failRoomAgentBackgroundAction")[0]?.input).toEqual({ roomId: "room:fixture", missionId: "mission:fixture", actionId: explicit ? "action:fixture" : h.workflowId.replace(/^space-room-action:/, ""), cancelled: false });
    await keepHistory(`roomAgentActionWorkflow${explicit ? "" : "-legacy-prefix-id"}`, h);
  }, 15_000);

  it("maintenance survives exhausted refresh/reconciliation retries and actually starts consolidation as a child", async () => {
    implementations.refreshAllMonthsMemoryGraphSnapshot = () => { throw Error("Synthetic refresh unavailable"); };
    implementations.reconcileStaleSpaceAgentSessions = () => { throw Error("Synthetic reconciliation unavailable"); };
    const h = await start("memoryMaintenanceWorkflow", [{ traceId: "trace:fixture" }]);
    expect(await h.result()).toMatchObject({ sourceHash: "synthetic-source" });
    expect(seen("refreshAllMonthsMemoryGraphSnapshot").map(c => c.attempt)).toEqual([1, 2, 3]);
    expect(seen("reconcileStaleSpaceAgentSessions").map(c => c.attempt)).toEqual([1, 2, 3]);
    expect(seen("prepareMemoryConsolidation")).toHaveLength(1);
    const history = await h.fetchHistory();
    expect(history.events?.filter(e => e.startChildWorkflowExecutionInitiatedEventAttributes)
      .map(e => e.startChildWorkflowExecutionInitiatedEventAttributes?.workflowType?.name)).toEqual(["memoryConsolidationWorkflow"]);
    expect(history.events?.filter(e => e.childWorkflowExecutionCompletedEventAttributes)).toHaveLength(1);
    await keepHistory("memoryMaintenanceWorkflow", h);
  }, 15_000);

  it("consolidation applies a changeset on the mutation queue before finalizing", async () => {
    implementations.prepareMemoryConsolidation = () => ({ changeSetId: "changeset:fixture", run: { id: "audit:fixture" } });
    const h = await start("memoryConsolidationWorkflow", [{ runId: "audit:fixture", traceId: "trace:fixture" }]);
    expect(await h.result()).toEqual({ id: "audit:fixture", status: "COMPLETED" });
    expect(calls.map(c => c.name)).toEqual(["prepareMemoryConsolidation", "executePersistedMemoryChangeSet", "finalizeMemoryRepair"]);
    await keepHistory("memoryConsolidationWorkflow", h);
  }, 15_000);

  it("mutation workflow retries its activity and returns the persisted result", async () => {
    implementations.executePersistedMemoryChangeSet = () => {
      if (Context.current().info.attempt === 1) throw Error("Synthetic retry");
      return { status: "APPLIED" };
    };
    const h = await start("memoryMutationWorkflow", [{ changeSetId: "changeset:fixture", traceId: "trace:fixture" }]);
    expect(await h.result()).toEqual({ status: "APPLIED" });
    expect(seen("executePersistedMemoryChangeSet").map(c => c.attempt)).toEqual([1, 2]);
    await keepHistory("memoryMutationWorkflow", h);
  }, 15_000);

  it("replays every frozen history through the current workflow bundle", async () => {
    const files = (await readdir(historiesDirectory)).filter(f => f.endsWith(".json")).sort();
    const required = ["dummyTurnWorkflow.json", "codexAppServerTurnWorkflow-runCodexAppServerTurn.json",
      "dotGoalWorkflow-wake.json", "roomAgentSupervisorWorkflow-enqueue.json",
      "roomAgentSupervisorWorkflow-pre-patch.json", "roomAgentActionWorkflow.json",
      "memoryMaintenanceWorkflow.json", "memoryConsolidationWorkflow.json", "memoryMutationWorkflow.json"];
    expect(files).toEqual(expect.arrayContaining(required));
    const histories = await Promise.all(files.map(async file => {
      const json = JSON.parse(await readFile(path.join(historiesDirectory, file), "utf8"));
      expect(json, `${file}: public diagnostic metadata`).toEqual(sanitizeHistory(json));
      return { workflowId: file, history: temporal.api.history.v1.History.fromObject(json) };
    }));
    const replayed: string[] = [];
    for await (const result of Worker.runReplayHistories({ workflowBundle: bundle }, histories)) {
      expect(result.error, result.workflowId).toBeUndefined();
      replayed.push(result.workflowId);
    }
    expect(replayed).toHaveLength(files.length);
  }, 30_000);

  it.each(["dummyTurnWorkflow.json", "dotGoalWorkflow-wake.json"])("batch gate identifies %s when an extra timer violates determinism", async file => {
    const history = temporal.api.history.v1.History.fromObject(JSON.parse(await readFile(path.join(historiesDirectory, file), "utf8")));
    const results = [];
    for await (const result of Worker.runReplayHistories({ workflowsPath: path.join(here, "support/nondeterministic-workflows.ts") },
      [{ workflowId: file, history }])) results.push(result);
    expect(results).toHaveLength(1);
    expect(results[0]?.workflowId).toBe(file);
    expect(results[0]?.error, file).toBeInstanceOf(DeterminismViolationError);
  }, 15_000);
});
