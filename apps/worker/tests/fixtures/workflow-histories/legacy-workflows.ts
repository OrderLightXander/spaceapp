// Historical pre-patch behavior frozen from production workflows.ts at f1c4e835.
// TEST ONLY: produce old command histories; current production code must replay them.
import { CancellationScope, ChildWorkflowCancellationType, condition, continueAsNew, defineSignal, executeChild, isCancellation, proxyActivities, setHandler } from "@temporalio/workflow";
import { buildCodexAppServerTurnWorkflowId, isSafeErrorCode, ROOM_AGENT_ENQUEUE_SIGNAL, ROOM_AGENT_STOP_SIGNAL, roomAgentSupervisorInputSchema, roomAgentSupervisorQueueItemSchema, roomAgentSupervisorStopSignalSchema, type RoomAgentSupervisorInput, type RoomAgentSupervisorQueueItem, type RoomAgentSupervisorStopSignal, type TurnWorkflowResult } from "@space/contracts";
import type * as activities from "../../../src/activities.js";
import { codexAppServerTurnWorkflow } from "../../../src/workflows.js";
export { codexAppServerTurnWorkflow };

// Frozen with the legacy producer; changes to current helpers cannot alter old recordings.
const ROOM_AGENT_TURN_ACTIVITY_TIMEOUT = "24 hours" as const;
const ROOM_AGENT_TURN_ACTIVITY_MAX_ATTEMPTS = 5;
const ROOM_AGENT_TURN_HEARTBEAT_TIMEOUT = "15 seconds" as const;

/** Map historical turn results to the supervisor completion contract. */
function roomAgentMissionCompletion(result: TurnWorkflowResult): {
  status: "COMPLETED" | "FAILED" | "INTERRUPTED";
  statusReason: string;
} {
  if (result.status === "CANCELLED") {
    return { status: "INTERRUPTED", statusReason: result.message };
  }
  if (result.status === "COMPLETED" && result.roomAgentOutcome?.status === "VERIFIED") {
    return { status: "COMPLETED", statusReason: result.roomAgentOutcome.statusReason };
  }
  if (result.roomAgentOutcome?.status === "UNVERIFIED") {
    return { status: "FAILED", statusReason: result.roomAgentOutcome.statusReason };
  }
  return {
    status: "FAILED",
    statusReason:
      result.status === "FAILED"
        ? result.message
        : "Room Agent turn ended without verified room-action evidence."
  };
}

/** Preserve historical run-ID deduplication when enqueueing follow-ups. */
function enqueueRoomAgentMission(
  pending: RoomAgentSupervisorQueueItem[],
  _activeMissionId: string | null,
  item: RoomAgentSupervisorQueueItem
): RoomAgentSupervisorQueueItem[] {
  const itemKey = item.turn.agentRunId ?? item.turn.traceId;
  if (pending.some((queued) => (queued.turn.agentRunId ?? queued.turn.traceId) === itemKey)) {
    return pending;
  }
  return [...pending, item];
}

/** Check the frozen historical queue for another turn of the same mission. */
function roomAgentMissionHasContinuation(
  pending: RoomAgentSupervisorQueueItem[],
  missionId: string
): boolean {
  return pending.some((item) => item.missionId === missionId);
}

/** Apply the historical stop-all or stop-one mission selection. */
function stopRoomAgentMissions(
  pending: RoomAgentSupervisorQueueItem[],
  activeMissionId: string | null,
  signal: RoomAgentSupervisorStopSignal
): {
  pending: RoomAgentSupervisorQueueItem[];
  interrupted: RoomAgentSupervisorQueueItem[];
  shouldCancelActive: boolean;
} {
  /** Select one mission, or every mission when the historical stop signal is global. */
  const matches = (missionId: string) => signal.missionId === null || signal.missionId === missionId;
  return {
    pending: pending.filter((item) => !matches(item.missionId)),
    interrupted: pending.filter((item) => matches(item.missionId)),
    shouldCancelActive: activeMissionId !== null && matches(activeMissionId)
  };
}

const roomAgentTurnActivities = proxyActivities<typeof activities>({ startToCloseTimeout: ROOM_AGENT_TURN_ACTIVITY_TIMEOUT, heartbeatTimeout: ROOM_AGENT_TURN_HEARTBEAT_TIMEOUT, retry: { maximumAttempts: ROOM_AGENT_TURN_ACTIVITY_MAX_ATTEMPTS } });
const { markRoomAgentMissionStarted, markRoomAgentMissionFinished, markRoomAgentMissionContinued } = proxyActivities<typeof activities>({ startToCloseTimeout: "1 minute", retry: { maximumAttempts: 5 } });
const enqueueRoomAgentMissionSignal = defineSignal<[RoomAgentSupervisorQueueItem]>(ROOM_AGENT_ENQUEUE_SIGNAL);
const stopRoomAgentMissionSignal = defineSignal<[{ missionId: string | null; reason: string }]>(ROOM_AGENT_STOP_SIGNAL);
/** Produce pre-patch supervisor commands for compatibility replay through current code. */
export async function roomAgentSupervisorWorkflow(rawInput: RoomAgentSupervisorInput): Promise<never> {
  const input = roomAgentSupervisorInputSchema.parse(rawInput);
  let pending = [...input.pending];
  let interrupted: Array<{ item: RoomAgentSupervisorQueueItem; reason: string }> = [];
  let activeMissionId: string | null = null;
  let activeScope: CancellationScope | null = null;
  let activeStopReason = "Stopped by operator.";
  let processedCount = input.processedCount;
  const monitoring = new Set(input.monitoringMissionIds);
  let monitoringPolls = 0;

  setHandler(enqueueRoomAgentMissionSignal, (rawItem) => {
    const parsed = roomAgentSupervisorQueueItemSchema.safeParse(rawItem);
    if (!parsed.success || parsed.data.turn.roomId !== input.roomId) return;
    pending = enqueueRoomAgentMission(pending, activeMissionId, parsed.data);
  });

  setHandler(stopRoomAgentMissionSignal, (rawSignal) => {
    const parsed = roomAgentSupervisorStopSignalSchema.safeParse(rawSignal);
    if (!parsed.success) return;
    const stopped = stopRoomAgentMissions(pending, activeMissionId, parsed.data);
    pending = stopped.pending;
    interrupted = [
      ...interrupted,
      ...stopped.interrupted.map((item) => ({ item, reason: parsed.data.reason }))
    ];
    if (stopped.shouldCancelActive) {
      activeStopReason = parsed.data.reason;
      activeScope?.cancel();
    }
  });

  for (;;) {
    if (monitoring.size && !pending.length && !interrupted.length) {
      await condition(() => pending.length > 0 || interrupted.length > 0, "1 second");
      if (!pending.length && !interrupted.length) {
        for (const missionId of monitoring) {
          const finished = await roomAgentTurnActivities.settleRoomAgentBackgroundMission({ roomId: input.roomId, missionId });
          if (finished) monitoring.delete(missionId);
        }
        if (++monitoringPolls >= 250) return continueAsNew<typeof roomAgentSupervisorWorkflow>({ ...input, pending, processedCount, monitoringMissionIds: [...monitoring] });
        continue;
      }
    } else await condition(() => interrupted.length > 0 || pending.length > 0);

    const stoppedBeforeStart = interrupted.shift();
    if (stoppedBeforeStart) {
      await markRoomAgentMissionFinished({
        missionId: stoppedBeforeStart.item.missionId,
        roomId: stoppedBeforeStart.item.turn.roomId,
        status: "INTERRUPTED",
        statusReason: stoppedBeforeStart.reason
      });
      processedCount += 1;
    } else {
      const item = pending.shift()!;
      activeMissionId = item.missionId;
      activeStopReason = "Stopped by operator.";
      activeScope = new CancellationScope({ cancellable: true });
      await markRoomAgentMissionStarted({ missionId: item.missionId, roomId: item.turn.roomId, paneId: item.turn.paneId });
      try {
        const result = await activeScope.run(() =>
          executeChild(codexAppServerTurnWorkflow, {
            args: [item.turn],
            workflowId: buildCodexAppServerTurnWorkflowId(item.turn),
            cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED
          })
        );
        const completion = roomAgentMissionCompletion(result);
        if (result.roomAgentOutcome?.status === "VERIFIED" && result.roomAgentOutcome.pendingActionIds?.length) {
          monitoring.add(item.missionId);
          await markRoomAgentMissionContinued({ missionId: item.missionId, roomId: item.turn.roomId,
            statusReason: "Pane work is running independently. Room Agent is ready for further commands." });
        } else if (monitoring.has(item.missionId) || (completion.status === "COMPLETED" && roomAgentMissionHasContinuation(pending, item.missionId))) {
          await markRoomAgentMissionContinued({
            missionId: item.missionId,
            roomId: item.turn.roomId,
            statusReason: "The current turn completed; continuing the same goal with the next operator follow-up."
          });
        } else {
          await markRoomAgentMissionFinished({
            missionId: item.missionId,
            roomId: item.turn.roomId,
            status: completion.status,
            statusReason: completion.statusReason
          });
        }
      } catch (error) {
        await CancellationScope.nonCancellable(() =>
          markRoomAgentMissionFinished({
            missionId: item.missionId,
            roomId: item.turn.roomId,
            status: isCancellation(error) ? "INTERRUPTED" : "FAILED",
            statusReason: isCancellation(error) ? activeStopReason : "Room agent child workflow failed.",
            ...(isCancellation(error) ? {} : { errorCode: supervisorFailureCode(error) })
          })
        );
      } finally {
        activeMissionId = null;
        activeScope = null;
        processedCount += 1;
      }
    }

    if (processedCount >= 50) {
      await continueAsNew<typeof roomAgentSupervisorWorkflow>({
        roomId: input.roomId,
        pending,
        monitoringMissionIds: [...monitoring],
        interrupted: [],
        processedCount: 0
      });
    }
  }
}

/** Recover the historical safe error code from a bounded chain of nested failures. */
function supervisorFailureCode(error: unknown): string {
  let code = "ROOM_AGENT_SUPERVISOR_FAILED";
  let failure = error as { name?: unknown; type?: unknown; cause?: unknown } | undefined;
  for (let depth = 0; failure && depth < 8; depth++, failure = failure.cause as typeof failure) {
    if (failure.name === "TimeoutFailure") code = "ROOM_AGENT_SUPERVISOR_TIMEOUT";
    else if (isSafeErrorCode(failure.type)) code = failure.type;
  }
  return code;
}
