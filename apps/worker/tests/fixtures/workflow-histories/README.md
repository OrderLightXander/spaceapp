# Real workflow histories and replay gate

Run `npm test -w @space/worker -- workflow-real-sandbox` after the normal locked install and internal-package build. This test is part of the ordinary worker suite, public test command and public CI. It starts a disposable Temporal time-skipping server and real workers, uses the production workflow code, and stubs activities outside the sandbox. It needs no Docker, database, provider account or credentials. The SDK downloads its test-server binary on its first use; keep that cache for offline subsequent runs.

The JSON files are actual `fetchHistory()` results from synthetic workflows, serialized through the SDK protobuf History type. They are not hand-authored command lists. All eight workflow types have recorded histories. Replay always uses current production code and `Worker.runReplayHistories`. A test-only extra timer must fail replay with `DeterminismViolationError`.

`legacy-workflows.ts` freezes the production legacy supervisor branch from source commit `f1c4e8351bb899ab9bd741b6e0d531b6ba8436c8`. It is used to record commands without the `room-background-continuation-v1` marker. Those histories must replay through the **current** patched workflow and take its legacy branch. This is synthetic compatibility evidence, not a claim to have imported customer histories. The original legacy function body SHA256 before adding this test fixture was `dbc88e3ee1d8539855aeb89d80c5ed4a0a6c6c37ddd0cc854dd2e87515b54b7a` after renaming its exported entrypoint. Never register the historical or deliberately incompatible entrypoints in production workers.

To record an initial baseline, run `npm run temporal:record-histories -w @space/worker`. Existing files are never overwritten, even before the initial commit. Once histories are tracked, recording requires `-- --patch NEW_PATCH_ID` and an accompanying uncommitted production diff that adds that `patched()` identifier. New histories receive a `--NEW_PATCH_ID` suffix; old histories remain in the replay corpus. Reusing a suffix preserves its existing files too. Reviewers must still check that the accompanying workflow change is replay-safe. **Never re-record histories just to make a failing replay pass.** Preserve old branch histories when adding new ones; the frozen legacy producer is not updated to match new behavior.

Activity return values and errors are synthetic. Terminated supervisor histories intentionally capture signals, cancellation and continue-as-new; an infinite supervisor is not expected to complete normally. Each test terminates its remaining latest run, and teardown drains all workers and shuts down the test server.

No workflow logic, timeouts, retry policies, or production worker registration changes are needed for this contribution.

Histories use one compact JSON event per line. This keeps the large timer and
continuation cases reviewable without deleting events or changing replay data.
Formatting existing histories does not authorize re-recording them.

Public histories omit diagnostic stack traces and replace worker identities with
`synthetic-worker` so local paths and hostnames are not published. This metadata
normalization does not re-record executions or change events, commands, payloads,
failure types/messages, timing, or patch markers. Replay still checks the original
recorded behavior.

The historical producer also keeps its timeout constants, queue helpers and
completion mapping locally, copied from that same source revision. It does not
import those mutable helpers from current production code. Contracts and the
child workflow entrypoint remain shared; the frozen supervisor's recorded
commands are always replayed through current production code.
