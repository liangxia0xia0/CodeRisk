package taskstate

import "testing"

func TestSevenStatusesAndTerminals(t *testing.T) {
	all := []Status{
		StatusPending, StatusQueued, StatusRunning,
		StatusSucceeded, StatusPartialSuccess, StatusFailed, StatusCancelled,
	}
	if len(all) != 7 {
		t.Fatalf("状态数量 = %d", len(all))
	}
	for _, status := range []Status{StatusSucceeded, StatusPartialSuccess, StatusFailed, StatusCancelled} {
		if !IsTerminal(status) {
			t.Fatalf("%s 应为终态", status)
		}
	}
	for _, status := range []Status{StatusPending, StatusQueued, StatusRunning} {
		if IsTerminal(status) {
			t.Fatalf("%s 不是终态", status)
		}
		if !CanTransitionTask(status, StatusCancelled) {
			t.Fatalf("%s 应能取消", status)
		}
	}
	for _, status := range []Status{StatusSucceeded, StatusPartialSuccess, StatusFailed, StatusCancelled} {
		if CanTransitionTask(status, StatusCancelled) {
			t.Fatalf("%s 不能再转为 CANCELLED", status)
		}
	}
}

func TestFailedOnlyLeavesViaManualRetryEdge(t *testing.T) {
	if !CanTransitionTask(StatusFailed, StatusQueued) {
		t.Fatal("合规手动重试需要 FAILED -> QUEUED")
	}
	for _, to := range []Status{StatusPending, StatusRunning, StatusSucceeded, StatusPartialSuccess, StatusFailed, StatusCancelled} {
		if CanTransitionTask(StatusFailed, to) {
			t.Fatalf("FAILED 不能转到 %s", to)
		}
	}
	if CanTransitionTask(StatusSucceeded, StatusQueued) || CanTransitionTask(StatusPartialSuccess, StatusQueued) || CanTransitionTask(StatusRunning, StatusQueued) {
		t.Fatal("只有 FAILED 能进入下一次排队")
	}
}

func TestManualRetryAllocatesOnlyWhenEligible(t *testing.T) {
	decision, err := DecideManualRetry(StatusFailed, FailureAnalysisFailed, 1, 0)
	if err != nil || decision != DecisionApply {
		t.Fatalf("首次失败后的手动重试应分配下一次，得到 %v %v", decision, err)
	}
	if _, err := DecideManualRetry(StatusFailed, FailureAnalysisFailed, MaxBusinessAttempts, 0); err != ErrAttemptLimitReached {
		t.Fatalf("第 4 次之后应拒绝，得到 %v", err)
	}
	if _, err := DecideManualRetry(StatusFailed, FailureInvalidInput, 1, 0); err != ErrRetryNotAllowed {
		t.Fatalf("确定性失败应拒绝，得到 %v", err)
	}
	for _, status := range []Status{StatusPending, StatusQueued, StatusRunning, StatusSucceeded, StatusPartialSuccess, StatusCancelled} {
		if _, err := DecideManualRetry(status, FailureAnalysisFailed, 1, 0); err != ErrStateConflict {
			t.Fatalf("%s 手动重试应冲突，得到 %v", status, err)
		}
	}
	if _, err := DecideManualRetry(StatusFailed, FailureAnalysisFailed, 1, 1); err != ErrStateConflict {
		t.Fatalf("仍有未结束尝试时应冲突，得到 %v", err)
	}
}

func TestCancelDoesNotApplyToTerminalSuccess(t *testing.T) {
	decision, err := DecideCancel(StatusRunning)
	if err != nil || decision.Kind != DecisionApply {
		t.Fatal(decision, err)
	}
	decision, err = DecideCancel(StatusCancelled)
	if err != nil || decision.Kind != DecisionIdempotent {
		t.Fatal(decision, err)
	}
	for _, status := range []Status{StatusSucceeded, StatusPartialSuccess, StatusFailed} {
		decision, err = DecideCancel(status)
		if err != nil || decision.Kind != DecisionNoChange {
			t.Fatalf("%s 取消应保持原终态，得到 %v %v", status, decision, err)
		}
	}
}

func TestLateCallbackAfterCancelIsRejected(t *testing.T) {
	decision, err := DecideComplete(StatusCancelled, StatusCancelled, 1, 1, "evt", "evt", StatusSucceeded)
	if err != ErrStaleCallback || decision != DecisionReject {
		t.Fatalf("取消后的成功回调应过期，得到 %v %v", decision, err)
	}
	decision, err = DecideFail(StatusCancelled, StatusCancelled, 1, 1, "evt", "evt", "", string(FailureAnalysisFailed))
	if err != ErrStaleCallback || decision != DecisionReject {
		t.Fatalf("取消后的失败回调应过期，得到 %v %v", decision, err)
	}
}

func TestCompleteAndFailStayOnCurrentAttempt(t *testing.T) {
	decision, err := DecideComplete(StatusRunning, StatusRunning, 2, 1, "evt-2", "evt-1", StatusSucceeded)
	if err != ErrStaleCallback || decision != DecisionReject {
		t.Fatalf("旧尝试成功回调应过期，得到 %v %v", decision, err)
	}
	decision, err = DecideComplete(StatusRunning, StatusRunning, 1, 1, "evt", "evt", StatusSucceeded)
	if err != nil || decision != DecisionApply {
		t.Fatal(decision, err)
	}
	decision, err = DecideComplete(StatusSucceeded, StatusSucceeded, 1, 1, "evt", "evt", StatusSucceeded)
	if err != nil || decision != DecisionIdempotent {
		t.Fatal(decision, err)
	}
	decision, err = DecideComplete(StatusSucceeded, StatusSucceeded, 1, 1, "evt", "evt", StatusPartialSuccess)
	if err != ErrStaleCallback {
		t.Fatalf("已成功的任务不能改成部分成功，得到 %v", err)
	}
	decision, err = DecideFail(StatusPending, StatusPending, 1, 1, "evt", "evt", "", string(FailureInvalidInput))
	if err != nil || decision != DecisionApply {
		t.Fatal(decision, err)
	}
}

func TestWorkerCannotWriteRecoveryFailureCodes(t *testing.T) {
	got, err := CanonicalFailureCode(FailureSourceWorker, FailureExecutionTimeout)
	if err != nil || got != FailureAnalysisFailed {
		t.Fatalf("Worker 超时码应改记为 ANALYSIS_FAILED，得到 %s %v", got, err)
	}
	got, err = CanonicalFailureCode(FailureSourceWorker, FailureExecutionInterrupted)
	if err != nil || got != FailureAnalysisFailed {
		t.Fatalf("Worker 中断码应改记为 ANALYSIS_FAILED，得到 %s %v", got, err)
	}
	got, err = CanonicalFailureCode(FailureSourceRecovery, FailureExecutionTimeout)
	if err != nil || got != FailureExecutionTimeout {
		t.Fatalf("恢复扫描应保留 EXECUTION_TIMEOUT，得到 %s %v", got, err)
	}
	if _, err := CanonicalFailureCode(FailureSourceWorker, "NO_SUCH_CODE"); err != ErrUnknownFailureCode {
		t.Fatalf("未知失败码应拒绝，得到 %v", err)
	}
}

func TestInfrastructureRedeliveryDoesNotAllocate(t *testing.T) {
	queued, err := DecideMarkQueued(StatusQueued, StatusPending)
	if err != nil || queued != DecisionIdempotent {
		t.Fatal(queued, err)
	}
	running, err := DecideMarkRunning(StatusRunning, StatusRunning)
	if err != nil || running != DecisionIdempotent {
		t.Fatal(running, err)
	}
	if MaxBusinessAttempts != 4 {
		t.Fatalf("业务执行上限 = %d", MaxBusinessAttempts)
	}
}
