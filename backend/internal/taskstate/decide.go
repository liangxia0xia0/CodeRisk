package taskstate

import "errors"

var (
	ErrIllegalTransition   = errors.New("illegal task transition")
	ErrStaleCallback       = errors.New("stale callback")
	ErrStateConflict       = errors.New("task state conflict")
	ErrRetryNotAllowed     = errors.New("retry not allowed")
	ErrAttemptLimitReached = errors.New("attempt limit reached")
	ErrUnknownFailureCode  = errors.New("unknown failure code")
	ErrThreadMismatch      = errors.New("thread does not belong to user")
)

type Decision int

const (
	DecisionApply Decision = iota
	DecisionIdempotent
	DecisionNoChange
	DecisionReject
)

type CancelDecision struct {
	Kind Decision
}

func DecideCancel(status Status) (CancelDecision, error) {
	switch status {
	case StatusPending, StatusQueued, StatusRunning:
		if !CanTransitionTask(status, StatusCancelled) {
			return CancelDecision{}, ErrIllegalTransition
		}
		return CancelDecision{Kind: DecisionApply}, nil
	case StatusCancelled:
		return CancelDecision{Kind: DecisionIdempotent}, nil
	case StatusSucceeded, StatusPartialSuccess, StatusFailed:
		return CancelDecision{Kind: DecisionNoChange}, nil
	default:
		return CancelDecision{}, ErrIllegalTransition
	}
}

func DecideComplete(taskStatus, attemptStatus Status, currentAttempt, callbackAttempt int, attemptEvent, callbackEvent string, result Status) (Decision, error) {
	if result != StatusSucceeded && result != StatusPartialSuccess {
		return DecisionReject, ErrIllegalTransition
	}
	if callbackAttempt != currentAttempt || callbackEvent != attemptEvent {
		return DecisionReject, ErrStaleCallback
	}
	if taskStatus == result && attemptStatus == result {
		return DecisionIdempotent, nil
	}
	if taskStatus == StatusRunning && attemptStatus == StatusRunning && CanTransitionTask(taskStatus, result) && CanTransitionAttempt(attemptStatus, result) {
		return DecisionApply, nil
	}
	return DecisionReject, ErrStaleCallback
}

func DecideFail(taskStatus, attemptStatus Status, currentAttempt, callbackAttempt int, attemptEvent, callbackEvent, storedCode, incomingCode string) (Decision, error) {
	if callbackAttempt != currentAttempt || callbackEvent != attemptEvent {
		return DecisionReject, ErrStaleCallback
	}
	if taskStatus == StatusFailed && attemptStatus == StatusFailed && storedCode == incomingCode {
		return DecisionIdempotent, nil
	}
	openTask := taskStatus == StatusPending || taskStatus == StatusQueued || taskStatus == StatusRunning
	openAttempt := attemptStatus == StatusPending || attemptStatus == StatusRunning
	aligned := (taskStatus == StatusRunning && attemptStatus == StatusRunning) ||
		((taskStatus == StatusPending || taskStatus == StatusQueued) && attemptStatus == StatusPending)
	if openTask && openAttempt && aligned && CanTransitionTask(taskStatus, StatusFailed) && CanTransitionAttempt(attemptStatus, StatusFailed) {
		return DecisionApply, nil
	}
	return DecisionReject, ErrStaleCallback
}

func DecideManualRetry(status Status, failureCode FailureCode, attemptCount, openAttempts int) (Decision, error) {
	if status != StatusFailed || openAttempts != 0 || !CanTransitionTask(status, StatusQueued) {
		return DecisionReject, ErrStateConflict
	}
	if !KnownFailureCode(failureCode) || !Retryable(failureCode) {
		return DecisionReject, ErrRetryNotAllowed
	}
	if attemptCount >= MaxBusinessAttempts {
		return DecisionReject, ErrAttemptLimitReached
	}
	if attemptCount < 1 {
		return DecisionReject, ErrStateConflict
	}
	return DecisionApply, nil
}

func DecideMarkQueued(taskStatus, attemptStatus Status) (Decision, error) {
	if taskStatus == StatusQueued && attemptStatus == StatusPending {
		return DecisionIdempotent, nil
	}
	if taskStatus == StatusPending && attemptStatus == StatusPending && CanTransitionTask(taskStatus, StatusQueued) {
		return DecisionApply, nil
	}
	return DecisionReject, ErrStaleCallback
}

func DecideMarkRunning(taskStatus, attemptStatus Status) (Decision, error) {
	if taskStatus == StatusRunning && attemptStatus == StatusRunning {
		return DecisionIdempotent, nil
	}
	if taskStatus == StatusQueued && attemptStatus == StatusPending && CanTransitionTask(taskStatus, StatusRunning) && CanTransitionAttempt(attemptStatus, StatusRunning) {
		return DecisionApply, nil
	}
	return DecisionReject, ErrStaleCallback
}
