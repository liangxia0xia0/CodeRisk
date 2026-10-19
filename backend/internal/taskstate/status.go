package taskstate

// 任务状态以架构文档的七态为准。
// 《数据库与任务状态设计》4.4、5.1 表格和第 12 节仍写「六种状态」，
// 与同文件 5.1 正文、5.2 取消规则和架构文档冲突。本包采用七态。

type Status string

const (
	StatusPending        Status = "PENDING"
	StatusQueued         Status = "QUEUED"
	StatusRunning        Status = "RUNNING"
	StatusSucceeded      Status = "SUCCEEDED"
	StatusPartialSuccess Status = "PARTIAL_SUCCESS"
	StatusFailed         Status = "FAILED"
	StatusCancelled      Status = "CANCELLED"
)

const MaxBusinessAttempts = 4

const (
	EventAnalysisRequested    = "analysis.requested"
	EventAnalysisCancel       = "analysis.cancel_requested"
	EventConversationReply    = "conversation.reply_requested"
	AggregateAnalysisTask     = "analysis_task"
)

type FailureCode string

const (
	FailureExecutionTimeout     FailureCode = "EXECUTION_TIMEOUT"
	FailureExecutionInterrupted FailureCode = "EXECUTION_INTERRUPTED"
	FailureModelTimeout         FailureCode = "MODEL_TIMEOUT"
	FailureModelUnavailable     FailureCode = "MODEL_UNAVAILABLE"
	FailureRateLimited          FailureCode = "RATE_LIMITED"
	FailureAnalysisFailed       FailureCode = "ANALYSIS_FAILED"
	FailureInternalError        FailureCode = "INTERNAL_ERROR"
	FailureInvalidInput         FailureCode = "INVALID_INPUT"
	FailureInputTooLarge        FailureCode = "INPUT_TOO_LARGE"
	FailureUnsupportedLanguage  FailureCode = "UNSUPPORTED_LANGUAGE"
	FailureResultInvalid        FailureCode = "RESULT_INVALID"
)

type FailureSource string

const (
	FailureSourceWorker   FailureSource = "worker"
	FailureSourceRecovery FailureSource = "recovery"
)

func IsTerminal(status Status) bool {
	switch status {
	case StatusSucceeded, StatusPartialSuccess, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func IsOpenAttempt(status Status) bool {
	return status == StatusPending || status == StatusRunning
}

// CanTransitionTask 是任务状态图。FAILED -> QUEUED 只表示迁移图允许手动重试，
// 是否合规还要再检查失败码、未结束尝试和次数。
// 5.2 正文「终态没有向外的迁移」与该图冲突；这里采用状态图、第 6 节和架构文档。
func CanTransitionTask(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusQueued || to == StatusFailed || to == StatusCancelled
	case StatusQueued:
		return to == StatusRunning || to == StatusFailed || to == StatusCancelled
	case StatusRunning:
		return to == StatusSucceeded || to == StatusPartialSuccess || to == StatusFailed || to == StatusCancelled
	case StatusFailed:
		return to == StatusQueued
	default:
		return false
	}
}

func CanTransitionAttempt(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusRunning || to == StatusFailed || to == StatusCancelled
	case StatusRunning:
		return to == StatusSucceeded || to == StatusPartialSuccess || to == StatusFailed || to == StatusCancelled
	default:
		return false
	}
}

func Retryable(code FailureCode) bool {
	switch code {
	case FailureExecutionTimeout, FailureExecutionInterrupted, FailureModelTimeout,
		FailureModelUnavailable, FailureRateLimited, FailureAnalysisFailed, FailureInternalError:
		return true
	default:
		return false
	}
}

func KnownFailureCode(code FailureCode) bool {
	switch code {
	case FailureExecutionTimeout, FailureExecutionInterrupted, FailureModelTimeout,
		FailureModelUnavailable, FailureRateLimited, FailureAnalysisFailed, FailureInternalError,
		FailureInvalidInput, FailureInputTooLarge, FailureUnsupportedLanguage, FailureResultInvalid:
		return true
	default:
		return false
	}
}

// CanonicalFailureCode 落实「EXECUTION_TIMEOUT 与 EXECUTION_INTERRUPTED 只由恢复扫描写入」。
func CanonicalFailureCode(source FailureSource, code FailureCode) (FailureCode, error) {
	if !KnownFailureCode(code) {
		return "", ErrUnknownFailureCode
	}
	if source == FailureSourceWorker && (code == FailureExecutionTimeout || code == FailureExecutionInterrupted) {
		return FailureAnalysisFailed, nil
	}
	if source != FailureSourceWorker && source != FailureSourceRecovery {
		return "", ErrUnknownFailureCode
	}
	return code, nil
}
