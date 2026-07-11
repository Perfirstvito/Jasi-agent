from __future__ import annotations


class RuntimeFailure(Exception):
    code = "runtime_failure"

    def safe_message(self) -> str:
        return str(self) or self.code


class ModelFailure(RuntimeFailure):
    code = "model_failure"


class ModelTimeout(ModelFailure):
    code = "model_timeout"


class ToolFailure(RuntimeFailure):
    code = "tool_failure"


class ToolRejected(RuntimeFailure):
    code = "tool_rejected"


class MaxStepsExceeded(RuntimeFailure):
    code = "max_steps_exceeded"


class HookGuardRejected(RuntimeFailure):
    code = "hook_guard_rejected"


class HookTransformFailed(RuntimeFailure):
    code = "hook_transform_failed"
