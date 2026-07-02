from __future__ import annotations

from typing import Any, Dict


class UserError(ValueError):
    """An error caused by the user's input, safe to show in an API response.

    Anything that is *not* a UserError is treated as an internal bug and
    surfaces as a 500 with a reference id, never as a 400.
    """

    def __init__(self, code: str, message: str, **context: Any):
        self.code = code
        self.message = message
        self.context: Dict[str, Any] = context
        super().__init__(message)

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.context:
            payload.update(self.context)
        return payload
