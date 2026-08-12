"""Error taxonomy for the workflow executor."""


from temporalio import exceptions


class DefinitionError(Exception):
    """Raised when the FSM definition is invalid or uses unrecognised constructs."""


class ApplicationError(exceptions.ApplicationError):
    """Base for activity errors that cross the boundary into workflow routing."""


class RetryableHttpError(ApplicationError):
    """Raised for HTTP 5xx, 429, timeout, or connection errors."""
    def __init__(self, message: str) -> None:
        super().__init__(message, type="RetryableHttp", non_retryable=False)


class ValidationError(ApplicationError):
    """Raised when an activity encounters a non-retryable constraint violation."""
    def __init__(self, message: str) -> None:
        super().__init__(message, type="ValidationError", non_retryable=True)


class InputValidationError(ValueError):
    """Raised synchronously in the update validator if user input is invalid."""