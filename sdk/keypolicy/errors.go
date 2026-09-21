package keypolicy

import "errors"

// AccessError is request-scoped; it must never cool down an upstream credential.
type AccessError struct{ Cause error }

func (e *AccessError) Error() string {
	switch {
	case errors.Is(e.Cause, ErrDenied):
		return "key_auth_forbidden: this API key is not authorized for this resource or model"
	case errors.Is(e.Cause, ErrBudget):
		return "key_budget_exhausted: no available budget for this resource in the current cycle"
	case errors.Is(e.Cause, ErrInvalid):
		return "key_policy_invalid: invalid policy or token accounting data"
	default:
		return "key_budget_unavailable: price, cycle, usage or persistent policy data is unavailable"
	}
}
func (e *AccessError) Unwrap() error         { return e.Cause }
func (e *AccessError) IsRequestScoped() bool { return true }
func (e *AccessError) StatusCode() int {
	switch {
	case errors.Is(e.Cause, ErrDenied):
		return 403
	case errors.Is(e.Cause, ErrBudget):
		return 429
	case errors.Is(e.Cause, ErrInvalid):
		return 400
	default:
		return 503
	}
}
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	if IsAccessError(err) {
		return err
	}
	return &AccessError{Cause: err}
}
func IsAccessError(err error) bool { var target *AccessError; return errors.As(err, &target) }
