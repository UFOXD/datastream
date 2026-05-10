package coordinator

import "github.com/pingcap/errors"

var (
	// ErrNotInitialized is returned when the coordinator is not initialized.
	ErrNotInitialized = errors.Normalize("coordinator not initialized", errors.RFCCodeText("DS:Coordinator:NotInitialized"))

	// ErrConnectionFailed is returned when connection to etcd fails.
	ErrConnectionFailed = errors.Normalize("etcd connection failed", errors.RFCCodeText("DS:Coordinator:ConnectionFailed"))

	// ErrSessionLost is returned when the etcd session is lost.
	ErrSessionLost = errors.Normalize("etcd session lost", errors.RFCCodeText("DS:Coordinator:SessionLost"))

	// ErrLeaderElectFailed is returned when leadership election fails.
	ErrLeaderElectFailed = errors.Normalize("leadership election failed", errors.RFCCodeText("DS:Coordinator:LeaderElectFailed"))

	// ErrInvalidConfig is returned when the configuration is invalid.
	ErrInvalidConfig = errors.Normalize("invalid coordinator config", errors.RFCCodeText("DS:Coordinator:InvalidConfig"))
)
