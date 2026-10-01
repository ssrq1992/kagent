package sandbox_test

import (
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/axtest"
	"testing"
)

func startAXFixture(t *testing.T) *axruntime.Client { return axtest.Start(t) }
