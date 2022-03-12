// Code generated by mockery 2.9.0. DO NOT EDIT.

package mocks

import (
	conf "github.com/barcostreams/barco/internal/conf"
	mock "github.com/stretchr/testify/mock"

	time "time"

	types "github.com/barcostreams/barco/internal/types"
)

// Config is an autogenerated mock type for the Config type
type Config struct {
	mock.Mock
}

// AdminPort provides a mock function with given fields:
func (_m *Config) AdminPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// AutoCommitInterval provides a mock function with given fields:
func (_m *Config) AutoCommitInterval() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// BaseHostName provides a mock function with given fields:
func (_m *Config) BaseHostName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ConsumerAddDelay provides a mock function with given fields:
func (_m *Config) ConsumerAddDelay() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// ConsumerPort provides a mock function with given fields:
func (_m *Config) ConsumerPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// ConsumerRanges provides a mock function with given fields:
func (_m *Config) ConsumerRanges() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// ConsumerReadThreshold provides a mock function with given fields:
func (_m *Config) ConsumerReadThreshold() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// CreateAllDirs provides a mock function with given fields:
func (_m *Config) CreateAllDirs() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DatalogPath provides a mock function with given fields: topicDataId
func (_m *Config) DatalogPath(topicDataId *types.TopicDataId) string {
	ret := _m.Called(topicDataId)

	var r0 string
	if rf, ok := ret.Get(0).(func(*types.TopicDataId) string); ok {
		r0 = rf(topicDataId)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// FixedTopologyFilePollDelay provides a mock function with given fields:
func (_m *Config) FixedTopologyFilePollDelay() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// FlowController provides a mock function with given fields:
func (_m *Config) FlowController() conf.FlowController {
	ret := _m.Called()

	var r0 conf.FlowController
	if rf, ok := ret.Get(0).(func() conf.FlowController); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(conf.FlowController)
		}
	}

	return r0
}

// GossipDataPort provides a mock function with given fields:
func (_m *Config) GossipDataPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// GossipPort provides a mock function with given fields:
func (_m *Config) GossipPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// HomePath provides a mock function with given fields:
func (_m *Config) HomePath() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// IndexFilePeriodBytes provides a mock function with given fields:
func (_m *Config) IndexFilePeriodBytes() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// ListenOnAllAddresses provides a mock function with given fields:
func (_m *Config) ListenOnAllAddresses() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// LocalDbPath provides a mock function with given fields:
func (_m *Config) LocalDbPath() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MaxDataBodyLength provides a mock function with given fields:
func (_m *Config) MaxDataBodyLength() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// MaxGroupSize provides a mock function with given fields:
func (_m *Config) MaxGroupSize() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// MaxMessageSize provides a mock function with given fields:
func (_m *Config) MaxMessageSize() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// MaxSegmentSize provides a mock function with given fields:
func (_m *Config) MaxSegmentSize() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// MetricsPort provides a mock function with given fields:
func (_m *Config) MetricsPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// Ordinal provides a mock function with given fields:
func (_m *Config) Ordinal() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// ProducerPort provides a mock function with given fields:
func (_m *Config) ProducerPort() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// ReadAheadSize provides a mock function with given fields:
func (_m *Config) ReadAheadSize() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// SegmentBufferSize provides a mock function with given fields:
func (_m *Config) SegmentBufferSize() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// SegmentFlushInterval provides a mock function with given fields:
func (_m *Config) SegmentFlushInterval() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// ShutdownDelay provides a mock function with given fields:
func (_m *Config) ShutdownDelay() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}
