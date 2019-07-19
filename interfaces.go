package consul_kv_locker

import "github.com/hashicorp/consul/api"

type ConsulService interface {
	CreateSession(session *api.SessionEntry) error
	AcquireLock(key string, opts *api.QueryOptions) (bool, error)
	ReleaseLock(key string, opts *api.QueryOptions) (bool, error)
	RenewSession(doneChan <-chan struct{}) error
	DestroySession() error
}
