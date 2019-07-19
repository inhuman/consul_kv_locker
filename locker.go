package consul_kv_locker

import "github.com/hashicorp/consul/api"

type Locker struct {
	service ConsulService
}

func NewLocker(consulService ConsulService) (*Locker, error) {
	l := &Locker{
		service: consulService,
	}

	if err := l.service.CreateSession(nil); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Locker) Lock(key string, opts *api.QueryOptions) (bool, error) {
	return l.service.AcquireLock(key, opts)
}

func (l *Locker) Unlock(key string, opts *api.QueryOptions) (bool, error) {
	return l.service.ReleaseLock(key, opts)
}
