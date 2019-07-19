package consul_kv_locker

import (
	"github.com/hashicorp/consul/api"
)

type ConsulServiceClient struct {
	client         *api.Client
	sessionID      string
	sessionTimeout string
}

func (c *ConsulServiceClient) GetKey(path string, q *api.QueryOptions) ([]byte, error) {
	panic("implement me")
}

// CreateSession creates consul session and stores it to ConsulServiceClient
// if given session is nil, creates default one
func (c *ConsulServiceClient) CreateSession(session *api.SessionEntry) error {

	if session == nil {
		session = &api.SessionEntry{
			TTL:      c.sessionTimeout,
			Behavior: "release",
		}
	}

	sessionID, _, err := c.client.Session().Create(session, nil)
	if err != nil {
		return err
	}

	c.sessionID = sessionID
	return nil
}

func (c *ConsulServiceClient) AcquireLock(key string, opts *api.QueryOptions) (bool, error) {

	pair, _, err := c.client.KV().Get(key, opts)
	if err != nil {
		return false, err
	}

	KVpair := &api.KVPair{
		Key:     key,
		Value:   pair.Value,
		Session: c.sessionID,
	}

	aquired, _, err := c.client.KV().Acquire(KVpair, nil)
	return aquired, err
}

func (c *ConsulServiceClient) ReleaseLock(key string, opts *api.QueryOptions) (bool, error) {

	pair, _, err := c.client.KV().Get(key, opts)
	if err != nil {
		return false, err
	}

	KVpair := &api.KVPair{
		Key:     key,
		Value:   pair.Value,
		Session: c.sessionID,
	}

	released, _, err := c.client.KV().Release(KVpair, nil)
	return released, err
}

func (c *ConsulServiceClient) RenewSession(doneChan <-chan struct{}) error {
	err := c.client.Session().RenewPeriodic(c.sessionTimeout, c.sessionID, nil, doneChan)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsulServiceClient) DestroySession() error {
	if _, err := c.client.Session().Destroy(c.sessionID, nil); err != nil {
		return err
	}

	return nil
}

func NewConsulService(client *api.Client, sessionTimeout string) *ConsulServiceClient {
	return &ConsulServiceClient{
		client:         client,
		sessionTimeout: sessionTimeout,
	}
}
