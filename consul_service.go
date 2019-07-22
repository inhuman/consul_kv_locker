package consul_kv_locker

import (
	consulapi "github.com/hashicorp/consul/api"
)

type ConsulServiceClient struct {
	client         *consulapi.Client
	sessionID      string
	sessionTimeout string
	doneChan       chan struct{}
}

func (c *ConsulServiceClient) StopRenewSession() error {
	c.doneChan <- struct{}{}
	close(c.doneChan)
	return nil
}

// CreateSession creates consul session and stores it to ConsulServiceClient
// if given session is nil, creates default one
func (c *ConsulServiceClient) CreateSession(session *consulapi.SessionEntry) error {

	if session == nil {
		session = &consulapi.SessionEntry{
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

func (c *ConsulServiceClient) AcquireLock(key string, opts *consulapi.QueryOptions) (bool, error) {

	pair, _, err := c.client.KV().Get(key, opts)
	if err != nil {
		return false, err
	}

	KVpair := &consulapi.KVPair{
		Key:     key,
		Value:   pair.Value,
		Session: c.sessionID,
	}

	aquired, _, err := c.client.KV().Acquire(KVpair, nil)
	return aquired, err
}

func (c *ConsulServiceClient) ReleaseLock(key string, opts *consulapi.QueryOptions) (bool, error) {

	pair, _, err := c.client.KV().Get(key, opts)
	if err != nil {
		return false, err
	}

	KVpair := &consulapi.KVPair{
		Key:     key,
		Value:   pair.Value,
		Session: c.sessionID,
	}

	released, _, err := c.client.KV().Release(KVpair, nil)
	return released, err
}

func (c *ConsulServiceClient) StartRenewSession() error {
	err := c.client.Session().RenewPeriodic(c.sessionTimeout, c.sessionID, nil, c.doneChan)
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

func NewConsulService(client *consulapi.Client, sessionTimeout string, doneChan chan struct{}) *ConsulServiceClient {
	return &ConsulServiceClient{
		client:         client,
		sessionTimeout: sessionTimeout,
		doneChan:       doneChan,
	}
}

func DefaultConsulClient(consulHttpAddr string) (*consulapi.Client, error) {

	return consulapi.NewClient(&consulapi.Config{
		Address: consulHttpAddr,
		Scheme:  "http",
	})
}

func DefaultQueryOpts(dc string) *consulapi.QueryOptions {
	return &consulapi.QueryOptions{
		Datacenter: dc,
	}
}
