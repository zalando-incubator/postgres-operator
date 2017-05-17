package controller

import (
	"fmt"
	"time"

	etcdclient "github.com/coreos/etcd/client"
)

func (c *Controller) initEtcdClient(etcdHost string) error {
	etcdURL := fmt.Sprintf("http://%s", etcdHost)

	cfg, err := etcdclient.New(etcdclient.Config{
		Endpoints:               []string{etcdURL},
		Transport:               etcdclient.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	})
	if err != nil {
		return err
	}

	c.EtcdClient = etcdclient.NewKeysAPI(cfg)

	return nil
}
