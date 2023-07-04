package main

import (
	"fmt"

	"github.com/gocql/gocql"

	"log"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
)

const (
	SimpleStrategy          = "SimpleStrategy"
	NetworkTopologyStrategy = "NetworkTopologyStrategy"
)

type keyspaceConfig struct {
	Name                string
	ReplicationStrategy string
	ReplicationFactor   int
	Datacenters         map[string]interface{}
}

// Create cassandra connection
func createSession(config *Config) (*gocql.Session, error) {
	cluster := gocql.NewCluster(config.ContactPoint)
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: config.Username, Password: config.Password}
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 1 * time.Minute
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Port = config.ContactPort
	cluster.SslOpts = &gocql.SslOptions{EnableHostVerification: false}
	session, err := cluster.CreateSession()

	if err != nil {
		fmt.Println("Error connecting to cassandra")
		return nil, err
	}

	return session, nil
}

func metaToKeyspaceConfig(d *schema.ResourceData, meta interface{}) keyspaceConfig {
	keyspaceName := d.Get("name").(string)
	replicationStrategy := d.Get("replication_strategy").(string)
	replicationFactor := d.Get("replication_factor").(int)

	iDatacenters := d.Get("datacenters").(map[string]interface{})
	datacenters := make(map[string]interface{})
	for datacenter, RF := range iDatacenters {
		datacenters[datacenter] = RF
	}

	return keyspaceConfig{
		Name:                keyspaceName,
		ReplicationStrategy: replicationStrategy,
		ReplicationFactor:   replicationFactor,
		Datacenters:         datacenters,
	}
}

func generateUpdateKeyspaceQuery(config keyspaceConfig) (string, error) {
	return generateKeyspaceQuery(config, "ALTER")
}

func generateCreateKeyspaceQuery(config keyspaceConfig) (string, error) {
	return generateKeyspaceQuery(config, "CREATE")
}

func generateKeyspaceQuery(conf keyspaceConfig, method string) (string, error) {

	log.Printf("[INFO] %s keyspace with name: %s", method, conf.Name)

	stmt := fmt.Sprintf("%s KEYSPACE %s WITH replication = {'class': '%s'", method, conf.Name, conf.ReplicationStrategy)

	switch conf.ReplicationStrategy {
	case SimpleStrategy:
		stmt += fmt.Sprintf(", 'replication_factor' : '%d'", conf.ReplicationFactor)
	case NetworkTopologyStrategy:
		for datacenter, replicationFactor := range conf.Datacenters {
			stmt += fmt.Sprintf(", '%s' : '%s'", datacenter, replicationFactor)
		}
	default:
		return "", fmt.Errorf("[ERROR] Invalid replication strategy for keyspace %s", conf.Name)
	}

	stmt += " } AND durable_writes = true;"

	return stmt, nil
}

func contains(elts []string, elt string) bool {
	for _, e := range elts {
		if e == elt {
			return true
		}
	}
	return false
}
