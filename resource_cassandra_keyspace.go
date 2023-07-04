package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceCassandraKeyspace() *schema.Resource {
	return &schema.Resource{
		Create: resourceCreateKeyspace,
		Read:   resourceReadKeyspace,
		Delete: resourceDeleteKeyspace,
		Update: resourceUpdateKeyspace,
		Exists: resourceExistsKeyspace,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the keyspace",
			},
			"replication_strategy": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: "The replication strategy, currently supporting SimpleStrategy or NetworkTopologyStrategy",
			},
			"replication_factor": {
				Type:        schema.TypeInt,
				Optional:    true,
				ForceNew:    false,
				Description: "number of replicas in case of SimpleStrategy",
			},
			"datacenters": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "the data centers & replication factor in case of NetworkTopologyStrategy",
			},
		},
	}
}

func resourceCreateKeyspace(d *schema.ResourceData, meta interface{}) error {
	config := metaToKeyspaceConfig(d, meta)
	cqlSession := meta.(*gocql.Session)

	stmt, err := generateCreateKeyspaceQuery(config)

	if err != nil {
		return err
	}

	err = cqlSession.Query(stmt).Exec()
	switch err.(type) {
	case *gocql.RequestErrAlreadyExists:
		if err := resourceUpdateKeyspace(d, meta); err != nil {
			return err
		}
	default:
		if err != nil {
			return err
		}
	}

	d.SetId(config.Name)

	return resourceReadKeyspace(d, meta)
}

func resourceReadKeyspace(d *schema.ResourceData, meta interface{}) error {

	stmt := "SELECT keyspace_name, replication FROM system_schema.keyspaces WHERE keyspace_name = ?"

	name := d.Get("name")
	cqlSession := meta.(*gocql.Session)

	var keyspaceName string
	var replication map[string]string

	if err := cqlSession.Query(stmt, name).Scan(&keyspaceName, &replication); err != nil {
		return errors.New(fmt.Sprintf("%s (%s): %v", stmt, name, err))
	}

	d.Set("name", keyspaceName)

	replicationStrategyClassArray := strings.Split(replication["class"], ".")
	replicationStrategy := replicationStrategyClassArray[len(replicationStrategyClassArray)-1]

	switch replicationStrategy {
	case SimpleStrategy:
		replicationFactor := replication["replication_factor"]
		d.Set("replication_factor", replicationFactor)
	case NetworkTopologyStrategy:
		var datacenters = make(map[string]interface{})
		for k, v := range replication {
			//Skip the replication strategy entry in the map
			if k != "class" {
				datacenters[k] = v
			}
		}
		d.Set("datacenters", datacenters)
	default:
		return fmt.Errorf("Unsupported replication strategy retrieved from the database ")
	}

	d.Set("replication_strategy", replicationStrategy)

	return nil
}

func resourceDeleteKeyspace(d *schema.ResourceData, meta interface{}) error {
	conf := metaToKeyspaceConfig(d, meta)
	cqlSession := meta.(*gocql.Session)

	stmt := fmt.Sprintf("DROP KEYSPACE %s;", conf.Name)

	err := cqlSession.Query(stmt).Exec()
	if err != nil {
		if !strings.Contains(err.Error(), "Cannot drop non existing keyspace") {
			return err
		}
	}

	d.SetId("")

	return nil
}

func resourceUpdateKeyspace(d *schema.ResourceData, meta interface{}) error {
	config := metaToKeyspaceConfig(d, meta)
	cqlSession := meta.(*gocql.Session)

	stmt, err := generateUpdateKeyspaceQuery(config)
	if err != nil {
		return err
	}

	if err = cqlSession.Query(stmt).Exec(); err != nil {
		return err
	}

	return resourceReadKeyspace(d, meta)
}

func resourceExistsKeyspace(d *schema.ResourceData, meta interface{}) (bool, error) {
	err := resourceReadKeyspace(d, meta)
	return err == nil, err
}
