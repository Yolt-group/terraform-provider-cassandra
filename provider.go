package main

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"contact_point": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "A cassandra node",
			},
			"contact_port": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     9142,
				Description: "Cassandra port",
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "the username to connect to cassandra",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "the password to connect to cassandra",
			},
		},

		ConfigureFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"cassandra_keyspace":       resourceCassandraKeyspace(),
			"cassandra_role":           resourceCassandraRole(),
			"cassandra_grant_keyspace": resourceCassandraGrantKeyspace(),
			"cassandra_grant_table":    resourceCassandraGrantTable(),
		},
	}
}

type Config struct {
	ContactPoint string
	ContactPort  int
	Username     string
	Password     string
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	contactPoint := d.Get("contact_point").(string)
	contactPort := d.Get("contact_port").(int)
	username := d.Get("username").(string)
	password := d.Get("password").(string)

	config := &Config{
		ContactPoint: contactPoint,
		ContactPort:  contactPort,
		Username:     username,
		Password:     password,
	}

	client, err := createSession(config)
	if err != nil {
		return nil, err
	}

	log.Println("[DEBUG] Successfully connected to cassandra")
	return client, nil
}
