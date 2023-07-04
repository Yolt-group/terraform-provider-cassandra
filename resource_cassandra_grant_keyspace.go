package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceCassandraGrantKeyspace() *schema.Resource {
	return &schema.Resource{
		Create: resourceGrantKeyspaceCreateOrUpdate,
		Update: resourceGrantKeyspaceCreateOrUpdate,
		Read:   resourceGrantKeyspaceRead,
		Delete: resourceGrantKeyspaceDelete,
		Exists: resourceGrantKeyspaceExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of role",
			},
			"keyspace": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: "Keyspace",
			},
			"permissions": {
				Type: schema.TypeSet,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Required:    true,
				ForceNew:    false,
				Description: "Permissions",
			},
		},
	}
}

func resourceGrantKeyspaceExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	role := d.Get("role").(string)
	keyspace := d.Get("keyspace").(string)
	resource := fmt.Sprintf("data/%s", keyspace)
	session := meta.(*gocql.Session)

	if _, err := readGrant(session, role, resource); err != nil {
		return false, fmt.Errorf("failed to read grant for role=%s and resource=%s: %s", role, resource, err)
	}

	return true, nil
}

func resourceGrantKeyspaceCreateOrUpdate(d *schema.ResourceData, meta interface{}) error {
	role := d.Get("role").(string)
	keyspace := d.Get("keyspace").(string)
	resource := fmt.Sprintf("data/%s", keyspace)
	permissions := d.Get("permissions").(*schema.Set).List()
	session := meta.(*gocql.Session)

	if err := session.Query("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES (?, ?, ?)", role, resource, permissions).Exec(); err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("%s_%s", role, keyspace))
	return nil
}

func resourceGrantKeyspaceRead(d *schema.ResourceData, meta interface{}) error {
	role := d.Get("role").(string)
	keyspace := d.Get("keyspace").(string)
	resource := fmt.Sprintf("data/%s", keyspace)
	session := meta.(*gocql.Session)

	grant, err := readGrant(session, role, resource)
	if err != nil {
		return fmt.Errorf("failed to read grant for role=%s and resource=%s: %s", role, resource, err)
	}

	d.Set("permissions", grant.Permissions)
	d.SetId(fmt.Sprintf("%s_%s", role, keyspace))

	return nil
}

func resourceGrantKeyspaceDelete(d *schema.ResourceData, meta interface{}) error {
	role := d.Get("role").(string)
	keyspace := d.Get("keyspace").(string)
	resource := fmt.Sprintf("data/%s", keyspace)
	session := meta.(*gocql.Session)

	if err := session.Query("DELETE FROM system_auth.role_permissions WHERE role = ? AND resource = ?", role, resource).Exec(); err != nil {
		return err
	}

	d.SetId("")
	return nil
}
