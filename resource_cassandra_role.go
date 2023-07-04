package main

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceCassandraRole() *schema.Resource {
	return &schema.Resource{
		Create: resourceRoleCreateOrUpdate,
		Update: resourceRoleCreateOrUpdate,
		Read:   resourceRoleRead,
		Delete: resourceRoleDelete,
		Exists: resourceRoleExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of role",
			},
			"superuser": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				ForceNew:    false,
				Description: "Allow role to create and manage other roles",
			},
			"login": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				ForceNew:    false,
				Description: "Enables role to be able to login",
			},
			"member_of": {
				Type: schema.TypeSet,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Optional:    true,
				Required:    false,
				ForceNew:    false,
				Description: "Member of role names",
			},
		},
	}
}

type role struct {
	Name      string
	Login     bool
	Superuser bool
	MemberOf  []string
}

func readRole(session *gocql.Session, name string) (role, error) {
	var r role
	query := session.Query("SELECT role, can_login, is_superuser, member_of FROM system_auth.roles WHERE role = ?", name)
	return r, query.Scan(&r.Name, &r.Login, &r.Superuser, &r.MemberOf)
}

func resourceRoleExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	name := d.Get("name").(string)
	session := meta.(*gocql.Session)

	if _, err := readRole(session, name); err != nil {
		return false, fmt.Errorf("failed to read role for name=%s: %s", name, err)
	}

	return true, nil
}

func resourceRoleCreateOrUpdate(d *schema.ResourceData, meta interface{}) error {
	name := d.Get("name").(string)
	login := d.Get("login").(bool)
	superuser := d.Get("superuser").(bool)
	memberOf := d.Get("member_of").(*schema.Set).List()
	session := meta.(*gocql.Session)

	if err := session.Query("INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of) VALUES (?, ?, ?, ?)", name, login, superuser, memberOf).Exec(); err != nil {
		return fmt.Errorf("failed to read role for name=%s: %s", name, err)
	}

	d.SetId(name)
	return nil
}

func resourceRoleRead(d *schema.ResourceData, meta interface{}) error {
	name := d.Get("name").(string)
	session := meta.(*gocql.Session)

	role, err := readRole(session, name)
	if err != nil {
		return err
	}

	d.SetId(role.Name)
	d.Set("name", role.Name)
	d.Set("superuser", role.Superuser)
	d.Set("login", role.Login)
	d.Set("member_of", role.MemberOf)

	return nil
}

func resourceRoleDelete(d *schema.ResourceData, meta interface{}) error {
	name := d.Get("name").(string)
	session := meta.(*gocql.Session)

	if err := session.Query("DELETE FROM system_auth.roles WHERE role = ?", name).Exec(); err != nil {
		return err
	}

	d.SetId("")
	return nil
}
