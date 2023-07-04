package main

import (
	"github.com/gocql/gocql"
)

type grant struct {
	Role        string
	Resource    string
	Permissions []string
}

func readGrant(session *gocql.Session, role, resource string) (grant, error) {
	var g grant
	query := session.Query("SELECT role, resource, permissions FROM system_auth.role_permissions WHERE role = ? AND resource = ?", role, resource)
	return g, query.Scan(&g.Role, &g.Resource, &g.Permissions)
}
