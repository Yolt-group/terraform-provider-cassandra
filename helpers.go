package main

import (
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func convertFieldToCassandraSet(d *schema.ResourceData, field string) []string {
	if _, ok := d.GetOk(field); !ok {
		return nil
	}

	return strings.Split(d.Get(field).(string), ",")
	//return fmt.Sprintf("{'%'}", strings.ReplaceAll(val, ",", "','"))
}
