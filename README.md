A [Terraform][1] plugin for managing [Apache Cassandra][2].

# Requirements
* Cassandra 3.0 or higher

# Example

```hcl
provider "cassandra" {
  contact_points  = ["localhost:9042"]
  username        = "username"
  password        = "password"
}

resource "cassandra_keyspace" "foo" {
  name                  = "foo"
  replication_strategy  = "SimpleStrategy"
  replication_factor    = 1
}

```

# Resources
* cassandra_keyspace

# Planned Resources
* User creation (Authentication)
* User access (Authorization)

[1]: https://www.terraform.io
[2]: https://cassandra.apache.org
