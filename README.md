# ⚠️ Please prefer using the actively maintained [Databricks (labs) Terraform Provider](https://registry.terraform.io/providers/databrickslabs/databricks/latest/docs).

Databricks Terraform Provider
=============================

## Installing
This provider is a third party Terraform provider and should be
[installed](https://www.terraform.io/docs/configuration/providers.html#third-party-plugins)
in a similiar manner. You can use the makefile and do a `make install` as well.

```sh
go get github.com/medivo/terraform-provider-databricks
cd $GOPATH/src/github.com/medivo/terraform-provider-databricks
make install
```

## Setup

This provider is configured in a similar manner as the Databricks API. In order
for it to work properly
[authentication](https://docs.databricks.com/api/latest/authentication.html)
should be setup properly. The netrc file should look something similar to this:

```
machine <account_id>.cloud.databricks.com
    login <foo@bar.com>
    password <generated_token>
```


## Example
This is a base example of some of the configuration options that can be set on
the different types.


```
provider "databricks" { account = "<account_id>" }

resource "databricks_cluster" "example_cluster" {
  node_type = "r3.xlarge"
  driver_node_type = "r3.xlarge"
  cluster_name = "terraform-test"
  enable_elastic_disk = true
  autotermination_minutes = 15
}

resource "databricks_dbfs" "example_dir" {
  dbfs_path = "/tmp/test/tf-dir"
}

resource "databricks_dbfs" "example_file" {
  dbfs_path = "/tmp/test/databricks.tf"
  source = "databricks.tf" /* this should be a real file */
}

resource "databricks_groups" "example_groups" {
  groups = [
    {
      name = "tf-test"
      members = [
        {
          name = "foo@bar.com",
        }
      ]
    },
  ]
}

resource "databricks_job" "example_job" {
  name  = "example-tf-job"
  cluster_id = "${databricks_cluster.example_cluster.id}"
  libraries = [
    {
      "pypi" = { "package" = "pandas"}
    },
  ]
  schedule = {
    "quartz_cron_expression" = "0 0 12 * * ?"
    "timezone_id" = "America/New_York"
  }
  email_notifications = {
    "on_start"   = ["foo@example.com"]
    "on_success" = ["bar@example.com"]
    "on_failure" = ["baz@example.com"]
  }
  notebook_task = {
    "notebook_path" = "/foo/bar/baz"
    "base_parameters" = {
        foo = "bar"
    }
  }
}
```

## TODOs
- Add tests
- Add [Workspace](https://docs.databricks.com/api/latest/workspace.html)/[Secrets](https://docs.databricks.com/api/latest/secrets.html) resources
