package main

import (
	"github.com/hashicorp/terraform/plugin"
	db "github.com/medivo/terraform-provider-databricks/databricks"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: db.Provider,
	})
}
