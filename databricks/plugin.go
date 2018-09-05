package databricks

import (
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
	db "github.com/medivo/databricks-go"
)

// Provider is a terraform ResourceProvider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		ResourcesMap: map[string]*schema.Resource{
			"databricks_cluster": resourceCluster(),
			"databricks_dbfs":    resourceDBFS(),
			"databricks_groups":  resourceGroups(),
			"databricks_job":     resourceJobs(),
		},
		Schema: map[string]*schema.Schema{
			"account": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
		},
		ConfigureFunc: func(data *schema.ResourceData) (interface{}, error) {
			println(data.Get("account").(string))
			return db.NewClient(
				data.Get("account").(string),
				db.ClientHTTPClient(db.NetrcHTTPClient),
			)
		},
	}
}
