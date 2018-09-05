package databricks

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform/helper/schema"
	db "github.com/medivo/databricks-go"
)

func resourceCluster() *schema.Resource {
	return &schema.Resource{
		Create: resourceServerCreate,
		Read:   resourceServerRead,
		Update: resourceServerUpdate,
		Delete: resourceServerDelete,
		Schema: map[string]*schema.Schema{
			"cluster_name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"spark_version": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "4.0.x-scala2.11",
			},
			"ssh_keys": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Description: `SSH public key contents that will be added to
				each Spark node in this cluster. The corresponding private keys
				can be used to login with the user name ubuntu on port 2200. Up
				to 10 keys can be specified.`,
				MaxItems: 10,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"node_type": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				Description: `This field encodes, through a single value, the
				resources available to each of the Spark nodes in this
				cluster.`,
			},
			"driver_node_type": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Description: `The node type of the Spark driver. Note that this
				field is optional; if unset, the driver node type will be set
				as the same value as node_type_id defined above.`,
			},
			"num_workers": &schema.Schema{
				Type: schema.TypeInt,
				Description: `Number of worker nodes that this cluster should
				have. A cluster has one Spark Driver and num_workers Executors
				for a total of num_workers + 1 Spark nodes.`,
				Default:  0,
				Optional: true,
			},
			"min_workers": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `Minimum number of worker nodes that this cluster should have.`,
				Default:     0,
				Optional:    true,
			},
			"max_workers": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `Maximum number of worker nodes that this cluster should have`,
				Default:     1,
				Optional:    true,
			},
			"autotermination_minutes": &schema.Schema{
				Type: schema.TypeInt,
				Description: `Automatically terminates the cluster after it is
				inactive for this time in minutes. If not set, this cluster
				will not be automatically terminated. If specified, the
				threshold must be between 10 and 10000 minutes.`,
				Default:  10,
				Optional: true,
				ValidateFunc: func(i interface{}, s string) ([]string, []error) {
					val := i.(int)
					if val < 10 {
						return []string{}, []error{fmt.Errorf(
							"autotermination_minutes must be greater than 10"),
						}
					}
					return []string{}, []error{}
				},
			},
			"enable_elastic_disk": &schema.Schema{
				Type: schema.TypeBool,
				Description: `Autoscaling Local Storage: when enabled, this
				cluster will dynamically acquire additional disk space when its
				Spark workers are running low on disk space.`,
				Default:  false,
				Optional: true,
			},
			"aws_attributes": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"first_on_demand": {
							Description: `The first first_on_demand nodes of
							the cluster will be placed on on-demand instances.
							If this value is greater than 0, the cluster driver
							node in particular will be placed on an on-demand
							instance. If this value is greater than or equal to
							the current cluster size, all nodes will be placed
							on on-demand instances. If this value is less than
							the current cluster size, first_on_demand nodes
							will be placed on on-demand instances and the
							remainder will be placed on availability instances.
							Note that this value does not affect cluster size
							and cannot be mutated over the lifetime of a
							cluster.`,
							Type:     schema.TypeInt,
							Default:  0,
							Optional: true,
						},

						"availability": {
							Description: `Availability type used for all
							subsequent nodes past the first_on_demand ones.
							Note: If first_on_demand is zero, this availability
							type will be used for the entire cluster.`,
							Type:     schema.TypeString,
							Optional: true,
						},

						"zone_id": {
							Description: `Identifier for the availability
							zone/datacenter in which the cluster resides.`,
							Type:     schema.TypeString,
							Optional: true,
						},

						"instance_profile_arn": {
							Description: `Nodes for this cluster will only be
							placed on AWS instances with this instance profile.
							If ommitted, nodes will be placed on instances
							without an IAM instance profile. The instance
							profile must have previously been added to the
							Databricks environment by an account
							administrator.`,
							Type:     schema.TypeString,
							Optional: true,
						},

						"spot_bid_price_percent": {
							Description: `The bid price for AWS spot instances,
							as a percentage of the corresponding instance
							type’s on-demand price.`,
							Type:     schema.TypeInt,
							Optional: true,
							Default:  100,
						},

						"ebs_volume_count": {
							Description: `The number of volumes launched for
							each instance. You can choose up to 10 volumes.
							This feature is only enabled for supported node
							types.`,
							Type:     schema.TypeInt,
							Optional: true,
							Default:  0,
						},

						"ebs_volume_size": {
							Description: `The size of each EBS volume (in GiB)
							launched for each instance. For general purpose
							SSD, this value must be within the range 100 -
							4096. For throughput optimized HDD, this value must
							be within the range 500 - 4096.`,
							Type:     schema.TypeInt,
							Optional: true,
							Default:  500,
						},
					},
				},
			},
		},
	}
}

func resourceServerCreate(data *schema.ResourceData, client interface{}) error {
	awsAttrs := &db.AWSAttributes{}

	// TODO(daniel): clean this up and check type casting better
	configuredAWSAttrs := data.Get("aws_attributes").([]interface{})
	for _, m := range configuredAWSAttrs {
		d := m.(map[string]interface{})
		awsAttrs.FirstOnDemand = d["first_on_demand"].(int32)
		awsAttrs.Availability = d["aws_availability"].(db.AWSAvailability)
		awsAttrs.ZoneID = d["zone_id"].(string)
		arn := d["instance_profile_arn"].(string)
		awsAttrs.InstanceProfileARN = &arn
		pricePercent := d["spot_bid_price_percent"].(int32)
		awsAttrs.SpotBidPricePercent = &pricePercent
		volType := d["ebs_volume_type"].(db.EBSVolumeType)
		awsAttrs.EBSVolumeType = &volType
		volCount := d["ebs_volume_count"].(int32)
		awsAttrs.EBSVolumeCount = &volCount
		volSize := d["ebs_volume_size"].(int32)
		awsAttrs.EBSVolumeSize = &volSize
	}

	createReq := &db.ClusterCreateRequest{
		ClusterName:            data.Get("cluster_name").(string),
		SparkVersion:           data.Get("spark_version").(string),
		NodeTypeID:             data.Get("node_type").(string),
		DriverNodeTypeID:       data.Get("driver_node_type").(string),
		AutoterminationMinutes: int32(data.Get("autotermination_minutes").(int)),
		EnableElasticDisk:      data.Get("enable_elastic_disk").(bool),
		AWSAttributes:          awsAttrs,
	}

	// either set number or workers or configure autoscaling
	numWorkers, ok := data.Get("num_workers").(int32)
	if ok {
		createReq.NumWorkers = &numWorkers
	} else {
		minWorkers, ok := data.Get("min_workers").(int32)
		if !ok {
			minWorkers = 0
		}

		maxWorkers, ok := data.Get("max_workers").(int32)
		if !ok {
			maxWorkers = 2
		}

		createReq.Autoscale = &db.Autoscale{
			Min: minWorkers,
			Max: maxWorkers,
		}
	}

	id, err := client.(*db.Client).Cluster().Create(
		context.Background(),
		createReq,
	)
	if err != nil {
		return err
	}

	data.SetId(id)

	return nil
}

func resourceServerRead(data *schema.ResourceData, client interface{}) error {
	getRes, err := client.(*db.Client).Cluster().Get(
		context.Background(),
		data.Id(),
	)
	if err != nil {
		return err
	}

	// TODO(daniel): add all the attributes
	data.SetId(getRes.ClusterID)
	data.Set("cluster_name", getRes.ClusterName)
	data.Set("spark_version", getRes.SparkVersion)
	data.Set("node_type", getRes.NodeTypeID)
	data.Set("driver_node_type", getRes.DriverNodeTypeID)
	data.Set("enable_elastic_disk", getRes.EnableElasticDisk)
	data.Set("autotermination_minutes", getRes.AutoterminationMinutes)
	if getRes.NumWorkers != nil {
		data.Set("num_workers", getRes.NumWorkers)
	}
	if getRes.Autoscale != nil {
		data.Set("min_workers", getRes.Autoscale.Min)
		data.Set("max_workers", getRes.Autoscale.Max)
	}

	return nil
}

func resourceServerUpdate(data *schema.ResourceData, client interface{}) error {
	awsAttrs := &db.AWSAttributes{}
	configuredAWSAttrs := data.Get("aws_attributes").([]interface{})
	for _, m := range configuredAWSAttrs {
		d := m.(map[string]interface{})
		awsAttrs.FirstOnDemand = d["first_on_demand"].(int32)
		awsAttrs.Availability = d["aws_availability"].(db.AWSAvailability)
		awsAttrs.ZoneID = d["zone_id"].(string)
		arn := d["instance_profile_arn"].(string)
		awsAttrs.InstanceProfileARN = &arn
		pricePercent := d["spot_bid_price_percent"].(int32)
		awsAttrs.SpotBidPricePercent = &pricePercent
		volType := d["ebs_volume_type"].(db.EBSVolumeType)
		awsAttrs.EBSVolumeType = &volType
		volCount := d["ebs_volume_count"].(int32)
		awsAttrs.EBSVolumeCount = &volCount
		volSize := d["ebs_volume_size"].(int32)
		awsAttrs.EBSVolumeSize = &volSize
	}

	editReq := &db.ClusterEditRequest{
		ClusterID:              data.Id(),
		ClusterName:            data.Get("cluster_name").(string),
		SparkVersion:           data.Get("spark_version").(string),
		NodeTypeID:             data.Get("node_type").(string),
		DriverNodeTypeID:       data.Get("driver_node_type").(string),
		AutoterminationMinutes: data.Get("autotermination_minutes").(int32),
		SSHPublicKeys:          data.Get("ssh_keys").([]string),
		EnableElasticDisk:      data.Get("enable_elastic_disk").(bool),
		AWSAttributes:          awsAttrs,
	}
	numWorkers, ok := data.Get("num_workers").(int32)
	if ok {
		editReq.NumWorkers = &numWorkers
	} else {
		minWorkers, ok := data.Get("min_workers").(int32)
		if !ok {
			minWorkers = 0
		}

		maxWorkers, ok := data.Get("max_workers").(int32)
		if !ok {
			maxWorkers = 2
		}

		editReq.Autoscale = &db.Autoscale{
			Min: minWorkers,
			Max: maxWorkers,
		}
	}
	return client.(*db.Client).Cluster().Edit(
		context.Background(),
		editReq,
	)
}

func resourceServerDelete(data *schema.ResourceData, client interface{}) error {
	return client.(*db.Client).Cluster().Delete(
		context.Background(),
		data.Id(),
	)
}
