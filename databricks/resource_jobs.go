package databricks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	db "github.com/medivo/databricks-go"
)

func resourceJobs() *schema.Resource {
	return &schema.Resource{
		Create: resourceJobsCreate,
		Read:   resourceJobsRead,
		Update: resourceJobsUpdate,
		Delete: resourceJobsDelete,
		Schema: map[string]*schema.Schema{
			"created_time": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Job creation time`,
				Computed:    true,
			},
			"name": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Spark cluster id.`,
				Required:    true,
			},
			"cluster_id": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Spark cluster id.`,
				Required:    true,
			},
			"timeout_seconds": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `Job timeout in seconds.`,
				Optional:    true,
			},
			"max_retries": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `An optional maximum number of times to retry an unsuccessful run. A run is considered to be unsuccessful if it completes with a FAILED result_state or INTERNAL_ERROR life_cycle_state.`,
				Optional:    true,
			},
			"min_retry_interval_millis": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `An optional minimal interval in milliseconds between attempts. The default behavior is that unsuccessful runs are immediately retried.`,
				Optional:    true,
			},
			"retry_on_timeout": &schema.Schema{
				Type:        schema.TypeBool,
				Description: `An optional policy to specify whether to retry a job when it times out. The default behavior is to not retry on timeout.`,
				Optional:    true,
				Default:     false,
			},
			"max_concurrent_runs": &schema.Schema{
				Type:        schema.TypeInt,
				Description: `An optional maximum allowed number of concurrent runs of the job.`,
				Optional:    true,
				Default:     1,
			},
			"run_id": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Spark Job run id.`,
				Computed:    true,
			},
			"state": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Job state.`,
				Computed:    true,
			},
			"creator": &schema.Schema{
				Type:        schema.TypeString,
				Description: `Job creator.`,
				Computed:    true,
			},
			"libraries": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"jar": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: `If jar, URI of the jar to be installed. DBFS and S3 URIs are supported.`,
						},
						"egg": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: `If egg, URI of the egg to be installed. DBFS and S3 URIs are supported.`,
						},
						"whl": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: `If whl, URI of the wheel or zipped wheels to be installed. DBFS and S3 URIs are supported. For example: { "whl": "dbfs:/my/whl" } or { "whl": "s3://my-bucket/whl" }. If S3 is used, make sure the cluster has read access on the library. You may need to launch the cluster with an IAM role to access the S3 URI. Also the wheel file name needs to use the correct convention. If zipped wheels are to be installed, the file name suffix should be .wheelhouse.zip.`,
						},
						"pypi": {
							Type:        schema.TypeSet,
							Optional:    true,
							Description: `If pypi, specification of a PyPi library to be installed.`,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"package": &schema.Schema{
										Type:        schema.TypeString,
										Required:    true,
										Description: `The name of the PyPi package to install. An optional exact version specification is also supported. Examples: simplejson and simplejson==3.8.0.`,
									},
									"repo": &schema.Schema{
										Type:        schema.TypeString,
										Optional:    true,
										Description: `The repository where the package can be found. If not specified, the default pip index is used.`,
									},
								},
							},
						},
						"maven": {
							Type:        schema.TypeSet,
							Optional:    true,
							Description: `If maven, specification of a Maven library to be installed.`,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"coordinates": &schema.Schema{
										Type:        schema.TypeString,
										Required:    true,
										Description: `Gradle-style Maven coordinates. For example: org.jsoup:jsoup:1.7.2.`,
									},
									"repo": &schema.Schema{
										Type:        schema.TypeString,
										Optional:    true,
										Description: `Maven repo to install the Maven package from. If omitted, both Maven Central Repository and Spark Packages are searched.`,
									},
									"exclusions": &schema.Schema{
										Type:     schema.TypeList,
										Optional: true,
										Description: `List of dependences to exclude. For example: ["slf4j:slf4j", "*:hadoop-client"].

`,
										Elem: &schema.Schema{
											Type: schema.TypeString,
										},
									},
								},
							},
						},
					},
				},
			},
			"schedule": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"quartz_cron_expression": &schema.Schema{
							Type:        schema.TypeString,
							Description: `A cron expression using quartz syntax that describes the schedule for a job.`,
							Required:    true,
						},
						"timezone_id": &schema.Schema{
							Type:        schema.TypeString,
							Description: `A Java timezone id. The schedule for a job will be resolved with respect to this timezone.`,
							Required:    true,
						},
					},
				},
			},
			"notebook_task": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"notebook_path": {
							Type:        schema.TypeString,
							Required:    true,
							Description: `The absolute path of the notebook to be run in the Databricks Workspace. This path must begin with a slash.`,
						},
						"base_parameters": {
							Type:     schema.TypeMap,
							Optional: true,
							Description: `Base parameters to be used for each run of this job. If the run is initiated by a call to run-now with parameters specified, the two parameters maps will be merged. If the same key is specified in base_parameters and in run-now, the value from run-now will be used.

If the notebook takes a parameter that is not specified in the job’s base_parameters or the run-now override parameters, the default value from the notebook will be used.

These parameters can be retrieved in a notebook by using dbutils.widgets.get().
`,
						},
					},
				},
			},
			"spark_python_task": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"python_file": {
							Type:        schema.TypeString,
							Required:    true,
							Description: `The absolute path of the notebook to be run in the Databricks Workspace. This path must begin with a slash.`,
						},
						"parameters": {
							Type:        schema.TypeList,
							Optional:    true,
							Description: `Command line parameters that will be passed to the Python file.`,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
				},
			},
			"spark_submit_task": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"parameters": {
							Type:        schema.TypeList,
							Optional:    true,
							Description: `Command line parameters that will be passed to spark submit.`,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
				},
			},
			"spark_jar_task": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"main_class_name": {
							Type:     schema.TypeString,
							Required: true,
							Description: `The full name of the class containing the main method to be executed. This class must be contained in a JAR provided as a library.

The code should use SparkContext.getOrCreate to obtain a Spark context; otherwise, runs of the job will fail.`,
						},
						"parameters": {
							Type:        schema.TypeList,
							Optional:    true,
							Description: `Parameters that will be passed to the main method.`,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
				},
			},
			"email_notifications": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"on_start": &schema.Schema{
							Type:        schema.TypeList,
							Description: `A list of email addresses to be notified when a run begins.`,
							Optional:    true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"on_success": &schema.Schema{
							Type:        schema.TypeList,
							Description: `A list of email addresses to be notified when a run successfully completes. A run is considered to have completed successfully if it ends with a TERMINATED life_cycle_state and a SUCCESSFUL result_state.`,
							Optional:    true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"on_failure": &schema.Schema{
							Type:        schema.TypeList,
							Description: `A list of email addresses to be notified when a run unsuccessfully completes. A run is considered to have completed unsuccessfully if it ends with an INTERNAL_ERROR life_cycle_state or a SKIPPED, FAILED, or TIMED_OUT result_state.`,
							Optional:    true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
				},
			},
		},
	}
}

func resourceJobsCreate(data *schema.ResourceData, client interface{}) error {
	jobsService := client.(*db.Client).Jobs()
	ctx := context.Background()

	clusterID := data.Get("cluster_id").(string)

	jobCreateReq := &db.JobCreateRequest{
		ExistingClusterID:  &clusterID,
		Name:               data.Get("name").(string),
		NotebookTask:       getJobNotebookTask(data),
		SparkJarTask:       getJobSparkJarTask(data),
		SparkPythonTask:    getJobSparkPythonTask(data),
		SparkSubmitTask:    getJobSparkSubmitTask(data),
		Libraries:          getJobLibraries(data),
		EmailNotifications: getJobEmailNotifications(data),
		Schedule:           getJobCron(data),
	}

	// going to lose some precision here, but what can you do?
	toIface, ok := data.GetOk("timeout_seconds")
	if ok {
		to := int32(toIface.(int))
		jobCreateReq.TimeoutSeconds = &to
	}

	maxRetriesIface, ok := data.GetOk("max_retries")
	if ok {
		retries := int32(maxRetriesIface.(int))
		jobCreateReq.MaxRetries = &retries
	}

	retryInterIface, ok := data.GetOk("min_retry_interval_millis")
	if ok {
		retryInter := int32(retryInterIface.(int))
		jobCreateReq.MinRetryIntervalMillis = &retryInter
	}

	retryOnTOIface, ok := data.GetOk("retry_on_timeout")
	if ok {
		retryOnTO := retryOnTOIface.(bool)
		jobCreateReq.RetryOnTimeout = &retryOnTO
	}

	maxRunsIface, ok := data.GetOk("max_concurrent_runs")
	if ok {
		maxRuns := int32(maxRunsIface.(int))
		jobCreateReq.MaxRetries = &maxRuns
	}

	id, err := jobsService.Create(ctx, jobCreateReq)
	if err != nil {
		return err
	}

	data.SetId(fmt.Sprintf("%d", id))
	return nil
}

func resourceJobsRead(data *schema.ResourceData, client interface{}) error {
	jobID, err := strconv.ParseInt(data.Id(), 10, 64)
	if err != nil {
		return err
	}
	job, err := client.(*db.Client).Jobs().Get(context.Background(), jobID)
	if err != nil {
		return err
	}
	data.Set("creator", job.CreatorUserName)
	data.Set(
		"created_time",
		time.Unix(0, job.CreatedTime*1000).Format(time.RFC3339),
	)
	return nil
}

func resourceJobsUpdate(data *schema.ResourceData, client interface{}) error {
	jobID, err := strconv.ParseInt(data.Id(), 10, 64)
	if err != nil {
		return err
	}
	clusterID := data.Get("cluster_id").(string)
	settings := db.JobSettings{
		ExistingClusterID:  &clusterID,
		NotebookTask:       getJobNotebookTask(data),
		SparkJarTask:       getJobSparkJarTask(data),
		SparkPythonTask:    getJobSparkPythonTask(data),
		SparkSubmitTask:    getJobSparkSubmitTask(data),
		Libraries:          getJobLibraries(data),
		EmailNotifications: getJobEmailNotifications(data),
		Schedule:           getJobCron(data),
	}

	// going to lose some precision here, but what can you do?
	toIface, ok := data.GetOk("timeout_seconds")
	if ok {
		to := int32(toIface.(int))
		settings.TimeoutSeconds = &to
	}

	maxRetriesIface, ok := data.GetOk("max_retries")
	if ok {
		retries := int32(maxRetriesIface.(int))
		settings.MaxRetries = &retries
	}

	retryInterIface, ok := data.GetOk("min_retry_interval_millis")
	if ok {
		retryInter := int32(retryInterIface.(int))
		settings.MinRetryIntervalMillis = &retryInter
	}

	retryOnTOIface, ok := data.GetOk("retry_on_timeout")
	if ok {
		retryOnTO := retryOnTOIface.(bool)
		settings.RetryOnTimeout = &retryOnTO
	}

	maxRunsIface, ok := data.GetOk("max_concurrent_runs")
	if ok {
		maxRuns := int32(maxRunsIface.(int))
		settings.MaxRetries = &maxRuns
	}

	return client.(*db.Client).Jobs().Reset(
		context.Background(),
		jobID,
		settings,
	)
}

func resourceJobsDelete(data *schema.ResourceData, client interface{}) error {
	jobID, err := strconv.ParseInt(data.Id(), 10, 64)
	if err != nil {
		return err
	}
	return client.(*db.Client).Jobs().Delete(context.Background(), jobID)
}

func getJobCron(data *schema.ResourceData) *db.CronSchedule {
	cronsData := data.Get("schedule").(*schema.Set)
	if cronsData.Len() == 0 {
		return nil
	}
	cronSchedule := &db.CronSchedule{}
	for _, cronData := range cronsData.List() {
		cronMap := cronData.(map[string]interface{})
		for cronOpt, cronOptData := range cronMap {
			if cronOpt == "quartz_cron_expression" {
				cronSchedule.QuartzCronExpression = cronOptData.(string)
			} else {
				cronSchedule.TimezoneID = cronOptData.(string)
			}
		}
	}

	return cronSchedule
}

func getJobEmailNotifications(data *schema.ResourceData) *db.JobEmailNotifications {
	emailsData := data.Get("email_notifications").(*schema.Set)
	if emailsData.Len() == 0 {
		return nil
	}
	emailNotifications := &db.JobEmailNotifications{}
	for _, emailData := range emailsData.List() {
		listData := emailData.(map[string]interface{})
		for listType, listTypeData := range listData {
			emailDataIf := listTypeData.([]interface{})
			emails := make([]string, len(emailDataIf))
			for i, emailIface := range emailDataIf {
				emails[i] = emailIface.(string)
			}
			switch listType {
			case "on_start":
				emailNotifications.OnStart = emails
			case "on_success":
				emailNotifications.OnSuccess = emails
			case "on_failure":
				emailNotifications.OnFailure = emails
			}
		}
	}

	return emailNotifications
}

func getJobLibraries(data *schema.ResourceData) []db.Library {
	libsData := data.Get("libraries").(*schema.Set)
	libs := []db.Library{}
	for _, libData := range libsData.List() {
		lib := libData.(map[string]interface{})
		library := db.Library{}
		for libType, libTypeData := range lib {
			switch libType {
			case "jar":
				jar := libTypeData.(string)
				if len(jar) > 0 {
					library.Jar = &jar
				}
			case "egg":
				egg := libTypeData.(string)
				if len(egg) > 0 {
					library.Egg = &egg
				}
			case "whl":
				whl := libTypeData.(string)
				if len(whl) > 0 {
					library.Whl = &whl
				}
			case "pypi":
				pypiLibrary := db.PythonPyPiLibrary{}
				pypiTfData := libTypeData.(*schema.Set)
				for _, setData := range pypiTfData.List() {
					setDataMap := setData.(map[string]interface{})
					packageStr := setDataMap["package"].(string)
					if len(packageStr) > 0 {
						pypiLibrary.Package = packageStr
					}
					repo, ok := setDataMap["repo"]
					if ok {
						repoStr := repo.(string)
						if len(repoStr) > 0 {
							pypiLibrary.Repo = &repoStr
						}
					}
				}
				library.Pypi = &pypiLibrary
			case "maven":
				mavenLibrary := db.MavenLibrary{}
				mavenTfData := libTypeData.(*schema.Set)
				for _, setData := range mavenTfData.List() {
					setDataMap := setData.(map[string]interface{})
					coords := setDataMap["coordinates"].(string)
					if len(coords) > 0 {
						mavenLibrary.Coordinates = coords
					}
					repo, ok := setDataMap["repo"]
					if ok {
						repoStr := repo.(string)
						if len(repoStr) > 0 {
							mavenLibrary.Repo = &repoStr
						}
					}
					// this is tedious....
					exIfaces := setDataMap["exclusions"].([]interface{})
					exclusions := make([]string, len(exIfaces))
					for i, exIface := range exIfaces {
						exclusions[i] = exIface.(string)
					}
					mavenLibrary.Exclusions = exclusions
				}
				library.Maven = &mavenLibrary
			case "cran":
				cranLibrary := db.RCranLibrary{}
				cranTfData := libTypeData.(*schema.Set)
				for _, setData := range cranTfData.List() {
					setDataMap := setData.(map[string]interface{})
					packageStr := setDataMap["package"].(string)
					if len(packageStr) > 0 {
						cranLibrary.Package = packageStr
					}
					repo, ok := setDataMap["repo"]
					if ok {
						repoStr := repo.(string)
						if len(repoStr) > 0 {
							cranLibrary.Repo = &repoStr
						}
					}
				}
				library.Cran = &cranLibrary
			}
		}
		libs = append(libs, library)
	}

	return libs
}

func getJobNotebookTask(data *schema.ResourceData) *db.NotebookTask {
	notebookIface, ok := data.GetOk("notebook_task")
	if !ok {
		return nil
	}
	notebookTask := &db.NotebookTask{}
	noteSchema := notebookIface.(*schema.Set)

	for _, noteData := range noteSchema.List() {
		noteMap := noteData.(map[string]interface{})
		if pathIface, ok := noteMap["notebook_path"]; ok {
			notebookTask.NotebookPath = pathIface.(string)
		}
		if paramMap, ok := noteMap["base_parameters"]; ok {
			params := []db.ParamPair{}
			for key, val := range paramMap.(map[string]interface{}) {
				params = append(params,
					db.ParamPair{
						Key:   key,
						Value: val.(string),
					},
				)
			}
			notebookTask.BaseParameters = params
		}
	}

	return notebookTask
}

func getJobSparkJarTask(data *schema.ResourceData) *db.SparkJarTask {
	taskIface, ok := data.GetOk("spark_jar_task")
	if !ok {
		return nil
	}
	jarTask := &db.SparkJarTask{}

	taskSchema := taskIface.(*schema.Set)

	for _, taskData := range taskSchema.List() {
		taskMap := taskData.(map[string]interface{})
		if mainIface, ok := taskMap["main_class_name"]; ok {
			jarTask.MainClassName = mainIface.(string)
		}
		if paramSlice, ok := taskMap["parameters"]; ok {
			params := make([]string, len(paramSlice.([]interface{})))
			for i, val := range paramSlice.([]interface{}) {
				params[i] = val.(string)
			}
			jarTask.Parameters = params
		}
	}

	return jarTask
}

func getJobSparkPythonTask(data *schema.ResourceData) *db.SparkPythonTask {
	taskIface, ok := data.GetOk("spark_python_task")
	if !ok {
		return nil
	}
	pythonTask := &db.SparkPythonTask{}

	taskSchema := taskIface.(*schema.Set)

	for _, taskData := range taskSchema.List() {
		taskMap := taskData.(map[string]interface{})
		if mainIface, ok := taskMap["python_file"]; ok {
			pythonTask.PythonFile = mainIface.(string)
		}
		if paramSlice, ok := taskMap["parameters"]; ok {
			params := make([]string, len(paramSlice.([]interface{})))
			for i, val := range paramSlice.([]interface{}) {
				params[i] = val.(string)
			}
			pythonTask.Parameters = params
		}
	}

	return pythonTask
}

func getJobSparkSubmitTask(data *schema.ResourceData) *db.SparkSubmitTask {
	taskIface, ok := data.GetOk("spark_submit_task")
	if !ok {
		return nil
	}
	submitTask := &db.SparkSubmitTask{}

	taskSchema := taskIface.(*schema.Set)

	for _, taskData := range taskSchema.List() {
		taskMap := taskData.(map[string]interface{})
		if paramSlice, ok := taskMap["parameters"]; ok {
			params := make([]string, len(paramSlice.([]interface{})))
			for i, val := range paramSlice.([]interface{}) {
				params[i] = val.(string)
			}
			submitTask.Parameters = params
		}
	}

	return submitTask
}
