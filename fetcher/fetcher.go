package fetcher

import (
	"sync"
	"time"

	"code.cloudfoundry.org/cli/api/cloudcontroller/ccv3"
	"github.com/cloudfoundry/cf_exporter/filters"
	"github.com/cloudfoundry/cf_exporter/models"
	log "github.com/sirupsen/logrus"
)

var (
	LargeQuery = ccv3.Query{
		Key:    ccv3.PerPage,
		Values: []string{"5000"},
	}
	SortDesc = ccv3.Query{
		Key:    ccv3.OrderBy,
		Values: []string{"-created_at"},
	}
	TaskActiveStates = ccv3.Query{
		Key:    ccv3.StatesFilter,
		Values: []string{"PENDING", "RUNNING", "CANCELING"},
	}
)

type CFConfig struct {
	SkipSSLValidation bool   `yaml:"skip_ssl_validation"`
	URL               string `yaml:"url"`
	ClientID          string `yaml:"client_id"`
	ClientSecret      string `yaml:"client_secret"`
	Username          string `yaml:"username"`
	Password          string `yaml:"password"`
}

type Fetcher struct {
	sync.Mutex
	config *CFConfig
	worker *Worker
}

func NewFetcher(threads int, config *CFConfig, filter *filters.Filter) *Fetcher {
	return &Fetcher{
		config: config,
		worker: NewWorker(threads, filter),
	}
}

func (c *Fetcher) GetObjects() *models.CFObjects {
	log.Infof("collecting objects from cloud foundry API")
	start := time.Now()
	data := c.fetch()
	took := time.Since(start).Seconds()
	log.Infof("collecting objects from cloud foundry API (done, %.0f sec)", took)
	data.Took = took
	return data
}

func (c *Fetcher) workInit() {
	c.worker.Reset()
	c.worker.Push("info", c.fetchInfo)
	
	// Always fetch organizations and spaces if metadata is enabled
	if c.worker.filter.Enabled(filters.Metadata) {
		c.worker.Push("organizations", c.fetchOrgs)
		c.worker.Push("spaces", c.fetchSpaces)
		c.worker.Push("applications", c.fetchApplications)
	} else {
		c.worker.PushIf("organizations", c.fetchOrgs, filters.Applications, filters.Organizations)
		c.worker.PushIf("spaces", c.fetchSpaces, filters.Applications, filters.Spaces)
		c.worker.PushIf("applications", c.fetchApplications, filters.Applications)
	}

	c.worker.PushIf("org_quotas", c.fetchOrgQuotas, filters.Organizations)
	c.worker.PushIf("space_quotas", c.fetchSpaceQuotas, filters.Spaces)
	c.worker.PushIf("domains", c.fetchDomains, filters.Domains)
	c.worker.PushIf("process", c.fetchProcesses, filters.Applications)
	c.worker.PushIf("routes", c.fetchRoutes, filters.Routes)
	c.worker.PushIf("route_services", c.fetchRouteServices, filters.Routes)
	c.worker.PushIf("security_groups", c.fetchSecurityGroups, filters.SecurityGroups)
	c.worker.PushIf("stacks", c.fetchStacks, filters.Stacks)
	c.worker.PushIf("buildpacks", c.fetchBuildpacks, filters.Buildpacks)
	c.worker.PushIf("tasks", c.fetchTasks, filters.Tasks)
	c.worker.PushIf("service_brokers", c.fetchServiceBrokers, filters.Services)
	c.worker.PushIf("service_offerings", c.fetchServiceOfferings, filters.Services)
	c.worker.PushIf("service_instances", c.fetchServiceInstances, filters.ServiceInstances)
	c.worker.PushIf("service_plans", c.fetchServicePlans, filters.ServicePlans)
	c.worker.PushIf("segments", c.fetchIsolationSegments, filters.IsolationSegments)
	c.worker.PushIf("service_bindings", c.fetchServiceBindings, filters.ServiceBindings)
	c.worker.PushIf("service_route_bindings", c.fetchServiceRouteBindings, filters.ServiceRouteBindings)
	c.worker.PushIf("users", c.fetchUsers, filters.Events)
	c.worker.PushIf("events", c.fetchEvents, filters.Events)
}

func (c *Fetcher) fetch() *models.CFObjects {
	result := models.NewCFObjects()

	session, err := NewSessionExt(c.config)
	if err != nil {
		log.WithError(err).Error("unable to initialize cloud foundry clients")
		result.Error = err
		return result
	}

	c.workInit()

	result.Error = c.worker.Do(session, result)
	return result
}

func (c *Fetcher) fetchOrgMetadata(session *SessionExt, result *models.CFObjects) error {
	for _, org := range result.Orgs {
		if org.Metadata != nil {
			// Update the organization's metadata in the result
			result.Orgs[org.GUID] = org
		}
	}
	return nil
}

func (c *Fetcher) fetchSpaceMetadata(session *SessionExt, result *models.CFObjects) error {
	for _, space := range result.Spaces {
		if space.Metadata != nil {
			// Update the space's metadata in the result
			result.Spaces[space.GUID] = space
		}
	}
	return nil
}

func (c *Fetcher) fetchAppMetadata(session *SessionExt, result *models.CFObjects) error {
	for _, app := range result.Apps {
		if app.Metadata != nil {
			// Update the application's metadata in the result
			result.Apps[app.GUID] = app
		}
	}
	return nil
}
