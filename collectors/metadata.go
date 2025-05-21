package collectors

import (
	"fmt"
	"time"

	"github.com/cloudfoundry/cf_exporter/models"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type MetadataCollector struct {
	namespace                              string
	environment                            string
	deployment                             string
	organizationMetadataMetric             *prometheus.GaugeVec
	spaceMetadataMetric                    *prometheus.GaugeVec
	applicationMetadataMetric              *prometheus.GaugeVec
	metadataScrapesTotalMetric             prometheus.Counter
	metadataScrapeErrorsTotalMetric        prometheus.Counter
	lastMetadataScrapeErrorMetric          prometheus.Gauge
	lastMetadataScrapeTimestampMetric      prometheus.Gauge
	lastMetadataScrapeDurationSecondsMetric prometheus.Gauge
}

func NewMetadataCollector(
	namespace string,
	environment string,
	deployment string,
) *MetadataCollector {
	organizationMetadataMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "organization",
			Name:        "metadata",
			Help:        "Cloud Foundry Organization metadata labels with a constant '1' value.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
		[]string{"organization_id", "organization_name", "label_key", "label_value"},
	)

	spaceMetadataMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "space",
			Name:        "metadata",
			Help:        "Cloud Foundry Space metadata labels with a constant '1' value.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
		[]string{"space_id", "space_name", "organization_id", "organization_name", "label_key", "label_value"},
	)

	applicationMetadataMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "application",
			Name:        "metadata",
			Help:        "Cloud Foundry Application metadata labels with a constant '1' value.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
		[]string{"application_id", "application_name", "organization_id", "organization_name", "space_id", "space_name", "label_key", "label_value"},
	)

	metadataScrapesTotalMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "metadata_scrapes",
			Name:        "total",
			Help:        "Total number of scrapes for Cloud Foundry Metadata.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
	)

	metadataScrapeErrorsTotalMetric := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "metadata_scrape_errors",
			Name:        "total",
			Help:        "Total number of scrape errors of Cloud Foundry Metadata.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
	)

	lastMetadataScrapeErrorMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "",
			Name:        "last_metadata_scrape_error",
			Help:        "Whether the last scrape of Metadata metrics from Cloud Foundry resulted in an error (1 for error, 0 for success).",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
	)

	lastMetadataScrapeTimestampMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "",
			Name:        "last_metadata_scrape_timestamp",
			Help:        "Number of seconds since 1970 since last scrape of Metadata metrics from Cloud Foundry.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
	)

	lastMetadataScrapeDurationSecondsMetric := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "",
			Name:        "last_metadata_scrape_duration_seconds",
			Help:        "Duration of the last scrape of Metadata metrics from Cloud Foundry.",
			ConstLabels: prometheus.Labels{"environment": environment, "deployment": deployment},
		},
	)

	return &MetadataCollector{
		namespace:                              namespace,
		environment:                            environment,
		deployment:                             deployment,
		organizationMetadataMetric:             organizationMetadataMetric,
		spaceMetadataMetric:                    spaceMetadataMetric,
		applicationMetadataMetric:              applicationMetadataMetric,
		metadataScrapesTotalMetric:             metadataScrapesTotalMetric,
		metadataScrapeErrorsTotalMetric:        metadataScrapeErrorsTotalMetric,
		lastMetadataScrapeErrorMetric:          lastMetadataScrapeErrorMetric,
		lastMetadataScrapeTimestampMetric:      lastMetadataScrapeTimestampMetric,
		lastMetadataScrapeDurationSecondsMetric: lastMetadataScrapeDurationSecondsMetric,
	}
}

func (c MetadataCollector) Collect(objs *models.CFObjects, ch chan<- prometheus.Metric) {
	errorMetric := float64(0)
	if objs.Error != nil {
		errorMetric = float64(1)
		c.metadataScrapeErrorsTotalMetric.Inc()
	} else {
		err := c.reportMetadataMetrics(objs, ch)
		if err != nil {
			errorMetric = float64(1)
			c.metadataScrapeErrorsTotalMetric.Inc()
		}
	}

	c.metadataScrapeErrorsTotalMetric.Collect(ch)
	c.metadataScrapesTotalMetric.Inc()
	c.metadataScrapesTotalMetric.Collect(ch)
	c.lastMetadataScrapeErrorMetric.Set(errorMetric)
	c.lastMetadataScrapeErrorMetric.Collect(ch)
	c.lastMetadataScrapeTimestampMetric.Set(float64(time.Now().Unix()))
	c.lastMetadataScrapeTimestampMetric.Collect(ch)
	c.lastMetadataScrapeDurationSecondsMetric.Set(objs.Took)
	c.lastMetadataScrapeDurationSecondsMetric.Collect(ch)
}

func (c MetadataCollector) Describe(ch chan<- *prometheus.Desc) {
	c.organizationMetadataMetric.Describe(ch)
	c.spaceMetadataMetric.Describe(ch)
	c.applicationMetadataMetric.Describe(ch)
	c.metadataScrapesTotalMetric.Describe(ch)
	c.metadataScrapeErrorsTotalMetric.Describe(ch)
	c.lastMetadataScrapeErrorMetric.Describe(ch)
	c.lastMetadataScrapeTimestampMetric.Describe(ch)
	c.lastMetadataScrapeDurationSecondsMetric.Describe(ch)
}

func (c MetadataCollector) reportMetadataMetrics(objs *models.CFObjects, ch chan<- prometheus.Metric) error {
	// Report organization metadata
	for _, org := range objs.Organizations {
		for key, value := range org.Metadata.Labels {
			c.organizationMetadataMetric.With(prometheus.Labels{
				"organization_id":   org.GUID,
				"organization_name": org.Name,
				"label_key":         key,
				"label_value":       value,
			}).Set(1)
		}
	}

	// Report space metadata
	for _, space := range objs.Spaces {
		orgGUID, ok := space.Relationships["organization"].Data.GUID
		if !ok {
			log.Warnf("Space %s has no organization relationship", space.GUID)
			continue
		}
		org, ok := objs.OrganizationsMap[orgGUID]
		if !ok {
			log.Warnf("Organization %s not found for space %s", orgGUID, space.GUID)
			continue
		}
		for key, value := range space.Metadata.Labels {
			c.spaceMetadataMetric.With(prometheus.Labels{
				"space_id":           space.GUID,
				"space_name":         space.Name,
				"organization_id":    org.GUID,
				"organization_name":  org.Name,
				"label_key":          key,
				"label_value":        value,
			}).Set(1)
		}
	}

	// Report application metadata with org and space metadata
	for _, app := range objs.Applications {
		spaceGUID, ok := app.Relationships["space"].Data.GUID
		if !ok {
			log.Warnf("Application %s has no space relationship", app.GUID)
			continue
		}
		space, ok := objs.SpacesMap[spaceGUID]
		if !ok {
			log.Warnf("Space %s not found for application %s", spaceGUID, app.GUID)
			continue
		}
		orgGUID, ok := space.Relationships["organization"].Data.GUID
		if !ok {
			log.Warnf("Space %s has no organization relationship", space.GUID)
			continue
		}
		org, ok := objs.OrganizationsMap[orgGUID]
		if !ok {
			log.Warnf("Organization %s not found for space %s", orgGUID, space.GUID)
			continue
		}

		// Report application's own metadata
		for key, value := range app.Metadata.Labels {
			c.applicationMetadataMetric.With(prometheus.Labels{
				"application_id":     app.GUID,
				"application_name":   app.Name,
				"organization_id":    org.GUID,
				"organization_name":  org.Name,
				"space_id":          space.GUID,
				"space_name":        space.Name,
				"label_key":         key,
				"label_value":       value,
			}).Set(1)
		}

		// Report organization's metadata for this application
		for key, value := range org.Metadata.Labels {
			c.applicationMetadataMetric.With(prometheus.Labels{
				"application_id":     app.GUID,
				"application_name":   app.Name,
				"organization_id":    org.GUID,
				"organization_name":  org.Name,
				"space_id":          space.GUID,
				"space_name":        space.Name,
				"label_key":         "org_" + key,
				"label_value":       value,
			}).Set(1)
		}

		// Report space's metadata for this application
		for key, value := range space.Metadata.Labels {
			c.applicationMetadataMetric.With(prometheus.Labels{
				"application_id":     app.GUID,
				"application_name":   app.Name,
				"organization_id":    org.GUID,
				"organization_name":  org.Name,
				"space_id":          space.GUID,
				"space_name":        space.Name,
				"label_key":         "space_" + key,
				"label_value":       value,
			}).Set(1)
		}
	}

	c.organizationMetadataMetric.Collect(ch)
	c.spaceMetadataMetric.Collect(ch)
	c.applicationMetadataMetric.Collect(ch)

	return nil
} 