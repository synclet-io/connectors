package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// The types below are the Airbyte OSS registry format, restricted to the
// fields Synclet's registry fetcher reads.

type Registry struct {
	Sources      []Entry `json:"sources"`
	Destinations []Entry `json:"destinations"`
}

type Entry struct {
	Name                      string                `json:"name"`
	DockerRepository          string                `json:"dockerRepository"`
	DockerImageTag            string                `json:"dockerImageTag"`
	DocumentationURL          string                `json:"documentationUrl,omitempty"`
	ReleaseStage              string                `json:"releaseStage"`
	SupportLevel              string                `json:"supportLevel"`
	SourceType                string                `json:"sourceType,omitempty"`
	IconURL                   string                `json:"iconUrl,omitempty"`
	License                   string                `json:"license"`
	Tombstone                 bool                  `json:"tombstone"`
	Spec                      json.RawMessage       `json:"spec"`
	Releases                  *Releases             `json:"releases,omitempty"`
	ExternalDocumentationURLs []ExternalDoc         `json:"externalDocumentationUrls,omitempty"`
	ResourceRequirements      *ResourceRequirements `json:"resourceRequirements,omitempty"`
	MaxSecondsBetweenMessages *int                  `json:"maxSecondsBetweenMessages,omitempty"`
	SuggestedStreams          *StreamList           `json:"suggestedStreams,omitempty"`
	AllowedHosts              *HostList             `json:"allowedHosts,omitempty"`
	ErdURL                    string                `json:"erdUrl,omitempty"`
	ReleaseDate               string                `json:"releaseDate,omitempty"`
	Language                  string                `json:"language,omitempty"`
	Tags                      []string              `json:"tags,omitempty"`
	SupportsRefreshes         bool                  `json:"supportsRefreshes,omitempty"`
	SupportsFileTransfer      bool                  `json:"supportsFileTransfer,omitempty"`
	SupportsDataActivation    bool                  `json:"supportsDataActivation,omitempty"`
}

type Releases struct {
	BreakingChanges           map[string]RegistryBreakingChange `json:"breakingChanges,omitempty"`
	MigrationDocumentationURL string                            `json:"migrationDocumentationUrl,omitempty"`
}

type RegistryBreakingChange struct {
	Message                   string `json:"message"`
	MigrationDocumentationURL string `json:"migrationDocumentationUrl,omitempty"`
	UpgradeDeadline           string `json:"upgradeDeadline,omitempty"`
}

type ResourceRequirements struct {
	JobSpecific []JobSpecific `json:"jobSpecific"`
}

type JobSpecific struct {
	JobType              string        `json:"jobType"`
	ResourceRequirements ResourceValue `json:"resourceRequirements"`
}

type ResourceValue struct {
	MemoryRequest string `json:"memory_request,omitempty"`
	MemoryLimit   string `json:"memory_limit,omitempty"`
	CPURequest    string `json:"cpu_request,omitempty"`
	CPULimit      string `json:"cpu_limit,omitempty"`
}

type StreamList struct {
	Streams []string `json:"streams"`
}

type HostList struct {
	Hosts []string `json:"hosts"`
}

func buildRegistry(connectors []Connector, imagePrefix string) Registry {
	reg := Registry{Sources: []Entry{}, Destinations: []Entry{}}

	for _, c := range connectors {
		entry := toEntry(c, imagePrefix)
		if c.Meta.ConnectorType == "source" {
			reg.Sources = append(reg.Sources, entry)
		} else {
			reg.Destinations = append(reg.Destinations, entry)
		}
	}

	return reg
}

func toEntry(c Connector, imagePrefix string) Entry {
	m := c.Meta
	entry := Entry{
		Name:                      m.Name,
		DockerRepository:          strings.ToLower(strings.TrimSuffix(imagePrefix, "/") + "/" + c.Dir),
		DockerImageTag:            m.Version,
		DocumentationURL:          m.DocumentationURL,
		ReleaseStage:              m.ReleaseStage,
		SupportLevel:              m.SupportLevel,
		SourceType:                m.SourceType,
		IconURL:                   m.IconURL,
		License:                   m.License,
		Tombstone:                 m.Tombstone,
		Spec:                      c.Spec,
		ExternalDocumentationURLs: m.ExternalDocumentationURLs,
		MaxSecondsBetweenMessages: m.MaxSecondsBetweenMessages,
		ErdURL:                    m.ErdURL,
		ReleaseDate:               m.ReleaseDate,
		Language:                  m.Language,
		Tags:                      m.Tags,
		SupportsRefreshes:         m.SupportsRefreshes,
		SupportsFileTransfer:      m.SupportsFileTransfer,
		SupportsDataActivation:    m.SupportsDataActivation,
	}

	if len(m.BreakingChanges) > 0 || m.MigrationDocumentationURL != "" {
		entry.Releases = &Releases{MigrationDocumentationURL: m.MigrationDocumentationURL}
		if len(m.BreakingChanges) > 0 {
			entry.Releases.BreakingChanges = make(map[string]RegistryBreakingChange, len(m.BreakingChanges))
			for version, bc := range m.BreakingChanges {
				entry.Releases.BreakingChanges[version] = RegistryBreakingChange(bc)
			}
		}
	}

	if len(m.ResourceRequirements) > 0 {
		entry.ResourceRequirements = &ResourceRequirements{}
		for _, r := range m.ResourceRequirements {
			entry.ResourceRequirements.JobSpecific = append(entry.ResourceRequirements.JobSpecific, JobSpecific{
				JobType: r.JobType,
				ResourceRequirements: ResourceValue{
					MemoryRequest: r.MemoryRequest,
					MemoryLimit:   r.MemoryLimit,
					CPURequest:    r.CPURequest,
					CPULimit:      r.CPULimit,
				},
			})
		}
	}

	if len(m.SuggestedStreams) > 0 {
		entry.SuggestedStreams = &StreamList{Streams: m.SuggestedStreams}
	}

	if len(m.AllowedHosts) > 0 {
		entry.AllowedHosts = &HostList{Hosts: m.AllowedHosts}
	}

	return entry
}

// resolveImagePrefix defaults to ghcr.io/$GITHUB_REPOSITORY inside GitHub Actions.
func resolveImagePrefix(prefix string) (string, error) {
	if prefix != "" {
		return prefix, nil
	}

	repo := os.Getenv("GITHUB_REPOSITORY")
	if repo == "" {
		return "", fmt.Errorf("--image-prefix is required outside GitHub Actions")
	}

	return "ghcr.io/" + strings.ToLower(repo), nil
}
