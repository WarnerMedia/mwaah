// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
)

type ProvidersBehaviours []struct {
	FieldBehaviours string `json:"field_behaviours"`
}

type ProviderLinks []struct {
	ExtraLinkClassName string `json:"extra_link_class_name"`
}

type ProviderHooks []struct {
	ConnectionType      string `json:"connection_type"`
	Class               string `json:"class"`
	ConnIDAttributeName string `json:"conn_id_attribute_name"`
	PackageName         string `json:"package_name"`
	HookName            string `json:"hook_name"`
}

// returned from: `airflow providers get`
type ProviderDetailed []struct {
	PackageName            string   `json:"package-name"`
	Name                   string   `json:"name"`
	Description            string   `json:"description"`
	Versions               []string `json:"versions"`
	AdditionalDependencies []string `json:"additional-dependencies"`
	Integrations           []struct {
		IntegrationName string   `json:"integration-name"`
		ExternalDocURL  string   `json:"external-doc-url"`
		Logo            string   `json:"logo,omitempty"`
		HowToGuide      []string `json:"how-to-guide,omitempty"`
		Tags            []string `json:"tags"`
	} `json:"integrations"`
	Operators []struct {
		IntegrationName string   `json:"integration-name"`
		PythonModules   []string `json:"python-modules"`
	} `json:"operators"`
	Sensors []struct {
		IntegrationName string   `json:"integration-name"`
		PythonModules   []string `json:"python-modules"`
	} `json:"sensors"`
	Hooks []struct {
		IntegrationName string   `json:"integration-name"`
		PythonModules   []string `json:"python-modules"`
	} `json:"hooks"`
	Transfers []struct {
		SourceIntegrationName string `json:"source-integration-name"`
		TargetIntegrationName string `json:"target-integration-name"`
		PythonModule          string `json:"python-module"`
		HowToGuide            string `json:"how-to-guide,omitempty"`
	} `json:"transfers"`
	HookClassNames  []string `json:"hook-class-names"`
	ExtraLinks      []string `json:"extra-links"`
	ConnectionTypes []struct {
		HookClassName  string `json:"hook-class-name"`
		ConnectionType string `json:"connection-type"`
	} `json:"connection-types"`
	SecretsBackends []string `json:"secrets-backends"`
	Logging         []string `json:"logging"`
}

func UnmarshalGetProviders(data MWAAData) ([]airflow.Provider, error) {
	var providers []airflow.Provider
	err := json.Unmarshal(data.Stdout, &providers)
	if err != nil {
		return []airflow.Provider{}, err
	}
	return providers, nil
}

func UnmarshalProviderDetailed(data MWAAData) (ProviderDetailed, error) {
	var providers ProviderDetailed
	err := json.Unmarshal(data.Stdout, &providers)
	if err != nil {
		return ProviderDetailed{}, err
	}
	return providers, nil
}

func UnmarshalListProviderHooks(data MWAAData) (ProviderHooks, error) {
	var providerHooks ProviderHooks
	err := json.Unmarshal(data.Stdout, &providerHooks)
	if err != nil {
		return ProviderHooks{}, err
	}
	return providerHooks, nil
}

func UnmarshalGetProviderLinks(data MWAAData) (ProviderLinks, error) {
	var providerLinks ProviderLinks
	err := json.Unmarshal(data.Stdout, &providerLinks)
	if err != nil {
		return ProviderLinks{}, err
	}
	return providerLinks, nil
}

func UnmarshalProvidersBehaviours(data MWAAData) (ProvidersBehaviours, error) {
	var providerBehaviours ProvidersBehaviours
	err := json.Unmarshal(data.Stdout, &providerBehaviours)
	if err != nil {
		return ProvidersBehaviours{}, err
	}
	return providerBehaviours, nil
}

func (cli *CLIENT) GetProviderHooks() (ProviderHooks, error) {
	// airflow providers hooks [-h] [-o table, json, yaml, plain] [-v]
	cmd := `providers hooks --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return ProviderHooks{}, err
	}
	providerHooks, err := UnmarshalListProviderHooks(data)
	if err != nil {
		return ProviderHooks{}, err
	}
	return providerHooks, nil
}

// List extra links registered by the providers
// https://airflow.apache.org/docs/apache-airflow/2.2.2/cli-and-env-variables-ref.html#links
func (cli *CLIENT) GetProviderLinks() (ProviderLinks, error) {
	// airflow providers links [-h] [-o table, json, yaml, plain] [-v]
	cmd := `providers links --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return ProviderLinks{}, err
	}
	providerLinks, err := UnmarshalGetProviderLinks(data)
	if err != nil {
		return ProviderLinks{}, err
	}
	return providerLinks, nil
}

// Get list of providers
func (cli *CLIENT) GetProviders() ([]airflow.Provider, error) {
	// airflow providers list --output json
	cmd := `providers list --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return []airflow.Provider{}, err
	}
	providers, err := UnmarshalGetProviders(data)
	if err != nil {
		return []airflow.Provider{}, err
	}
	return providers, nil
}

// Get detailed information about a provider
func (cli *CLIENT) GetProviderDetailed(providerName string) (ProviderDetailed, error) {
	// airflow providers get --full --output json 'apache-airflow-providers-amazon'
	cmd := fmt.Sprintf(`providers get --full --output json '%s'`, providerName)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return ProviderDetailed{}, err
	}
	providers, err := UnmarshalProviderDetailed(data)
	if err != nil {
		return ProviderDetailed{}, err
	}
	return providers, nil
}

func (cli *CLIENT) GetProvidersBehaviours() (ProvidersBehaviours, error) {
	// airflow providers behaviours [-h] [-o table, json, yaml, plain] [-v]
	cmd := `providers behaviours --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return ProvidersBehaviours{}, err
	}
	return UnmarshalProvidersBehaviours(data)
}
