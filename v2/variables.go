// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Variables []struct {
	Key string `json:"key"`
}

func UnmarshalGetVariables(data MWAAData) (Variables, error) {
	var variables Variables
	err := json.Unmarshal(data.Stdout, &variables)
	if err != nil {
		return Variables{}, err
	}
	return variables, nil
}

// set an airflow variable
func (cli *CLIENT) setVariable(key string, val string, serialize bool) error {
	// airflow variables set [-h] [-j] key VALUE
	cmd := `variables set`
	if serialize {
		cmd += ` --json`
	}
	cmd += fmt.Sprintf(` '%s' '%s'`, key, val)
	_, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return err
	}
	return nil
}

// get an airflow variable
func (cli *CLIENT) getVariable(key string, deserialize bool) (string, error) {
	// airflow variables get [-h] [-d VAL] [-j] [-v] key
	cmd := `variables get`
	if deserialize {
		cmd += ` --json`
	}
	cmd += fmt.Sprintf(` '%s'`, key)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return "", err
	}
	return data.StdoutStr, nil
}

// get airflow variable val for key in string form
func (cli *CLIENT) GetVariableNoSerialize(key string) (string, error) {
	val, err := cli.getVariable(key, false)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(val, "\n"), nil
}

// get airflow variable val for key, serialized to []byte with json.Marshal()
func (cli *CLIENT) GetVariableSerialize(key string) ([]byte, error) {
	val, err := cli.getVariable(key, true)
	if err != nil {
		return []byte{}, err
	}
	marshaledJSON, err := json.Marshal(val)
	if err != nil {
		return []byte{}, err
	}
	return marshaledJSON, nil
}

// set airflow "key" = "value"
func (cli *CLIENT) SetVariableNoSerialize(key string, val string) error {
	err := cli.setVariable(key, val, false)
	if err != nil {
		return err
	}
	return nil
}

// set airflow "key" = "value"
func (cli *CLIENT) SetVariableSerialize(key string, val string) error {
	err := cli.setVariable(key, val, true)
	if err != nil {
		return err
	}
	return nil
}

// delete an airflow variable
func (cli *CLIENT) DeleteVariable(key string) error {
	// airflow variables delete [-h] key
	cmd := fmt.Sprintf(`variables delete '%s'`, key)
	_, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return err
	}
	return nil
}

// get a list of airflow variables
func (cli *CLIENT) GetVariables() (Variables, error) {
	// airflow variables get [-h] [-d VAL] [-j] [-v] key
	cmd := `variables list --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return Variables{}, err
	}
	variables, err := UnmarshalGetVariables(data)
	if err != nil {
		return Variables{}, err
	}
	return variables, nil
}
