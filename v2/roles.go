// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"fmt"
)

type Roles []struct {
	Name string `json:"name"`
}

func UnmarshalRoles(data MWAAData) (Roles, error) {
	var roles Roles
	err := json.Unmarshal(data.Stdout, &roles)
	if err != nil {
		return Roles{}, err
	}
	return roles, nil
}

func (cli *CLIENT) GetRoles() (Roles, error) {
	cmd := fmt.Sprintf(`roles list --output json`)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return Roles{}, err
	}
	return UnmarshalRoles(data)
}
