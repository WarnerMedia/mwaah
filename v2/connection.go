// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"errors"
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
)

type Connection struct {
	Description airflow.NullableString `json:"description"`
	airflow.Connection
}

// GetDescription returns the Description field value if set, zero value otherwise (both if not set or set to explicit null).
func (o *Connection) GetDescription() string {
	if o == nil || o.Description.Get() == nil {
		var ret string
		return ret
	}
	return *o.Description.Get()
}

// GetDescriptionOk returns a tuple with the Description field value if set, nil otherwise
// and a boolean to check if the value has been set.
// NOTE: If the value is an explicit nil, `nil, true` will be returned
func (o *Connection) GetDescriptionOk() (*string, bool) {
	if o == nil {
		return nil, false
	}
	return o.Description.Get(), o.Description.IsSet()
}

// HasDescription returns a boolean if a field has been set.
func (o *Connection) HasDescription() bool {
	if o != nil && o.Description.IsSet() {
		return true
	}
	return false
}

// SetDescription gets a reference to the given NullableString and assigns it to the Description field.
func (o *Connection) SetDescription(v string) {
	o.Description.Set(&v)
}

// SetDescriptionNil sets the value for Description to be an explicit nil
func (o *Connection) SetDescriptionNil() {
	o.Description.Set(nil)
}

// UnsetDecription ensures that no value is present for Schema, not even an explicit nil
func (o *Connection) UnsetDescription() {
	o.Description.Unset()
}

// adds an airflow connection
func (cli *CLIENT) AddConnection(conn Connection) error {
	// airflow connections add [-h]
	// [--conn-description CONN_DESCRIPTION]
	// [--conn-extra CONN_EXTRA] [--conn-host CONN_HOST]
	// [--conn-login CONN_LOGIN]
	// [--conn-password CONN_PASSWORD]
	// [--conn-port CONN_PORT] [--conn-schema CONN_SCHEMA]
	// [--conn-type CONN_TYPE] [--conn-uri CONN_URI]
	// conn_id
	connId := conn.GetConnectionId()
	if connId == "" {
		return errors.New("ConnectionId is empty, please provide ConnectionId")
	}
	cmd := "connections add"
	if conn.HasDescription() {
		cmd += fmt.Sprintf(` --conn-description '%s'`, conn.GetDescription())
	}
	if conn.HasExtra() {
		cmd += fmt.Sprintf(` --conn-extra '%s'`, conn.GetExtra())
	}
	if conn.HasLogin() {
		cmd += fmt.Sprintf(` --conn-login '%s'`, conn.GetLogin())
	}
	if conn.HasPassword() {
		cmd += fmt.Sprintf(` --conn-password '%s'`, conn.GetPassword())
	}
	if conn.HasPort() {
		cmd += fmt.Sprintf(` --conn-port '%s'`, string(conn.GetPort()))
	}
	if conn.HasConnType() {
		cmd += fmt.Sprintf(` --conn-type '%s'`, conn.GetConnType())
	}
	cmd += fmt.Sprintf(` '%s'`, connId)
	_, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return err
	}
	return nil
}
