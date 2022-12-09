// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

// returns the semantic version of airflow cli
func (cli *CLIENT) GetVersion() (string, error) {
	cmd := `version`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return "", err
	}
	return data.StdoutStr, nil
}
