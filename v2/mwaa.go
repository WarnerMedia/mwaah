// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/mwaa"
)

type CLIENT struct {
	svc             mwaa.MWAA
	Name            *string
	host            *string
	tokenOutput     *mwaa.CreateCliTokenOutput
	tokenExpiration time.Time
	//version *string
}

type MWAAData struct {
	Stderr    []byte `json:"stderr"`
	Stdout    []byte `json:"stdout"`
	StderrStr string `json:"stdout_str"`
	StdoutStr string `json:"stderr_str"`
}

func DecodeMWAAData(resp http.Response) (MWAAData, error) {
	var data MWAAData
	err := json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return MWAAData{}, err
	}
	stderrDecoded := string(data.Stderr)
	if err != nil {
		return MWAAData{}, err
	}
	stdoutDecoded := string(data.Stdout)
	if err != nil {
		return MWAAData{}, err
	}
	data.StderrStr = strings.TrimSuffix(stderrDecoded, "\n")
	data.StdoutStr = strings.TrimSuffix(stdoutDecoded, "\n")
	if strings.Contains(data.StderrStr, "airflow.exceptions") {
		return data, errors.New(data.StderrStr)
	}
	return data, err
}

/*
NewCLI creates a new MWAA client

@param svc mwaa.MWAA - A mwaa client session, with appropriate Config/credentials.

@param name *string - The managed airflow instance name.

@param autoRefresh bool - automatically, continually refresh the cli token used to issue commands, if false will lazily refresh tokens with a command is sent.

@return *CLIENT
*/
func NewClient(svc mwaa.MWAA, name *string) *CLIENT {
	return &CLIENT{
		svc:  svc,
		Name: name,
	}
}

// post a cmd string to "airflow" entrypoint in mwaa and returns the response data
func PostMWAACommand(cli *CLIENT, cmd string) (MWAAData, error) {
	// https://airflow.apache.org/docs/apache-airflow/2.2.2/usage-cli.html
	// https://airflow.apache.org/docs/apache-airflow/2.2.2/cli-and-env-variables-ref.html
	// https://airflow.apache.org/docs/apache-airflow/2.2.2/cli-and-env-variables-ref.html#command-line-interface
	// now := time.Now()
	if time.Now().After(cli.tokenExpiration) {
		refreshToken(cli)
	}
	client := http.Client{
		Timeout: time.Second * 60,
	}
	body := strings.NewReader(cmd)

	req, err := http.NewRequest(http.MethodPost, `https://`+*cli.tokenOutput.WebServerHostname+`/aws_mwaa/cli`, body)
	if err != nil {
		return MWAAData{}, err
	}
	req.Header = http.Header{
		"Content-Type":  {"text/plain"},
		"Authorization": {"Bearer " + *cli.tokenOutput.CliToken},
	}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return MWAAData{}, err
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("%+v\n", resp)
		return MWAAData{}, errors.New("Status: " + resp.Status)
	}
	return DecodeMWAAData(*resp)
}

func (cli *CLIENT) createToken() *mwaa.CreateCliTokenOutput {
	tokenInput := &mwaa.CreateCliTokenInput{Name: aws.String(*cli.Name)}
	tokenOutput, err := cli.svc.CreateCliToken(tokenInput)
	if err != nil {
		panic(err)
	}
	return tokenOutput
}

// generate a new aws mwaa cli token
func refreshToken(cli *CLIENT) {
	cli.tokenOutput = cli.createToken()
	cli.tokenExpiration = time.Now().Add(1 * time.Minute)
}
