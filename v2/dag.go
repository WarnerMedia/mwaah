// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/airflow-client-go/airflow"
)

// fields used in several Dag structs
type dagFields struct {
	DagId     string `json:"dag_id,omitempty"`
	State     string `json:"state,omitempty"`
	StartDate string `json:"start_date,omitempty"`
	EndDate   string `json:"end_date,omitempty"`
}

// returned from: `airflow dags list`
type Dags []struct {
	DagId    string `json:"dag_id,omitempty"`
	FilePath string `json:"filepath,omitempty"`
	Owner    string `json:"owner,omitempty"`
	Paused   string `json:"paused,omitempty"`
}

// returned from: `airflow dags list-runs`
type DagRun struct {
	dagFields
	// DagId          string `json:"dag_id,omitempty"`
	DagRunId      string `json:"run_id,omitempty"`
	ExecutionDate string `json:"execution_date,omitempty"`
	// StartDate      string `json:"start_date,omitempty"`
	// EndDate        string `json:"end_date,omitempty"`
	// State          string `json:"state,omitempty"`
	DagRunIdOutput string `json:"dag_run_id,omitempty"`
	// ExternalTrigger bool   `json:"external_trigger,omitempty"`
}

func (d *DagRun) MarshalJSON() ([]byte, error) {
	d.DagRunIdOutput = d.DagRunId
	return json.Marshal(*d)
}

// returned from: `airflow dags list-jobs`
type DagJobs []struct {
	JobType string `json:"job_type,omitempty"`
	dagFields
}

// optional args == pain
type DagJobsInput struct {
	DagId airflow.NullableString `json:"dag_id,omitempty"`
	State airflow.DagState       `json:"state,omitempty"`
	Limit airflow.NullableInt    `json:"limit,omitempty"`
}

// returned from: `airflow dags report`
type DagReport []struct {
	File     string   `json:"file,omitempty"`
	Duration string   `json:"duration,omitempty"`
	DagNum   string   `json:"dag_num,omitempty"`
	TaskNum  string   `json:"task_num,omitempty"`
	Dags     []string `json:"dags,omitempty"`
}

// string version of airflow.DAGRun
type DAGRunString []struct {
	DagFields dagFields
	DagRunId  string `json:"dag_run_id,omitempty"`
	// DagId                  string `json:"dag_id,omitempty"`
	LogicalDate   string `json:"logical_date,omitempty"`
	ExecutionDate string `json:"execution_date,omitempty"`
	// StartDate              string `json:"start_date,omitempty"`
	// EndDate                string `json:"end_date,omitempty"`
	DataIntervalStart      string `json:"data_interval_start,omitempty"`
	DataIntervalEnd        string `json:"data_interval_end,omitempty"`
	LastSchedulingDecision string `json:"last_scheduling_decision,omitempty"`
	RunType                string `json:"run_type,omitempty"`
	// State                  string `json:"state,omitempty"`
	ExternalTrigger string `json:"external_trigger,omitempty"`
	Conf            string `json:"conf,omitempty"`
}

func ParseNewDagRun(data MWAAData) (airflow.DAGRun, error) {
	r := regexp.MustCompile(`(?m)(?:.+\nCreated <DagRun )([a-zA-Z0-9\-\_.]+) @ ([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}): (.+), externally triggered: (True|False)>`)
	matches := r.FindStringSubmatch(data.StdoutStr)
	expectedNumMatches := 5
	if len(matches) < expectedNumMatches || matches == nil {
		return airflow.DAGRun{}, errors.New("unable to parse stdout\nStderr:\n" + data.StdoutStr)
	}
	airflowDagRun := airflow.NewDAGRun()
	externalTrigger, err := strconv.ParseBool(matches[4])
	if err != nil {
		return airflow.DAGRun{}, err
	}
	airflowDagRun.SetDagId(matches[1])
	executionDate, err := GetTimePythonISONoDecimal(matches[2])
	if err != nil {
		return airflow.DAGRun{}, err
	}
	airflowDagRun.SetExecutionDate(executionDate)
	airflowDagRun.SetDagRunId(matches[3])
	airflowDagRun.SetExternalTrigger(externalTrigger)
	return *airflowDagRun, nil
}

func UnmarshalDagRuns(data MWAAData) ([]airflow.DAGRun, error) {
	var dags []DagRun
	err := json.Unmarshal(data.Stdout, &dags)
	if err != nil {
		return []airflow.DAGRun{}, err
	}
	var AirflowDagRuns []airflow.DAGRun
	bytes, err := json.Marshal(dags)
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(bytes, &AirflowDagRuns)
	if err != nil {
		return []airflow.DAGRun{}, err
	}
	return AirflowDagRuns, nil
}

func UnmarshalDagDiGraph(data MWAAData) (string, error) {
	var diGraph string
	err := json.Unmarshal(data.Stdout, &diGraph)
	if err != nil {
		return "", err
	}
	return diGraph, nil
}

func UnmarshalGetDags(data MWAAData) (Dags, error) {
	var dags Dags
	err := json.Unmarshal(data.Stdout, &dags)
	if err != nil {
		return Dags{}, err
	}
	return dags, nil
}

func UnmarshalDagsReport(data MWAAData) (DagReport, error) {
	var dagReport DagReport
	err := json.Unmarshal(data.Stdout, &dagReport)
	if err != nil {
		return DagReport{}, err
	}
	return dagReport, nil
}

func UnmarshalGetDagJobs(data MWAAData) (DagJobs, error) {
	var dagReport DagJobs
	err := json.Unmarshal(data.Stdout, &dagReport)
	if err != nil {
		return DagJobs{}, err
	}
	return dagReport, nil
}

// deletes an airflow connection
func (cli *CLIENT) DeleteConnection(connectionId string) error {
	// airflow connections delete [-h] [--color {auto,off,on}] conn_id
	cmd := fmt.Sprintf(`connections delete '%s'`, connectionId)
	_, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return err
	}
	return nil
}

// returns all dag runs
func (cli *CLIENT) GetAllDagRuns() ([]airflow.DAGRun, error) {
	dagRun := airflow.NewDAGRun()
	return cli.GetDagRuns(*dagRun)
}

// returns dagruns that match dagRun
func (cli *CLIENT) GetDagRuns(dagRun airflow.DAGRun) ([]airflow.DAGRun, error) {
	cmd := `dags list-runs`
	// Override start_date in format YYYY-MM-DD
	if dagRun.HasStartDate() {
		cmd += fmt.Sprintf(` --start-date '%s'`, dagRun.GetLogicalDate().Format(time.Layout))
	}
	cmd += ` --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return []airflow.DAGRun{}, err
	}
	dagRuns, err := UnmarshalDagRuns(data)
	if err != nil {
		return []airflow.DAGRun{}, err
	}
	if dagRun.HasDagId() {
		foundDagRun, found := GetDagByRunId(dagRuns, dagRun.GetDagRunId())
		if !found {
			return []airflow.DAGRun{}, errors.New("found no dag with runId: " + dagRun.GetDagRunId())
		} else {
			return []airflow.DAGRun{foundDagRun}, nil
		}
	}
	return dagRuns, nil
}

// PERMANTENTLY Delete all DB records related to the specified DAG
// USE WITH CARE!
func (cli *CLIENT) DeleteDag(dagId string) error {
	// airflow dags delete [-h] [-y] dag_id
	cmd := fmt.Sprintf(`dags delete --yes '%s'`, dagId)
	_, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return err
	}
	return nil
}

// Trigger a new DAG run, return representation of the new dagrun
func (cli *CLIENT) NewDagRun(dagRun airflow.DAGRun) (*airflow.DAGRun, error) {
	// airflow dags trigger [-h] [-c CONF] [-e EXEC_DATE] [-r RUN_ID] [-S SUBDIR] dag_id
	cmd := `dags trigger`
	if dagRun.GetDagId() == "" {
		return &airflow.DAGRun{}, errors.New("DagRun.DagId is empty, please provide a DagId")
	}
	if dagRun.HasConf() {
		jsonStr, err := json.Marshal(dagRun.GetConf())
		if err != nil {
			return &airflow.DAGRun{}, errors.New("error marshaling dagRun.Conf")
		}
		cmd += fmt.Sprintf(` --conf '%s'`, jsonStr)
	}
	if dagRun.HasExecutionDate() || dagRun.HasLogicalDate() {
		var date string
		if dagRun.HasExecutionDate() {
			date = dagRun.GetExecutionDate().Format(PythonISONoDecimalTimeLayout)
		} else {
			date = dagRun.GetLogicalDate().Format(PythonISONoDecimalTimeLayout)
		}
		cmd += fmt.Sprintf(` --exec-date '%s'`, date)
	}
	if dagRun.HasDagRunId() {
		cmd += fmt.Sprintf(` --run-id '%s'`, dagRun.GetDagRunId())
	}
	cmd += fmt.Sprintf(` '%s'`, dagRun.GetDagId())
	// airflow dags trigger does not use --output flag
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return &airflow.DAGRun{}, err
	}
	newDagRun, err := ParseNewDagRun(data)
	newDagRun.SetConf(*dagRun.Conf)
	if err != nil {
		return &airflow.DAGRun{}, err
	}
	return &newDagRun, err
}

// Returns all DAGs
func (cli *CLIENT) GetDags() (Dags, error) {
	cmd := `dags list --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		panic(err)
	}
	dags, err := UnmarshalGetDags(data)
	if err != nil {
		panic(err)
	}
	return dags, err
}

// pause a DAG
func (cli *CLIENT) PauseDag(dagId string) error {
	// airflow dags pause [-h] [-S SUBDIR] dag_id
	cmd := `dags pause`
	cmd += fmt.Sprintf(` '%s'`, dagId)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return err
	}
	return nil
}

// unpause a DAG
func (cli *CLIENT) UnpauseDag(dagId string) error {
	// airflow dags unpause [-h] [-S SUBDIR] dag_id
	cmd := `dags unpause`
	cmd += fmt.Sprintf(` '%s'`, dagId)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return err
	}
	return nil
}

// returns dagbag stats
func (cli *CLIENT) DagsReport() (DagReport, error) {
	// airflow dags report [-h] [-o table, json, yaml, plain] [-S SUBDIR] [-v]
	cmd := `dags report --output json`
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return DagReport{}, err
	}
	dagsReport, err := UnmarshalDagsReport(data)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return DagReport{}, err
	}
	return dagsReport, nil
}

// returns DiGraph represenation of DAG
func (cli *CLIENT) DagShow(dagId string) (string, error) {
	// airflow dags show [-h] [--imgcat] [-s SAVE] [-S SUBDIR] dag_id
	cmd := `dags show`
	cmd += fmt.Sprintf(` '%s'`, dagId)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return "", err
	}
	dagsReport, err := UnmarshalDagDiGraph(data)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return "", err
	}
	return dagsReport, nil
}

// func parseDagState(data MWAAData) (airflow.DagState, error) {

// 	stateStr := strings.Split(data.StdoutStr, ",")[0]
// 	if err != nil {
// 		fmt.Printf("%+v\n", data)
// 		return airflow.DagState(""), err
// 	}
// 	if strings.Contains(data.StderrStr, dagIdNotFoundException) {
// 		return airflow.DagState(""), errors.New(dagIdNotFoundException)
// 	}
// 	if strings.HasSuffix(data.StdoutStr, "None") {
// 		return airflow.DagState(""), errors.New("no dag found with executionDate: " + executionDateFormatted)
// 	}
// 	notExists := fmt.Sprintf("%s does not exist", dagId)
// 	if strings.Contains(data.StdoutStr, notExists) {
// 		return airflow.DagState(""), errors.New(data.StdoutStr)
// 	}
// 	state, err := airflow.NewDagStateFromValue(stateStr)
// 	if err != nil {
// 		return airflow.DagState(""), errors.New(data.StdoutStr)
// 	}
// 	return state, nil
// }

// return state of a DagRun with dagId and exact executionDate
func (cli *CLIENT) GetDagState(dagId string, executionDate time.Time) (*airflow.DagState, error) {
	ev := airflow.DagState("")
	// airflow dags state 'example_bash_operator' '2022-11-06T00:00:00+00:00'
	// returns one or none so it needs to be an exact time.Time in python iso no-decimal
	executionDateFormatted := executionDate.Format(PythonISONoDecimalTimeLayout)
	dagIdNotFoundException := fmt.Sprintf(`airflow.exceptions.AirflowException: Dag '%s' could not be found`, dagId)
	cmd := fmt.Sprintf(`dags state '%s' '%s'`, dagId, executionDateFormatted)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("%+v\n", data)
		return &ev, err
	}
	if strings.Contains(data.StderrStr, dagIdNotFoundException) {
		return &ev, errors.New(dagIdNotFoundException)
	}
	state := strings.Split(strings.Split(data.StdoutStr, "\n")[1], ",")[0]
	// if strings.HasSuffix(data.StdoutStr, "None") {
	// 	return airflow.DagState(""), errors.New("no dag found with executionDate: " + executionDateFormatted)
	// }
	if state == "None" {
		return &ev, errors.New("no dag found with executionDate: " + executionDateFormatted)
	}
	notExists := fmt.Sprintf("%s does not exist", dagId)
	if strings.Contains(data.StdoutStr, notExists) {
		return &ev, errors.New(data.StdoutStr)
	}
	return airflow.NewDagStateFromValue(state)
}

// "Lists latest n jobs"
// optionally filtered by dagId, dagState
// no limit will return all
func (cli *CLIENT) GetDagJobs(i DagJobsInput) (DagJobs, error) {
	// airflow dags list-jobs [-h] [-d DAG_ID] [--limit LIMIT] [-o table, json, yaml, plain] [--state STATE] [-v]
	var dagIdNotFoundException string
	cmd := `dags list-jobs`
	if i.DagId.IsSet() {
		cmd += fmt.Sprintf(` --dag-id '%s'`, *i.DagId.Get())
	}
	if i.Limit.IsSet() {
		limit := *i.Limit.Get()
		if limit > 0 {
			cmd += fmt.Sprintf(` --limit '%s'`, strconv.Itoa(limit))
		} else {
			return DagJobs{}, nil
		}
	}
	cmd += ` --output json`
	if !i.State.IsValid() {
		return DagJobs{}, errors.New(fmt.Sprintf(`'%s' is not a valid DagState`, string(i.State)))
	} else {
		cmd += fmt.Sprintf(` --state '%s'`, i.State)
	}
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("%+v\n", data)
		return DagJobs{}, err
	}
	if i.DagId.IsSet() {
		dagIdNotFoundException = fmt.Sprintf(`airflow.exceptions.AirflowException: Dag id %s not found`, *i.DagId.Get())
		if strings.Contains(data.StderrStr, dagIdNotFoundException) {
			return DagJobs{}, errors.New(dagIdNotFoundException)
		}
	}
	return UnmarshalGetDagJobs(data)
}

// execute one single DagRun for a given DAG and execution date, using the DebugExecutor.
// func (cli *CLIENT) DagTest(dagId string, executionDate time.Time) (MWAAData, error) {
// 	// airflow dags test [-h] [--imgcat-dagrun] [--save-dagrun SAVE_DAGRUN]
// 	// [--show-dagrun] [-S SUBDIR]
// 	// dag_id execution_date
// 	cmd := fmt.Sprintf(`dags test '%s' '%s'`, dagId, executionDate.Format(PythonISONoDecimalTimeLayout))
// 	return PostMWAACommand(cli, cmd)
// }

// find a dagRun by dagId
func GetDagByRunId(dags []airflow.DAGRun, runId string) (airflow.DAGRun, bool) {
	for _, dag := range dags {
		if dag.GetDagRunId() == runId {
			return dag, true
		}
	}
	return airflow.DAGRun{}, false
}
