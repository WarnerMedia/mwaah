// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/apache/airflow-client-go/airflow"
)

type ClearTasks struct {
	CLI               *CLIENT
	DagId             *string                `json:"dag_id"`
	TaskRegexp        airflow.NullableString `json:"task_regexp"`
	UseDagIdRegex     airflow.NullableBool   `json:"use_dag_id_as_regex,omitempty"`
	IncludeDownstream airflow.NullableBool   `json:"include_downstream,omitempty"`
	IncludeUpstream   airflow.NullableBool   `json:"include_upstream,omitempty"`
	airflow.ClearTaskInstance
}

type DagTask struct {
	TaskId   *string                `json:"task_id"`
	Operator airflow.NullableString `json:"operator,omitempty"`
}

type Task struct {
	MatchString *string `json:"match_string"`
	DagId       *string `json:"dag_id"`
	TaskId      *string `json:"task_id"`
	DagRunId    *string `json:"dag_run_id"`
	State       *string `json:"state"`
}

type TaskStatesDetailed struct {
	dagFields
	TaskId *string `json:"task_id"`
}

func UnmarshalTasks(data MWAAData) ([]Task, error) {
	regex := `(?:<TaskInstance: )([a-zA-Z0-9\-\_]+).([a-zA-Z0-9\-\_]+) (.+) (?:\[)(failed|success|skipped)(?:]>)`
	r := regexp.MustCompile(regex)
	matches := r.FindAllStringSubmatch(data.StdoutStr, -1)
	if len(matches[0]) < reflect.TypeOf(Task{}).NumField() {
		return []Task{}, errors.New("unable to parse response using listTasksRegex")
	}
	var tasks []Task
	for i := 0; i < len(matches); i++ {
		task := Task{}
		populate(&task, matches[i])
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func UnmarshalDagTasks(data MWAAData) ([]DagTask, error) {
	var tasks []DagTask
	splitLines := strings.Split(data.StdoutStr, "\n")
	for _, line := range splitLines {
		replacer := strings.NewReplacer("<", "", ">", "", "(", "", ")", "")
		vals := strings.Split(replacer.Replace(line), ": ")
		operator := airflow.NewNullableString(&vals[0])
		taskId := vals[1]
		task := DagTask{
			Operator: *operator,
			TaskId:   &taskId,
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func UnmarshalTaskStatesDetailed(data MWAAData) ([]TaskStatesDetailed, error) {
	var tasks []TaskStatesDetailed
	err := json.Unmarshal(data.Stdout, &tasks)
	if err != nil {
		return []TaskStatesDetailed{}, err
	}
	return tasks, nil
}

// get the tasks as constrained by ClearTasks obj
func (o *ClearTasks) GetTasks() ([]Task, error) {
	// dryRun = true
	data, err := o.clearTasks(true)
	if err != nil {
		return []Task{}, err
	}
	return UnmarshalTasks(data)
}

// clear the tasks as constrained by ClearTasks obj
func (o *ClearTasks) Clear() error {
	// dryRun = false
	_, err := o.clearTasks(false)
	return err
}

// Returns task instances that meet the constraints of current ClearTasks
// useDagIdAsRegex=true will treat DagId as a regex constraint for target tasks
// https://airflow.apache.org/docs/apache-airflow/2.2.2/cli-and-env-variables-ref.html#clear
func (o *ClearTasks) clearTasks(dryRun bool) (MWAAData, error) {
	if o.HasTaskIds() {
		return MWAAData{}, errors.New("Cannot use TaskIds as constraints for Clearing Tasks with the CLI")
	}
	// airflow tasks clear [-h] [-R] [-d] [-e END_DATE] [-X] [-x] [-f] [-r]
	// [-s START_DATE] [-S SUBDIR] [-t TASK_REGEX] [-u] [-y]
	// dag_id
	cmd := `tasks clear`

	// -R, --dag-regex
	// Search dag_id as regex instead of exact string
	// Default: False
	if *o.UseDagIdRegex.Get() {
		cmd += ` --dag-regex`
	}

	// -d, --downstream
	// Include downstream tasks
	// Default: False
	if *o.IncludeDownstream.Get() {
		cmd += ` --downstream`
	}

	if o.HasEndDate() {
		endDateString := o.GetEndDate()
		// attempt to parse the date to confirm it's YYYY-MM-DD
		_, err := time.Parse("2006-01-02", endDateString)
		if err != nil {
			return MWAAData{}, err
		}
		cmd += fmt.Sprintf(` --end-date '%s'`, endDateString)
	}

	// -e, --end-date
	// Override end_date YYYY-MM-DD
	if !o.GetIncludeParentdag() {
		cmd += ` --exclude-parentdag`
	}

	// -X, --exclude-parentdag
	// Exclude ParentDAGS if the task cleared is a part of a SubDAG
	// Default: False
	if !o.GetIncludeSubdags() {
		cmd += ` --exclude-subdags`
	}

	// -x, --exclude-subdags
	// Exclude subdags
	// Default: False
	if !o.GetIncludeSubdags() {
		cmd += ` --exclude-subdags`
	}

	// -f, --only-failed
	// Only failed jobs
	// Default: False
	if o.GetOnlyFailed() {
		cmd += ` --only-failed`
	}

	// -r, --only-running
	// Only running jobs
	// Default: False
	if o.GetOnlyRunning() {
		cmd += ` --only-running`
	}

	// -s, --start-date
	// Override start_date YYYY-MM-DD
	if o.HasStartDate() {
		startDateString := o.GetStartDate()
		// attempt to parse the date to confirm it's YYYY-MM-DD
		_, err := time.Parse("2006-01-02", startDateString)
		if err != nil {
			return MWAAData{}, err
		}
		cmd += fmt.Sprintf(` --start-date '%s'`, startDateString)
	}

	// Not planning on Implementing?
	// -S, --subdir
	// File location or directory from which to look for the dag. Defaults to ‘[AIRFLOW_HOME]/dags’ where [AIRFLOW_HOME] is the value you set for ‘AIRFLOW_HOME’ config you set in ‘airflow.cfg’
	// Default: “[AIRFLOW_HOME]/dags”

	// -t, --task-regex
	// The regex to filter specific task_ids to backfill (optional)
	if o.TaskRegexp.IsSet() {
		cmd += fmt.Sprintf(` --task-regex '%s'`, *o.TaskRegexp.Get())
	}

	// -u, --upstream
	// Include upstream tasks
	// Default: False
	if o.IncludeUpstream.IsSet() && *o.IncludeUpstream.Get() {
		cmd += ` --upstream`
	}

	// -y, --yes
	// if not a dryrun then do not ask to approve
	if !dryRun {
		cmd += ` --yes`
	}
	// cmd += fmt.Sprintf(` '%s'`, clearTask.)
	data, err := PostMWAACommand(o.CLI, cmd)
	if err != nil {
		return MWAAData{}, err
	}
	return data, nil
}

// Returns the unmet dependencies for a task instance
func (cli *CLIENT) GetTaskFailedDeps(dagId string, taskId string, executionDate airflow.NullableTime, runId airflow.NullableString) (MWAAData, error) {
	// airflow tasks failed-deps [-h] [-S SUBDIR]
	//     dag_id task_id execution_date_or_run_id
	if executionDate.IsSet() && runId.IsSet() {
		return MWAAData{}, errors.New("executionDate and runId are mutually exclusive")
	}
	if !executionDate.IsSet() && !runId.IsSet() {
		return MWAAData{}, errors.New("executionDate or runId required")
	}
	cmd := fmt.Sprintf(`tasks failed-deps '%s' '%s'`, dagId, taskId)
	if executionDate.IsSet() {
		cmd += fmt.Sprintf(` '%s'`, executionDate.Get())
	} else {
		cmd += fmt.Sprintf(` '%s'`, *runId.Get())
	}
	return PostMWAACommand(cli, cmd)
}

// Returns tasks for given dagId
func (cli *CLIENT) GetDagTasks(dagId string) ([]DagTask, error) {
	// airflow tasks list dag_id --tree
	cmd := fmt.Sprintf(`tasks list '%s' --tree`, dagId)
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		fmt.Printf("\n%+v", data)
		return []DagTask{}, err
	}
	dagIdNotFoundException := fmt.Sprintf(`airflow.exceptions.AirflowException: Dag '%s' could not be found; either it does not exist or it failed to parse.`, dagId)
	if strings.Contains(data.StderrStr, dagIdNotFoundException) {
		return []DagTask{}, errors.New(dagIdNotFoundException)
	}
	return UnmarshalDagTasks(data)
}

// Get the status of a task instance
func (cli *CLIENT) GetTaskState(dagId string, taskId string, executionDate airflow.NullableTime, runId airflow.NullableString) (airflow.DagState, error) {
	// airflow tasks state [-h] [-S SUBDIR] [-v]
	// dag_id task_id execution_date_or_run_id
	if executionDate.IsSet() && runId.IsSet() {
		return airflow.DagState(""), errors.New("executionDate and runId are mutually exclusive")
	}
	if !executionDate.IsSet() && !runId.IsSet() {
		return airflow.DagState(""), errors.New("executionDate or runId required")
	}
	cmd := fmt.Sprintf(`tasks failed-deps '%s' '%s'`, dagId, taskId)
	if executionDate.IsSet() {
		cmd += fmt.Sprintf(` '%s'`, executionDate.Get())
	} else {
		cmd += fmt.Sprintf(` '%s'`, *runId.Get())
	}
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return airflow.DagState(""), err
	}
	return airflow.DagState(data.StdoutStr), nil
}

// Get the status of all task instances in a dag run
func (cli *CLIENT) GetTaskStatesDetailed(dagId string, executionDate airflow.NullableTime, runId airflow.NullableString) ([]TaskStatesDetailed, error) {
	// airflow tasks states-for-dag-run [-h] [-o table, json, yaml, plain] [-v]
	// dag_id execution_date_or_run_id
	if executionDate.IsSet() && runId.IsSet() {
		return []TaskStatesDetailed{}, errors.New("executionDate and runId are mutually exclusive")
	}
	if !executionDate.IsSet() && !runId.IsSet() {
		return []TaskStatesDetailed{}, errors.New("executionDate or runId required")
	}
	cmd := fmt.Sprintf(`tasks states-for-dag-run --output json '%s'`, dagId)
	if executionDate.IsSet() {
		cmd += fmt.Sprintf(` '%s'`, executionDate.Get())
	} else {
		cmd += fmt.Sprintf(` '%s'`, *runId.Get())
	}
	data, err := PostMWAACommand(cli, cmd)
	if err != nil {
		return []TaskStatesDetailed{}, err
	}
	return UnmarshalTaskStatesDetailed(data)
}

// Get the status of all task instances in a dag run
// func (cli *CLIENT) GetTaskStatesDetailed(dagId string, executionDate airflow.NullableTime, runId airflow.NullableString) ([]TaskStatesDetailed, error) {
// 	// airflow tasks test [-h] [-n] [--env-vars ENV_VARS] [-m] [-S SUBDIR]
// 	// [-t TASK_PARAMS]
// 	// dag_id task_id execution_date_or_run_id
// 	if executionDate.IsSet() && runId.IsSet() {
// 		return []TaskStatesDetailed{}, errors.New("executionDate and runId are mutually exclusive")
// 	}
// 	if !executionDate.IsSet() && !runId.IsSet() {
// 		return []TaskStatesDetailed{}, errors.New("executionDate or runId required")
// 	}
// 	cmd := fmt.Sprintf(`tasks test '%s'`, dagId)
// 	if executionDate.IsSet() {
// 		cmd += fmt.Sprintf(` '%s'`, executionDate.Get())
// 	} else {
// 		cmd += fmt.Sprintf(` '%s'`, *runId.Get())
// 	}
// 	data, err := PostMWAACommand(cli.token, cmd)
// 	if err != nil {
// 		return []TaskStatesDetailed{}, err
// 	}
// 	return UnmarshalTaskStatesDetailed(data)
// }
