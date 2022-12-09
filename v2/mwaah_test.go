// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"encoding/json"
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/mock"
	"github.com/aws/aws-sdk-go/service/mwaa"
)

var (
	stdOutNewDagRun string
	stdOutDagruns   string
	// b64String       string
	dataNewDagRun MWAAData
	dataDagRuns   MWAAData
	stderr        string
)

func init() {
	stderr = "/usr/local/lib/python3.7/site-packages/airflow/configuration.py:361 DeprecationWarning: The dag_concurrency option in [core] has been renamed to max_active_tasks_per_dag - the old setting has been used, but please update your config.\n"
	stdOutNewDagRun = "[2022-11-05 18:15:04,763] {{__init__.py:38}} INFO - Loaded API auth backend: <module airflow.api.auth.backend.basic_auth from /usr/local/lib/python3.7/site-packages/airflow/api/auth/backend/basic_auth.py>\nCreated <DagRun sizzle_reel_dev @ 2022-11-05T18:15:05-00:00: manual__2022-11-05T18:15:05+00:00, externally triggered: False>"
	// jsonString := `[{"dag_id": "sizzle_reel_dev", "run_id": "2GrxGljf6YHeLgXWNlHOtyZuF1I", "state": "failed", "execution_date": "2022-10-30T20:06:51.884209+00:00", "start_date": "2022-10-30T20:06:52.368385+00:00", "end_date": "2022-10-30T20:06:55.845341+00:00"}]`
	// b64String = base64.StdEncoding.EncodeToString([]byte(jsonString))
	// b64String = `W3siZGFnX2lkIjogInNpenpsZV9yZWVsX2RldiIsICJydW5faWQiOiAiMkdyeEdsamY2WUhlTGdYV05sSE90eVp1RjFJIiwgInN0YXRlIjogImZhaWxlZCIsICJleGVjdXRpb25fZGF0ZSI6ICIyMDIyLTEwLTMwVDIwOjA2OjUxLjg4NDIwOSswMDowMCIsICJzdGFydF9kYXRlIjogIjIwMjItMTAtMzBUMjA6MDY6NTIuMzY4Mzg1KzAwOjAwIiwgImVuZF9kYXRlIjogIjIwMjItMTAtMzBUMjA6MDY6NTUuODQ1MzQxKzAwOjAwIn1dCg==`
	dataNewDagRun = MWAAData{
		Stderr: []byte(stderr),
		Stdout: []byte(stdOutNewDagRun),
		// StdoutDecoded: stdOutNewDagRun,
	}
	stdOutDagruns = `[{"dag_id": "sizzle_reel_dev", "run_id": "2GrxGljf6YHeLgXWNlHOtyZuF1I", "state": "failed", "execution_date": "2022-10-30T20:06:51.884209+00:00", "start_date": "2022-10-30T20:06:52.368385+00:00", "end_date": "2022-10-30T20:06:55.845341+00:00"}]` + "\n"
	dataDagRuns = MWAAData{
		Stdout: []byte(stdOutDagruns),
		// StdoutDecoded: stdOutDagruns,
	}
}

func TestNewCLI(t *testing.T) {
	name := "testInstanceName"
	session := mock.Session
	svc := *mwaa.New(session, aws.NewConfig().WithRegion("us-east-1").WithDisableEndpointHostPrefix(true))
	m := CLIENT{
		svc:             svc,
		Name:            &name,
		tokenExpiration: time.Time{},
	}
	type args struct {
		svc  mwaa.MWAA
		name *string
	}
	tests := []struct {
		name string
		args args
		want *CLIENT
	}{
		{
			name: "TestNewClient",
			args: args{svc: svc, name: &name},
			want: &m,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClient(tt.args.svc, tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCLI() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestUnmarshalDagRuns(t *testing.T) {
	dagRuns := []airflow.DAGRun{
		*airflow.NewDAGRun(),
	}
	startDate, _ := GetTimePythonISODecimal("2022-10-30T20:06:52.368385+00:00")
	executionDate, _ := GetTimePythonISODecimal("2022-10-30T20:06:51.884209+00:00")
	endDate, _ := GetTimePythonISODecimal("2022-10-30T20:06:55.845341+00:00")
	dagRuns[0].SetDagId("sizzle_reel_dev")
	dagRuns[0].SetDagRunId("2GrxGljf6YHeLgXWNlHOtyZuF1I")
	dagRuns[0].SetState("failed")
	dagRuns[0].SetExecutionDate(executionDate)
	dagRuns[0].SetStartDate(startDate)
	dagRuns[0].SetEndDate(endDate)

	type args struct {
		data MWAAData
	}
	tests := []struct {
		name    string
		args    args
		want    []airflow.DAGRun
		wantErr bool
	}{
		{
			name:    "UnmarshalDagRuns",
			args:    args{data: dataDagRuns},
			want:    dagRuns,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalDagRuns(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalDagRuns() error = %+v, wantErr %+v", err, tt.wantErr)
			}
			gotM, err := json.Marshal(got)
			if (err != nil) != tt.wantErr {
				t.Errorf("could not json.Marshal error = %+v, gotM: %+v", err, gotM)
			}
			wantM, err := json.Marshal(tt.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("could not json.Marshal error = %+v, wantM: %+v", err, wantM)
			}
			if !(reflect.DeepEqual(gotM, wantM)) {
				t.Errorf("unmarshalDagRuns() = %+v, want %+v", string(gotM), string(wantM))
			}
		})
	}
}

func TestParseNewDagRun(t *testing.T) {
	str := "[2022-11-05 18:15:04,763] {{__init__.py:38}} INFO - Loaded API auth backend: <module airflow.api.auth.backend.basic_auth from /usr/local/lib/python3.7/site-packages/airflow/api/auth/backend/basic_auth.py>\nCreated <DagRun sizzle_reel_dev @ 2022-11-05T18:15:05-00:00: manual__2022-11-05T18:15:05+00:00, externally triggered: False>"
	// b64String := `WzIwMjItMTEtMDUgMTg6MTU6MDQsNzYzXSB7e19faW5pdF9fLnB5OjM4fX0gSU5GTyAtIExvYWRlZCBBUEkgYXV0aCBiYWNrZW5kOiA8bW9kdWxlIGFpcmZsb3cuYXBpLmF1dGguYmFja2VuZC5iYXNpY19hdXRoIGZyb20gL3Vzci9sb2NhbC9saWIvcHl0aG9uMy43L3NpdGUtcGFja2FnZXMvYWlyZmxvdy9hcGkvYXV0aC9iYWNrZW5kL2Jhc2ljX2F1dGgucHk+CkNyZWF0ZWQgPERhZ1J1biBzaXp6bGVfcmVlbF9kZXYgQCAyMDIyLTExLTA1VDE4OjE1OjA1LTAwOjAwOiBtYW51YWxfXzIwMjItMTEtMDVUMTg6MTU6MDUrMDA6MDAsIGV4dGVybmFsbHkgdHJpZ2dlcmVkOiBGYWxzZT4K`
	matches := airflow.NewDAGRun()
	executionDate, _ := GetTimePythonISONoDecimal("2022-11-05T18:15:05-00:00")
	matches.SetDagId("sizzle_reel_dev")
	matches.SetDagRunId("manual__2022-11-05T18:15:05+00:00")
	matches.SetExecutionDate(executionDate)
	matches.SetExternalTrigger(false)
	type args struct {
		data MWAAData
	}
	tests := []struct {
		name string
		args args
		want airflow.DAGRun
	}{
		{
			name: "ParseNewDagRun",
			args: args{data: MWAAData{
				StdoutStr: str,
				StderrStr: `this is an error`,
			}},
			want: *matches,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseNewDagRun(tt.args.data)
			if err != nil {
				panic(err)
			}
			gotM, err := json.Marshal(got)
			if err != nil {
				panic(err)
			}
			if err != nil {
				t.Errorf("could not json.Marshal error = %+v, gotM: %+v", err, gotM)
			}
			wantM, err := json.Marshal(tt.want)
			if err != nil {
				t.Errorf("could not json.Marshal error = %+v, wantM: %+v", err, wantM)
			}
			if !(reflect.DeepEqual(gotM, wantM)) {
				t.Errorf("ParseNewDagRun() = %+v, want %+v", string(gotM), string(wantM))
			}
		})
	}
}

func TestClearTask(t *testing.T) {
	stdoutString := "You are about to delete these 3 tasks:\n\n	<TaskInstance: example_bash_operator.run_after_loop manual__2022-11-07T00:31:19.756196+00:00 [success]>\n\n<TaskInstance: sizzle_reel_dev.sizzle_reel 2GrxGljf6YHeLgXWNlHOtyZuF1I [failed]>"
	// b64String := base64.StdEncoding.EncodeToString([]byte(stdoutString))
	MatchString1 := "<TaskInstance: example_bash_operator.run_after_loop manual__2022-11-07T00:31:19.756196+00:00 [success]>"
	DagId1 := "example_bash_operator"
	TaskId1 := "run_after_loop"
	DagRunId1 := "manual__2022-11-07T00:31:19.756196+00:00"
	State1 := "success"

	MatchString2 := "<TaskInstance: sizzle_reel_dev.sizzle_reel 2GrxGljf6YHeLgXWNlHOtyZuF1I [failed]>"
	DagId2 := "sizzle_reel_dev"
	TaskId2 := "sizzle_reel"
	DagRunId2 := "2GrxGljf6YHeLgXWNlHOtyZuF1I"
	State2 := "failed"

	want := []Task{
		{
			MatchString: &MatchString1,
			DagId:       &DagId1,
			TaskId:      &TaskId1,
			DagRunId:    &DagRunId1,
			State:       &State1,
		},
		{
			MatchString: &MatchString2,
			DagId:       &DagId2,
			TaskId:      &TaskId2,
			DagRunId:    &DagRunId2,
			State:       &State2,
		},
	}

	type args struct {
		data MWAAData
	}
	tests := []struct {
		name string
		args args
		want []Task
	}{
		{
			name: "TestClearTask",
			args: args{MWAAData{StdoutStr: stdoutString}},
			want: want,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalTasks(tt.args.data)
			if err != nil {
				panic(err)
			}
			gotM, err := json.Marshal(got)
			if err != nil {
				panic(err)
			}
			if err != nil {
				t.Errorf("could not json.Marshal error = %+v, gotM: %+v", err, gotM)
			}
			wantM, err := json.Marshal(tt.want)
			if err != nil {
				t.Errorf("could not json.Marshal error = %+v, wantM: %+v", err, wantM)
			}
			if !(reflect.DeepEqual(gotM, wantM)) {
				t.Errorf("unmarshalDagRuns() = %+v, want %+v", string(gotM), string(wantM))
			}
		})
	}
}

func TestUnmarshalMWAADags(t *testing.T) {
	data := MWAAData{
		StdoutStr: `[{"dag_id": "Hello_World_ECS", "run_id": "test-new-run-2022-11-02T02:09:45.621947Z", "state": "running", "execution_date": "2022-11-02T02:09:49+00:00", "start_date": "2022-11-02T02:09:49.711294+00:00", "end_date": ""}, {"dag_id": "Hello_World_ECS", "run_id": "test-new-run-2022-11-02T02:06:22.550887Z", "state": "success", "execution_date": "2022-11-02T02:06:26+00:00", "start_date": "2022-11-02T02:06:27.242091+00:00", "end_date": "2022-11-02T02:07:59.552628+00:00"}, {"dag_id": "Hello_World_ECS", "run_id": "test-new-run-2022-11-02T00:01:41.935035Z", "state": "success", "execution_date": "2022-11-02T00:01:45+00:00", "start_date": "2022-11-02T00:01:45.857448+00:00", "end_date": "2022-11-02T00:03:18.263817+00:00"}]`,
	}
	data.Stdout = []byte(data.StdoutStr)
	var apacheDags []DagRun
	err := json.Unmarshal(data.Stdout, &apacheDags)
	if err != nil {
		panic(err)
	}
}

func walk(s string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	if !d.IsDir() {
		println(s)
	}
	return nil
}

func TestCopyrightHeadersExist(t *testing.T) {
	missingPrefix := new([]string)
	prefix := "// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.\n// See the LICENSE file for license information.\n"
	validCopyrightHeader := func(filePath string) bool {
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			panic(err)
		}
		return strings.HasPrefix(string(data), prefix)
	}
	filesWithoutPrefix := func(s string, d fs.DirEntry, err error) error {
		if strings.HasSuffix(s, ".go") && !validCopyrightHeader(s) {
			*missingPrefix = append(*missingPrefix, s)
		}
		return nil
	}
	filepath.WalkDir("..", filesWithoutPrefix)
	if len(*missingPrefix) > 0 {
		panic("\n found files without a valid copyright header:\n" + strings.Join(*missingPrefix, "\n"))
	}
}
