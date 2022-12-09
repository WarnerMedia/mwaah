// Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
// See the LICENSE file for license information.
package mwaah

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mwaah/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/mwaa"
	"github.com/joho/godotenv"
)

var (
	cli       *mwaah.CLIENT
	mwaaName  string
	newDagRun *airflow.DAGRun
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}
	sess := session.Must(session.NewSession())
	// Create a MWAA client with additional configuration
	svc := mwaa.New(sess, aws.NewConfig().WithRegion("eu-west-1"))
	mwaaName = os.Getenv("MWAA_ENVIRONMENT_NAME")
	cli = mwaah.NewClient(*svc, &mwaaName)
}

func TestGetDags(t *testing.T) {
	dags, err := cli.GetDags()
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%+v\n", dags)
		fmt.Println("Found DAGs:")
		fmt.Println("--------------------------------")
		for _, v := range dags {
			fmt.Println(v.DagId)
		}
	}
}

func TestNewDagRun(t *testing.T) {
	dagRun := airflow.NewDAGRun()
	ts, err := time.Now().UTC().MarshalText()
	if err != nil {
		fmt.Println(err)
	}
	newRunId := "test-new-run-" + string(ts)
	targetDagId := os.Getenv("DAG_ID")
	dagRun.SetDagRunId(newRunId)
	dagRun.SetConf(map[string]interface{}{"foo": "bar", "blah": 1234})
	dagRun.SetDagId(targetDagId)
	newDagRun, err = cli.NewDagRun(*dagRun)
	if err != nil {
		panic(err)
	} else {
		d, _ := json.Marshal(newDagRun)
		dst := &bytes.Buffer{}
		err := json.Indent(dst, d, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(dst.String())
	}
}

func TestAddConnection(t *testing.T) {
	conn := mwaah.Connection{}
	conn.SetConnectionId("new4")
	conn.SetConnType("hive_metastore")
	conn.SetLogin("airflow")
	conn.SetPassword("airflow")
	conn.SetHost("host")
	conn.SetPort(9083)
	conn.SetSchema("airflow")
	conn.SetDescription("new4 description")
	err := cli.AddConnection(conn)
	if err != nil {
		panic(err)
	}
}

func TestDeleteConnection(t *testing.T) {
	err := cli.DeleteConnection("new4")
	if err != nil {
		panic(err)
	}
}

func TestPauseDag(t *testing.T) {
	err := cli.PauseDag(os.Getenv("DAG_ID"))
	if err != nil {
		panic(err)
	}
}

func TestUnpauseDag(t *testing.T) {
	err := cli.UnpauseDag(os.Getenv("DAG_ID"))
	if err != nil {
		panic(err)
	}
}

func TestGetProviders(t *testing.T) {
	p, err := cli.GetProviders()
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%+v\n", p)
		fmt.Println("Found Providers:")
		fmt.Println("--------------------------------")
		for _, v := range p {
			fmt.Println(string(*v.PackageName))
		}
	}
}

func TestGetProviderHooks(t *testing.T) {
	p, err := cli.GetProviderHooks()
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%+v\n", p)
		fmt.Println("Found Provider Hooks:")
		fmt.Println("--------------------------------")
		for _, v := range p {
			fmt.Println(string(v.HookName))
		}
	}
}

func TestGetProviderLinks(t *testing.T) {
	p, err := cli.GetProviderLinks()
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("%+v\n", p)
		fmt.Println("Found Provider Links:")
		fmt.Println("--------------------------------")
		for _, v := range p {
			fmt.Println(string(v.ExtraLinkClassName))
		}
	}
}

func TestGetProviderDetailed(t *testing.T) {
	p, err := cli.GetProviderDetailed("apache-airflow-providers-amazon")
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Provider Detail:")
		fmt.Printf("%+v\n", p)
	}
}

func TestVariableSetGetDelete(t *testing.T) {
	key := "testvarkey"
	val := "testvarval"
	err := cli.SetVariableNoSerialize(key, val)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 4)
	getVal, err := cli.GetVariableNoSerialize(key)
	if err != nil {
		panic(err)
	}
	if !(val == getVal) {
		fmt.Println([]byte(val))
		fmt.Println([]byte(getVal))
		panic("vals are not equal")
	}
	time.Sleep(time.Second * 6)
	err = cli.DeleteVariable(key)
	if err != nil {
		panic(err)
	}
}

func TestGetVersion(t *testing.T) {
	ver, err := cli.GetVersion()
	if err != nil {
		panic(err)
	}
	fmt.Printf("version: %s\n", ver)
}

func TestGetDagState(t *testing.T) {
	dagState, err := cli.GetDagState(newDagRun.GetDagId(), newDagRun.GetExecutionDate())
	if err != nil {
		panic(err)
	}
	fmt.Printf("dagState: %+s\n", *dagState)
}

func TestGetDagStateNoSuchDagId(t *testing.T) {
	dagId := "this_dag_id_does_not_exist"
	// dagIdNotFoundException := fmt.Sprintf(`airflow.exceptions.AirflowException: Dag '%s' could not be found`, dagId)
	// errorDNE := fmt.Sprintf("%s does not exist", dagId)
	timeTomorrow, _ := mwaah.GetTimePythonISONoDecimal(time.Now().AddDate(0, 0, 1).String())
	dagState, err := cli.GetDagState(dagId, timeTomorrow)
	if err == nil {
		panic(err)
	}
	fmt.Printf("dagState: %+s\n", *dagState)
}

func TestGetDagStateNone(t *testing.T) {
	dagId := os.Getenv("DAG_ID")
	// errorDNE := fmt.Sprintf("%s does not exist", dagId)
	timeTomorrow := time.Now().AddDate(0, 0, 1)
	dagState, err := cli.GetDagState(dagId, timeTomorrow)
	if !strings.Contains(fmt.Sprint(err), "no dag found with executionDate:") {
		panic(err)
	}
	fmt.Printf("dagState: %+s\n", *dagState)
}

func TestGetDagJobs(t *testing.T) {
	dagId := os.Getenv("DAG_ID")
	state, _ := airflow.NewDagStateFromValue("success")
	// leave off limit
	i := mwaah.DagJobsInput{
		DagId: *airflow.NewNullableString(&dagId),
		State: *state,
	}
	dagJobs, err := cli.GetDagJobs(i)
	if err != nil {
		panic(err)
	}
	if len(dagJobs) < 1 {
		panic("no jobs found")
	}
	// fmt.Printf("dagJobs %+s", dagJobs)

	// add limit 1
	limit := 1
	i.Limit = *airflow.NewNullableInt(&limit)
	dagJobs, err = cli.GetDagJobs(i)
	// fmt.Printf("dagJobs %+v\n", dagJobs)
	if len(dagJobs) != 1 {
		panic("should return 1")
	}

	// add limit < 0
	limit = -1
	i.Limit = *airflow.NewNullableInt(&limit)
	dagJobs, err = cli.GetDagJobs(i)
	// fmt.Printf("dagJobs %+v\n", dagJobs)
	if len(dagJobs) != 0 {
		panic("should return [] for limit < 0")
	}

	// add limit 0
	limit = 0
	i.Limit = *airflow.NewNullableInt(&limit)
	dagJobs, err = cli.GetDagJobs(i)
	// fmt.Printf("dagJobs %+v\n", dagJobs)
	if len(dagJobs) != 0 {
		panic("should return [] for limit = 0")
	}

	// try non-existing dagId
	i.Limit.Unset()
	dagId = "this_dag_id_does_not_exist"
	i.DagId.Set(&dagId)
	// dagIdNotFoundException := fmt.Sprintf(`airflow.exceptions.AirflowException: Dag id %s not found`, *i.DagId.Get())
	dagJobs, err = cli.GetDagJobs(i)
	if err == nil {
		panic(err)
	}
}

// func TestDagTest(t *testing.T) {
// 	dagId := os.Getenv("DAG_ID")
// 	now := time.Now().AddDate(0, 0, -1)
// 	test, err := cli.DagTest(dagId, now)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Printf("%+v", test)
// }

// func TestGetProvidersBehaviours(t *testing.T) {
// 	test, err := cli.GetProvidersBehaviours()
// 	if err != nil {
// 		panic(err)
// 	}
// 	if len(test) < 1 {
// 		fmt.Printf("%+v", test)
// 		panic("found no providers behaviours")
// 	}
// }

func TestGetRoles(t *testing.T) {
	test, err := cli.GetRoles()
	if err != nil {
		panic(err)
	}
	if len(test) < 1 {
		fmt.Printf("%+v", test)
		panic("found no roles")
	}
}

func TestTokenRefreshes(t *testing.T) {
	ver, err := cli.GetVersion()
	if err != nil {
		panic(err)
	}
	fmt.Println(ver)
	time.Sleep(time.Second * 70)
	ver, err = cli.GetVersion()
	if err != nil {
		panic(err)
	}
	fmt.Println(ver)
}
