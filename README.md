#  AWS MWAA (Managed Workflows for Apache Airflow) Helper - Go
Go Client for running Apache Airflow CLI commands on AWS MWAA (Managed Workflows for Apache Airflow) instances.  
https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-cli-command-reference.html

# Running CLI on a private VPC instance
Test locally using the following env and ssh tunnel configuration  
```shell
export HTTPS_PROXY="socks5://0:8080"
ssh -D 8080 -C -N  user@example.com
```


# Setting up a new CLI session
Create a mwaa service session and pass it to the NewCLI constructor
```go
sess := session.Must(session.NewSession())
// Create a MWAA client with additional configuration
svc := mwaah.New(sess, aws.NewConfig().WithRegion("example-region-1"))
mwaaName := "MWAA_ENVIRONMENT_NAME"
// create a new client without autoRefresh = false; lazily refresh cli token
cli := mwaah.NewClient(*svc, &mwaaName, false)
```


# Examples
## Triggering a New DAG Run
<table>
<tr>
<td>

```go
dagRun := airflow.NewDAGRun()
newRunId := "example-run-id"
targetDagId := "example-dag-id"
dagRun.SetDagRunId(newRunId)
dagRun.SetConf(map[string]interface{}{"foo": "bar", "blah": 1234})
dagRun.SetDagId(targetDagId)
newDagRun, err := cli.NewDagRun(*dagRun)
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
    fmt.Printf("newDagRun:\n %v\n", newDagRun)
}
```
</td>
<td>

```shell
newDagRun:
{
  "conf": {
    "blah": 1234,
    "foo": "bar"
  },
  "dag_id": "example-dag-id",
  "dag_run_id": "example-run-id",
  "execution_date": "2022-11-17T16:08:12Z",
  "external_trigger": true
}









```

</td>
</tr>
<tr>
<td>

```go
	dagState, err := cli.GetDagState(newDagRun.GetDagId(), newDagRun.GetExecutionDate())
    if err != nil {
        panic(err)
    }
	fmt.Printf("dagState: %+s", &dagState)
```
</td>
<td>

```shell
running
```

</td>
</tr>
</table>




# Currently Supported Apache Airflow CLI commands
| Version | Command                  |
|---------|--------------------------|
| v2.0+   | connections add          |
| v2.0+   | connections delete       |
| v2.0+   | dags delete              |
| v2.2.2  | dags list                |
| v2.0+   | dags list-jobs           |
| v2.2.2  | dags list-runs           |
| v2.0+   | dags pause               |
| v2.0+   | dags report              |
| v2.0+   | dags show                |
| v2.0+   | dags state               |
| v2.0+   | dags trigger             |
| v2.0+   | dags unpause             |
| v2.0+   | providers behaviours     |
| v2.0+   | providers get            |
| v2.0+   | providers hooks          |
| v2.0+   | providers links          |
| v2.0+   | providers list           |
| v2.0+   | roles list               |
| v2.0+   | tasks clear              |
| v2.0+   | tasks failed-deps        |
| v2.0+   | tasks list               |
| v2.0+   | tasks state              |
| v2.0+   | tasks states-for-dag-run |
| v2.0+   | variables delete         |
| v2.0+   | variables get            |
| v2.0+   | variables set            |
| v2.0+   | variables list           |
| v2.0+   | version                  |

# About
Copyright (c) Warner Media, LLC. All rights reserved. Licensed under the MIT license.
See the LICENSE file for license information.
