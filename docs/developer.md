<h1>Developer Guide</h1>

Read this guide if you want to debug the operator, fix bugs or contribute new
features and tests.

## Setting up Go

Postgres Operator is written in Go. Use the [installation instructions](https://golang.org/doc/install#install)
if you don't have Go on your system. You won't be able to compile the operator
with Go older than 1.7. We recommend installing [the latest one](https://golang.org/dl/).

Go projects expect their source code and all the dependencies to be located
under the [GOPATH](https://github.com/golang/go/wiki/GOPATH). Normally, one
would create a directory for the GOPATH (i.e. ~/go) and place the source code
under the ~/go/src sub directories.

Given the schema above, the Postgres Operator source code located at
`github.com/zalando/postgres-operator` should be put at
-`~/go/src/github.com/zalando/postgres-operator`.

```bash
export GOPATH=~/go
mkdir -p ${GOPATH}/src/github.com/zalando/
cd ${GOPATH}/src/github.com/zalando/
git clone https://github.com/zalando/postgres-operator.git
```

## Building the operator

We use [Go Modules](https://github.com/golang/go/wiki/Modules) for handling
dependencies. When using Go below v1.13 you need to explicitly enable Go modules
by setting the `GO111MODULE` environment variable to `on`. The make targets do
this for you, so simply run

```bash
make deps
```

This would take a while to complete. You have to redo `make deps` every time
your dependencies list changes, i.e. after adding a new library dependency.

Build the operator with the `make docker` command. You may define the TAG
variable to assign an explicit tag to your Docker image and the IMAGE to set
the image name. By default, the tag is computed with
`git describe --tags --always --dirty` and the image is
`registry.opensource.zalan.do/acid/postgres-operator`

```bash
export TAG=$(git describe --tags --always --dirty)
make docker
```

Building the operator binary (for testing the out-of-cluster option):

```bash
make
```

The binary will be placed into the build directory.

## Deploying self build image

The fastest way to run and test your Docker image locally is to reuse the Docker
environment from [minikube](https://github.com/kubernetes/minikube/releases)
or use the `load docker-image` from [kind](https://kind.sigs.k8s.io/). The
following steps will get you the Docker image built and deployed.

```bash
# minikube
eval $(minikube docker-env)
make docker

# kind
make docker
kind load docker-image <image> --name <kind-cluster-name>
```

Then create a new Postgres Operator deployment. You can reuse the provided
manifest but replace the version and tag. Don't forget to also apply
configuration and RBAC manifests first, e.g.:

```bash
kubectl create -f manifests/configmap.yaml
kubectl create -f manifests/operator-service-account-rbac.yaml
sed -e "s/\(image\:.*\:\).*$/\1$TAG/" manifests/postgres-operator.yaml | kubectl create  -f -

# check if the operator is coming up
kubectl get pod -l name=postgres-operator
```

## Code generation

The operator employs K8s-provided code generation to obtain deep copy methods
and K8s-like APIs for its custom resource definitions, namely the
Postgres CRD and the operator CRD. The usage of the code generation follows
conventions from the K8s community. Relevant scripts live in the `hack`
directory:
* `update-codegen.sh` triggers code generation for the APIs defined in `pkg/apis/acid.zalan.do/`,
* `verify-codegen.sh` checks if the generated code is up-to-date (to be used within CI).

The `/pkg/generated/` contains the resultant code. To make these scripts work,
you may need to `export GOPATH=$(go env GOPATH)`

References for code generation are:
* [Relevant pull request](https://github.com/zalando/postgres-operator/pull/369)
See comments there for minor issues that can sometimes broke the generation process.
* [Code generator source code](https://github.com/kubernetes/code-generator)
* [Code Generation for CustomResources](https://blog.openshift.com/kubernetes-deep-dive-code-generation-customresources/) - intro post on the topic
* Code generation in [Prometheus](https://github.com/coreos/prometheus-operator) and [etcd](https://github.com/coreos/etcd-operator) operators

To debug the generated API locally, use the
[kubectl proxy](https://kubernetes.io/docs/tasks/access-kubernetes-api/http-proxy-access-api/)
and `kubectl --v=8` log level to display contents of HTTP requests (run the
operator itself with `--v=8` to log all REST API requests). To attach a debugger
to the operator, use the `-outofcluster` option to run the operator locally on
the developer's laptop (and not in a docker container).

## Debugging the operator

There is a web interface in the operator to observe its internal state. The
operator listens on port 8080. It is possible to expose it to the
`localhost:8080` by doing:

```bash
kubectl --context minikube port-forward $(kubectl --context minikube get pod -l name=postgres-operator -o jsonpath={.items..metadata.name}) 8080:8080
```

The inner query gets the name of the Postgres Operator pod, and the outer one
enables port forwarding. Afterwards, you can access the operator API with:

```bash
curl --location http://127.0.0.1:8080/$endpoint | jq .
```

The available endpoints are listed below. Note that the worker ID is an integer
from 0 up to 'workers' - 1 (value configured in the operator configuration and
defaults to 4)

* /databases - all databases per cluster
* /workers/all/queue - state of the workers queue (cluster events to process)
* /workers/$id/queue - state of the queue for the worker $id
* /workers/$id/logs - log of the operations performed by a given worker
* /clusters/ - list of teams and clusters known to the operator
* /clusters/$team - list of clusters for the given team
* /clusters/$team/$namespace/$clustername - detailed status of the cluster,
  including the specifications for CRD, master and replica services, endpoints
  and statefulsets, as well as any errors and the worker that cluster is
  assigned to.
* /clusters/$team/$namespace/$clustername/logs/ - logs of all operations
  performed to the cluster so far.
* /clusters/$team/$namespace/$clustername/history/ - history of cluster changes
  triggered by the changes of the manifest (shows the somewhat obscure diff and
  what exactly has triggered the change)

The operator also supports pprof endpoints listed at the
[pprof package](https://golang.org/pkg/net/http/pprof/), such as:

* /debug/pprof/
* /debug/pprof/cmdline
* /debug/pprof/profile
* /debug/pprof/symbol
* /debug/pprof/trace

It's possible to attach a debugger to troubleshoot postgres-operator inside a
Docker container. It's possible with [gdb](https://www.gnu.org/software/gdb/)
and [delve](https://github.com/derekparker/delve). Since the latter one is a
specialized debugger for Go, we will use it as an example. To use it you need:

* Install delve locally

```bash
go get -u github.com/derekparker/delve/cmd/dlv
```

* Add following dependencies to the `Dockerfile`

```
RUN apk --no-cache add go git musl-dev
RUN go get github.com/derekparker/delve/cmd/dlv
```

* Update the `Makefile` to build the project with debugging symbols. For that
  you need to add `gcflags` to a build target for corresponding OS (e.g.
  GNU/Linux)

```
-gcflags "-N -l"
```

* Run `postgres-operator` under the delve. For that you need to replace
  `ENTRYPOINT` with the following `CMD`:

```
CMD ["/root/go/bin/dlv", "--listen=:DLV_PORT", "--headless=true", "--api-version=2", "exec", "/postgres-operator"]
```

* Forward the listening port

```bash
kubectl port-forward POD_NAME DLV_PORT:DLV_PORT
```

* Attach to it

```bash
dlv connect 127.0.0.1:DLV_PORT
```

## Unit tests

To run all unit tests, you can simply do:

```bash
go test ./pkg/...
```

In case if you need to debug your unit test, it's possible to use delve:

```bash
dlv test ./pkg/util/retryutil/
Type 'help' for list of commands.
(dlv) c
PASS
```

To test the multi-namespace setup, you can use

```bash
./run_operator_locally.sh --rebuild-operator
```
It will automatically create an `acid-minimal-cluster` in the namespace `test`.
Then you can for example check the Patroni logs:

```bash
kubectl logs acid-minimal-cluster-0
```

## End-to-end tests

The operator provides reference end-to-end tests (e2e) (as Docker image) to
ensure various infrastructure parts work smoothly together. Each e2e execution
tests a Postgres Operator image built from the current git branch. The test
runner creates a new local K8s cluster using [kind](https://kind.sigs.k8s.io/),
utilizes provided manifest examples, and runs e2e tests contained in the `tests`
folder. The K8s API client in the container connects to the `kind` cluster via
the standard Docker `bridge` network. The kind cluster is deleted if tests
finish successfully or on each new run in case it still exists.

End-to-end tests are executed automatically during builds (for more details,
see the [README](../e2e/README.md) in the `e2e` folder):

```bash
make e2e
```

End-to-end tests are written in Python and use `flake8` for code quality.
Please run flake8 [before submitting a PR](http://flake8.pycqa.org/en/latest/user/using-hooks.html).

## Introduce additional configuration parameters

In the case you want to add functionality to the operator that shall be
controlled via the operator configuration there are a few places that need to
be updated. As explained [here](reference/operator_parameters.md), it's possible
to configure the operator either with a ConfigMap or CRD, but currently we aim
to synchronize parameters everywhere.

When choosing a parameter name for a new option in a Postgres cluster manifest,
keep in mind the naming conventions there. We use `camelCase` for manifest
parameters (with exceptions for certain Patroni/Postgres options) and
`snake_case` variables in the configuration. Only introduce new manifest
variables if you feel a per-cluster configuration is necessary.

Note: If one option is defined in the operator configuration and in the cluster
[manifest](../manifests/complete-postgres-manifest.yaml), the latter takes
precedence.

### Go code

Update the following Go files that obtain the configuration parameter from the
manifest files:
* [operator_configuration_type.go](../pkg/apis/acid.zalan.do/v1/operator_configuration_type.go)
* [operator_config.go](../pkg/controller/operator_config.go)
* [config.go](../pkg/util/config/config.go)

Postgres manifest parameters are defined in the [api package](../pkg/apis/acid.zalan.do/v1/postgresql_type.go).
The operator behavior has to be implemented at least in [k8sres.go](../pkg/cluster/k8sres.go).
Validation of CRD parameters is controlled in [crds.go](../pkg/apis/acid.zalan.do/v1/crds.go).
Please, reflect your changes in tests, for example in:
* [config_test.go](../pkg/util/config/config_test.go)
* [k8sres_test.go](../pkg/cluster/k8sres_test.go)
* [util_test.go](../pkg/apis/acid.zalan.do/v1/util_test.go)

### Updating manifest files

For the CRD-based configuration, please update the following files:
* the default [OperatorConfiguration](../manifests/postgresql-operator-default-configuration.yaml)
* the Helm chart's [values-crd file](../charts/postgres-operator/values.yaml)
* the CRD's [validation](../manifests/operatorconfiguration.crd.yaml)

Reflect the changes in the ConfigMap configuration as well (note that numeric
and boolean parameters have to use double quotes here):
* [ConfigMap](../manifests/configmap.yaml) manifest
* the Helm chart's default [values file](../charts/postgres-operator/values.yaml)

### Updating documentation

Finally, add a section for each new configuration option and/or cluster manifest
parameter in the reference documents:
* [config reference](reference/operator_parameters.md)
* [manifest reference](reference/cluster_manifest.md)

It also helps users to explain new features with examples in the
[administrator docs](administrator.md).
