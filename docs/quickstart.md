<h1>Quickstart</h1>

This guide aims to give you a quick look and feel for using the Postgres
Operator on a local Kubernetes environment.

## Prerequisites

Since the Postgres Operator is designed for the Kubernetes (K8s) framework,
hence set it up first. For local tests we recommend to use one of the following
solutions:

* [minikube](https://github.com/kubernetes/minikube/releases), which creates a
  single-node K8s cluster inside a VM (requires KVM or VirtualBox),
* [kind](https://kind.sigs.k8s.io/) and [k3d](https://k3d.io), which allows creating multi-nodes K8s
  clusters running on Docker (requires Docker)

To interact with the K8s infrastructure install it's CLI runtime [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl-binary-via-curl).

This quickstart assumes that you have started minikube or created a local kind
cluster. Note that you can also use built-in K8s support in the Docker Desktop
for Mac to follow the steps of this tutorial. You would have to replace
`minikube start` and `minikube delete` with your launch actions for the Docker
built-in K8s support.

## Configuration Options

Configuring the Postgres Operator is only possible before deploying a new
Postgres cluster. This can work in two ways: via a ConfigMap or a custom
`OperatorConfiguration` object. More details on configuration can be found
[here](reference/operator_parameters.md).

## Deployment options

The Postgres Operator can be deployed in the following ways:

* Manual deployment
* Kustomization
* Helm chart

### Manual deployment setup

The Postgres Operator can be installed simply by applying yaml manifests. Note,
we provide the `/manifests` directory as an example only; you should consider
adjusting the manifests to your K8s environment (e.g. namespaces).

```bash
# First, clone the repository and change to the directory
git clone https://github.com/zalando/postgres-operator.git
cd postgres-operator

# apply the manifests in the following order
kubectl create -f manifests/configmap.yaml  # configuration
kubectl create -f manifests/operator-service-account-rbac.yaml  # identity and permissions
kubectl create -f manifests/postgres-operator.yaml  # deployment
kubectl create -f manifests/api-service.yaml  # operator API to be used by UI
```

There is a [Kustomization](https://github.com/kubernetes-sigs/kustomize)
manifest that [combines the mentioned resources](../manifests/kustomization.yaml)
(except for the CRD) - it can be used with kubectl 1.14 or newer as easy as:

```bash
kubectl apply -k github.com/zalando/postgres-operator/manifests
```

For convenience, we have automated starting the operator with minikube using the
`run_operator_locally` script. It applies the [`acid-minimal-cluster`](../manifests/minimal-postgres-manifest.yaml).
manifest.

```bash
./run_operator_locally.sh
```

### Helm chart

Alternatively, the operator can be installed by using the provided [Helm](https://helm.sh/)
chart which saves you the manual steps. Clone this repo and change directory to
the repo root. With Helm v3 installed you should be able to run:

```bash
helm install postgres-operator ./charts/postgres-operator
```

The chart works with both Helm 2 and Helm 3. The `crd-install` hook from v2 will
be skipped with warning when using v3. Documentation for installing applications
with Helm 2 can be found in the [v2 docs](https://v2.helm.sh/docs/).

The chart is also hosted at: https://opensource.zalando.com/postgres-operator/charts/postgres-operator/

## Check if Postgres Operator is running

Starting the operator may take a few seconds. Check if the operator pod is
running before applying a Postgres cluster manifest.

```bash
# if you've created the operator using yaml manifests
kubectl get pod -l name=postgres-operator

# if you've created the operator using helm chart
kubectl get pod -l app.kubernetes.io/name=postgres-operator
```

If the operator doesn't get into `Running` state, either check the latest K8s
events of the deployment or pod with `kubectl describe` or inspect the operator
logs:

```bash
kubectl logs "$(kubectl get pod -l name=postgres-operator --output='name')"
```

## Deploy the operator UI

In the following paragraphs we describe how to access and manage PostgreSQL
clusters from the command line with kubectl. But it can also be done from the
browser-based [Postgres Operator UI](operator-ui.md). Before deploying the UI
make sure the operator is running and its REST API is reachable through a
[K8s service](../manifests/api-service.yaml). The URL to this API must be
configured in the [deployment manifest](../ui/manifests/deployment.yaml#L43)
of the UI.

To deploy the UI simply apply all its manifests files or use the UI helm chart:

```bash
# manual deployment
kubectl apply -f ui/manifests/

# or kustomization
kubectl apply -k github.com/zalando/postgres-operator/ui/manifests

# or helm chart
helm install postgres-operator-ui ./charts/postgres-operator-ui
```

Like with the operator, check if the UI pod gets into `Running` state:

```bash
# if you've created the operator using yaml manifests
kubectl get pod -l name=postgres-operator-ui

# if you've created the operator using helm chart
kubectl get pod -l app.kubernetes.io/name=postgres-operator-ui
```

You can now access the web interface by port forwarding the UI pod (mind the
label selector) and enter `localhost:8081` in your browser:

```bash
kubectl port-forward svc/postgres-operator-ui 8081:80
```

Available option are explained in detail in the [UI docs](operator-ui.md).

## Create a Postgres cluster

If the operator pod is running it listens to new events regarding `postgresql`
resources. Now, it's time to submit your first Postgres cluster manifest.

```bash
# create a Postgres cluster
kubectl create -f manifests/minimal-postgres-manifest.yaml
```

After the cluster manifest is submitted and passed the validation the operator
will create Service and Endpoint resources and a StatefulSet which spins up new
Pod(s) given the number of instances specified in the manifest. All resources
are named like the cluster. The database pods can be identified by their number
suffix, starting from `-0`. They run the [Spilo](https://github.com/zalando/spilo)
container image by Zalando. As for the services and endpoints, there will be one
for the master pod and another one for all the replicas (`-repl` suffix). Check
if all components are coming up. Use the label `application=spilo` to filter and
list the label `spilo-role` to see who is currently the master.

```bash
# check the deployed cluster
kubectl get postgresql

# check created database pods
kubectl get pods -l application=spilo -L spilo-role

# check created service resources
kubectl get svc -l application=spilo -L spilo-role
```

## Connect to the Postgres cluster via psql

You can create a port-forward on a database pod to connect to Postgres. See the
[user guide](user.md#connect-to-postgresql) for instructions. With minikube it's
also easy to retrieve the connections string from the K8s service that is
pointing to the master pod:

```bash
export HOST_PORT=$(minikube service acid-minimal-cluster --url | sed 's,.*/,,')
export PGHOST=$(echo $HOST_PORT | cut -d: -f 1)
export PGPORT=$(echo $HOST_PORT | cut -d: -f 2)
```

Retrieve the password from the K8s Secret that is created in your cluster.
Non-encrypted connections are rejected by default, so set the SSL mode to
require:

```bash
export PGPASSWORD=$(kubectl get secret postgres.acid-minimal-cluster.credentials -o 'jsonpath={.data.password}' | base64 -d)
export PGSSLMODE=require
psql -U postgres
```

## Delete a Postgres cluster

To delete a Postgres cluster simply delete the `postgresql` custom resource.

```bash
kubectl delete postgresql acid-minimal-cluster
```

This should remove the associated StatefulSet, database Pods, Services and
Endpoints. The PersistentVolumes are released and the PodDisruptionBudget is
deleted. Secrets however are not deleted and backups will remain in place.

When deleting a cluster while it is still starting up or got stuck during that
phase it can [happen](https://github.com/zalando/postgres-operator/issues/551)
that the `postgresql` resource is deleted leaving orphaned components behind.
This can cause troubles when creating a new Postgres cluster. For a fresh setup
you can delete your local minikube or kind cluster and start again.
