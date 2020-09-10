#!/usr/bin/env bash

# enable unofficial bash strict mode
set -o errexit
set -o nounset
set -o pipefail
IFS=$'\n\t'

cd $(dirname "$0");

readonly cluster_name="postgres-operator-e2e-tests"
readonly kubeconfig_path="/tmp/kind-config-${cluster_name}"

function pull_images(){

  operator_tag=$(git describe --tags --always --dirty)
  if [[ -z $(docker images -q registry.opensource.zalan.do/acid/postgres-operator:${operator_tag}) ]]
  then
    docker pull registry.opensource.zalan.do/acid/postgres-operator:latest
  fi
  if [[ -z $(docker images -q registry.opensource.zalan.do/acid/postgres-operator-e2e-tests:${operator_tag}) ]]
  then
    docker pull registry.opensource.zalan.do/acid/postgres-operator-e2e-tests:latest
  fi

  operator_image=$(docker images --filter=reference="registry.opensource.zalan.do/acid/postgres-operator" --format "{{.Repository}}:{{.Tag}}" | head -1)
  e2e_test_image=$(docker images --filter=reference="registry.opensource.zalan.do/acid/postgres-operator-e2e-tests" --format "{{.Repository}}:{{.Tag}}" | head -1)
}

function start_kind(){

  # avoid interference with previous test runs
  if [[ $(kind get clusters | grep "^${cluster_name}*") != "" ]]
  then
    kind delete cluster --name ${cluster_name}
  fi

  export KUBECONFIG="${kubeconfig_path}"
  kind create cluster --name ${cluster_name} --config kind-cluster-postgres-operator-e2e-tests.yaml
  kind load docker-image "${operator_image}" --name ${cluster_name}
  kind load docker-image "${e2e_test_image}" --name ${cluster_name}
}

function set_kind_api_server_ip(){
  # use the actual kubeconfig to connect to the 'kind' API server
  # but update the IP address of the API server to the one from the Docker 'bridge' network
  readonly local kind_api_server_port=6443 # well-known in the 'kind' codebase
  readonly local kind_api_server=$(docker inspect --format "{{ .NetworkSettings.Networks.kind.IPAddress }}:${kind_api_server_port}" "${cluster_name}"-control-plane)
  sed -i "s/server.*$/server: https:\/\/$kind_api_server/g" "${kubeconfig_path}"
}

function run_tests(){
  docker run --rm --network kind --mount type=bind,source="$(readlink -f ${kubeconfig_path})",target=/root/.kube/config -e OPERATOR_IMAGE="${operator_image}" "${e2e_test_image}"
}

function clean_up(){
  unset KUBECONFIG
  kind delete cluster --name ${cluster_name}
  rm -rf ${kubeconfig_path}
}

function main(){

  trap "clean_up" QUIT TERM EXIT

  pull_images
  start_kind
  set_kind_api_server_ip
  run_tests
  exit 0
}

main "$@"
