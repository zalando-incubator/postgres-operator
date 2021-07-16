import json
import time
import subprocess
import warnings

from kubernetes import client, config
from kubernetes.client.rest import ApiException


def to_selector(labels):
    return ",".join(["=".join(lbl) for lbl in labels.items()])


class K8sApi:

    def __init__(self):

        # https://github.com/kubernetes-client/python/issues/309
        warnings.simplefilter("ignore", ResourceWarning)

        self.config = config.load_kube_config()
        self.k8s_client = client.ApiClient()

        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.batch_v1_beta1 = client.BatchV1beta1Api()
        self.custom_objects_api = client.CustomObjectsApi()
        self.policy_v1_beta1 = client.PolicyV1beta1Api()
        self.storage_v1_api = client.StorageV1Api()


class K8s:
    '''
    Wraps around K8s api client and helper methods.
    '''

    RETRY_TIMEOUT_SEC = 1

    def __init__(self, labels='x=y', namespace='default'):
        self.api = K8sApi()
        self.labels = labels
        self.namespace = namespace

    def get_pg_nodes(self, pg_cluster_name, namespace='default'):
        master_pod_node = ''
        replica_pod_nodes = []
        podsList = self.api.core_v1.list_namespaced_pod(namespace, label_selector=pg_cluster_name)
        for pod in podsList.items:
            if pod.metadata.labels.get('spilo-role') == 'master':
                master_pod_node = pod.spec.node_name
            elif pod.metadata.labels.get('spilo-role') == 'replica':
                replica_pod_nodes.append(pod.spec.node_name)

        return master_pod_node, replica_pod_nodes

    def get_cluster_nodes(self, cluster_labels='cluster-name=acid-minimal-cluster', namespace='default'):
        m = []
        r = []
        podsList = self.api.core_v1.list_namespaced_pod(namespace, label_selector=cluster_labels)
        for pod in podsList.items:
            if pod.metadata.labels.get('spilo-role') == 'master' and pod.status.phase == 'Running':
                m.append(pod.spec.node_name)
            elif pod.metadata.labels.get('spilo-role') == 'replica' and pod.status.phase == 'Running':
                r.append(pod.spec.node_name)

        return m, r

    def wait_for_operator_pod_start(self):
        self.wait_for_pod_start("name=postgres-operator")
        # give operator time to subscribe to objects
        time.sleep(1)
        return True

    def get_operator_pod(self):
        pods = self.api.core_v1.list_namespaced_pod(
            'default', label_selector='name=postgres-operator'
        ).items

        pods = list(filter(lambda x: x.status.phase == 'Running', pods))

        if len(pods):
            return pods[0]

        return None

    def get_operator_log(self):
        operator_pod = self.get_operator_pod()
        pod_name = operator_pod.metadata.name
        return self.api.core_v1.read_namespaced_pod_log(
            name=pod_name,
            namespace='default'
        )

    def pg_get_status(self, name="acid-minimal-cluster", namespace="default"):
        pg = self.api.custom_objects_api.get_namespaced_custom_object(
            "acid.zalan.do", "v1", namespace, "postgresqls", name)
        return pg.get("status", {}).get("PostgresClusterStatus", None)

    def wait_for_pod_start(self, pod_labels, namespace='default'):
        pod_phase = 'No pod running'
        while pod_phase != 'Running':
            pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=pod_labels).items
            if pods:
                pod_phase = pods[0].status.phase

            time.sleep(self.RETRY_TIMEOUT_SEC)

    def get_service_type(self, svc_labels, namespace='default'):
        svc_type = ''
        svcs = self.api.core_v1.list_namespaced_service(namespace, label_selector=svc_labels, limit=1).items
        for svc in svcs:
            svc_type = svc.spec.type
        return svc_type

    def check_service_annotations(self, svc_labels, annotations, namespace='default'):
        svcs = self.api.core_v1.list_namespaced_service(namespace, label_selector=svc_labels, limit=1).items
        for svc in svcs:
            for key, value in annotations.items():
                if not svc.metadata.annotations or key not in svc.metadata.annotations or svc.metadata.annotations[key] != value:
                    print("Expected key {} not found in service annotations {}".format(key, svc.metadata.annotations))
                    return False
        return True

    def check_statefulset_annotations(self, sset_labels, annotations, namespace='default'):
        ssets = self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=sset_labels, limit=1).items
        for sset in ssets:
            for key, value in annotations.items():
                if key not in sset.metadata.annotations or sset.metadata.annotations[key] != value:
                    print("Expected key {} not found in statefulset annotations {}".format(key, sset.metadata.annotations))
                    return False
        return True

    def scale_cluster(self, number_of_instances, name="acid-minimal-cluster", namespace="default"):
        body = {
            "spec": {
                "numberOfInstances": number_of_instances
            }
        }
        self.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", namespace, "postgresqls", name, body)

    def wait_for_running_pods(self, labels, number, namespace=''):
        while self.count_pods_with_label(labels) != number:
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_pods_to_stop(self, labels, namespace=''):
        while self.count_pods_with_label(labels) != 0:
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_service(self, labels, namespace='default'):
        def get_services():
            return self.api.core_v1.list_namespaced_service(
                namespace, label_selector=labels
            ).items

        while not get_services():
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def count_pods_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items)

    def count_services_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_service(namespace, label_selector=labels).items)

    def count_endpoints_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_endpoints(namespace, label_selector=labels).items)

    def count_secrets_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_secret(namespace, label_selector=labels).items)

    def count_statefulsets_with_label(self, labels, namespace='default'):
        return len(self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=labels).items)

    def count_deployments_with_label(self, labels, namespace='default'):
        return len(self.api.apps_v1.list_namespaced_deployment(namespace, label_selector=labels).items)

    def count_pdbs_with_label(self, labels, namespace='default'):
        return len(self.api.policy_v1_beta1.list_namespaced_pod_disruption_budget(
            namespace, label_selector=labels).items)

    def count_running_pods(self, labels='application=spilo,cluster-name=acid-minimal-cluster', namespace='default'):
        pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
        return len(list(filter(lambda x: x.status.phase == 'Running', pods)))

    def count_pods_with_container_capabilities(self, capabilities, labels, namespace='default'):
        pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
        return len(list(filter(lambda x: x.spec.containers[0].security_context.capabilities.add == capabilities, pods)))

    def wait_for_pod_failover(self, failover_targets, labels, namespace='default'):
        pod_phase = 'Failing over'
        new_pod_node = ''

        while (pod_phase != 'Running') or (new_pod_node not in failover_targets):
            pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
            if pods:
                new_pod_node = pods[0].spec.node_name
                pod_phase = pods[0].status.phase
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_namespace_creation(self, namespace='default'):
        ns_found = False
        while ns_found != True:
            ns = self.api.core_v1.list_namespace().items
            for n in ns:
                if n.metadata.name == namespace:
                    ns_found = True
                    break
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def get_logical_backup_job(self, namespace='default'):
        return self.api.batch_v1_beta1.list_namespaced_cron_job(namespace, label_selector="application=spilo")

    def wait_for_logical_backup_job(self, expected_num_of_jobs):
        while (len(self.get_logical_backup_job().items) != expected_num_of_jobs):
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_logical_backup_job_deletion(self):
        self.wait_for_logical_backup_job(expected_num_of_jobs=0)

    def wait_for_logical_backup_job_creation(self):
        self.wait_for_logical_backup_job(expected_num_of_jobs=1)

    def delete_operator_pod(self, step="Delete operator pod"):
        # patching the pod template in the deployment restarts the operator pod
        self.api.apps_v1.patch_namespaced_deployment("postgres-operator", "default", {"spec": {"template": {"metadata": {"annotations": {"step": "{}-{}".format(step, time.time())}}}}})
        self.wait_for_operator_pod_start()

    def update_config(self, config_map_patch, step="Updating operator deployment"):
        self.api.core_v1.patch_namespaced_config_map("postgres-operator", "default", config_map_patch)
        self.delete_operator_pod(step=step)

    def patch_pod(self, data, pod_name, namespace="default"):
        self.api.core_v1.patch_namespaced_pod(pod_name, namespace, data)

    def create_with_kubectl(self, path):
        return subprocess.run(
            ["kubectl", "apply", "-f", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    def exec_with_kubectl(self, pod, cmd):
        return subprocess.run(["./exec.sh", pod, cmd],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    def patroni_rest(self, pod, path):
        r = self.exec_with_kubectl(pod, "curl localhost:8008/" + path)
        if not r.returncode == 0 or not r.stdout.decode()[0:1] == "{":
            return None

        return json.loads(r.stdout.decode())

    def get_patroni_state(self, pod):
        r = self.exec_with_kubectl(pod, "patronictl list -f json")
        if not r.returncode == 0 or not r.stdout.decode()[0:1] == "[":
            return []
        return json.loads(r.stdout.decode())

    def get_operator_state(self):
        pod = self.get_operator_pod()
        if pod is None:
            return None
        pod = pod.metadata.name

        r = self.exec_with_kubectl(pod, "curl localhost:8080/workers/all/status/")
        if not r.returncode == 0 or not r.stdout.decode()[0:1] == "{":
            return None

        return json.loads(r.stdout.decode())

    def get_patroni_running_members(self, pod="acid-minimal-cluster-0"):
        result = self.get_patroni_state(pod)
        return list(filter(lambda x: "State" in x and x["State"] == "running", result))

    def get_deployment_replica_count(self, name="acid-minimal-cluster-pooler", namespace="default"):
        try:
            deployment = self.api.apps_v1.read_namespaced_deployment(name, namespace)
            return deployment.spec.replicas
        except ApiException:
            return None

    def get_statefulset_image(self, label_selector="application=spilo,cluster-name=acid-minimal-cluster", namespace='default'):
        ssets = self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=label_selector, limit=1)
        if len(ssets.items) == 0:
            return None
        return ssets.items[0].spec.template.spec.containers[0].image

    def get_effective_pod_image(self, pod_name, namespace='default'):
        '''
        Get the Spilo image pod currently uses. In case of lazy rolling updates
        it may differ from the one specified in the stateful set.
        '''
        pod = self.api.core_v1.list_namespaced_pod(
            namespace, label_selector="statefulset.kubernetes.io/pod-name=" + pod_name)

        if len(pod.items) == 0:
            return None
        return pod.items[0].spec.containers[0].image

    def get_cluster_pod(self, role, labels='application=spilo,cluster-name=acid-minimal-cluster', namespace='default'):
        labels = labels + ',spilo-role=' + role

        pods = self.api.core_v1.list_namespaced_pod(
                namespace, label_selector=labels).items

        if pods:
            return pods[0]

    def get_cluster_leader_pod(self, labels='application=spilo,cluster-name=acid-minimal-cluster', namespace='default'):
        return self.get_cluster_pod('master', labels, namespace)

    def get_cluster_replica_pod(self, labels='application=spilo,cluster-name=acid-minimal-cluster', namespace='default'):
        return self.get_cluster_pod('replica', labels, namespace)


class K8sBase:
    '''
    K8s basic API wrapper class supposed to be inherited by other more specific classes for e2e tests
    '''

    RETRY_TIMEOUT_SEC = 1

    def __init__(self, labels='x=y', namespace='default'):
        self.api = K8sApi()
        self.labels = labels
        self.namespace = namespace

    def get_pg_nodes(self, pg_cluster_labels='cluster-name=acid-minimal-cluster', namespace='default'):
        master_pod_node = ''
        replica_pod_nodes = []
        podsList = self.api.core_v1.list_namespaced_pod(namespace, label_selector=pg_cluster_labels)
        for pod in podsList.items:
            if pod.metadata.labels.get('spilo-role') == 'master':
                master_pod_node = pod.spec.node_name
            elif pod.metadata.labels.get('spilo-role') == 'replica':
                replica_pod_nodes.append(pod.spec.node_name)

        return master_pod_node, replica_pod_nodes

    def get_cluster_nodes(self, cluster_labels='cluster-name=acid-minimal-cluster', namespace='default'):
        m = []
        r = []
        podsList = self.api.core_v1.list_namespaced_pod(namespace, label_selector=cluster_labels)
        for pod in podsList.items:
            if pod.metadata.labels.get('spilo-role') == 'master' and pod.status.phase == 'Running':
                m.append(pod.spec.node_name)
            elif pod.metadata.labels.get('spilo-role') == 'replica' and pod.status.phase == 'Running':
                r.append(pod.spec.node_name)

        return m, r

    def wait_for_operator_pod_start(self):
        self.wait_for_pod_start("name=postgres-operator")

    def get_operator_pod(self):
        pods = self.api.core_v1.list_namespaced_pod(
            'default', label_selector='name=postgres-operator'
        ).items

        if pods:
            return pods[0]

        return None

    def get_operator_log(self):
        operator_pod = self.get_operator_pod()
        pod_name = operator_pod.metadata.name
        return self.api.core_v1.read_namespaced_pod_log(
            name=pod_name,
            namespace='default'
        )

    def wait_for_pod_start(self, pod_labels, namespace='default'):
        pod_phase = 'No pod running'
        while pod_phase != 'Running':
            pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=pod_labels).items
            if pods:
                pod_phase = pods[0].status.phase

            time.sleep(self.RETRY_TIMEOUT_SEC)

    def get_service_type(self, svc_labels, namespace='default'):
        svc_type = ''
        svcs = self.api.core_v1.list_namespaced_service(namespace, label_selector=svc_labels, limit=1).items
        for svc in svcs:
            svc_type = svc.spec.type
        return svc_type

    def check_service_annotations(self, svc_labels, annotations, namespace='default'):
        svcs = self.api.core_v1.list_namespaced_service(namespace, label_selector=svc_labels, limit=1).items
        for svc in svcs:
            for key, value in annotations.items():
                if key not in svc.metadata.annotations or svc.metadata.annotations[key] != value:
                    print("Expected key {} not found in annotations {}".format(key, svc.metadata.annotation))
                    return False
        return True

    def check_statefulset_annotations(self, sset_labels, annotations, namespace='default'):
        ssets = self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=sset_labels, limit=1).items
        for sset in ssets:
            for key, value in annotations.items():
                if key not in sset.metadata.annotations or sset.metadata.annotations[key] != value:
                    print("Expected key {} not found in annotations {}".format(key, sset.metadata.annotation))
                    return False
        return True

    def scale_cluster(self, number_of_instances, name="acid-minimal-cluster", namespace="default"):
        body = {
            "spec": {
                "numberOfInstances": number_of_instances
            }
        }
        self.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", namespace, "postgresqls", name, body)

    def wait_for_running_pods(self, labels, number, namespace=''):
        while self.count_pods_with_label(labels) != number:
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_pods_to_stop(self, labels, namespace=''):
        while self.count_pods_with_label(labels) != 0:
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_service(self, labels, namespace='default'):
        def get_services():
            return self.api.core_v1.list_namespaced_service(
                namespace, label_selector=labels
            ).items

        while not get_services():
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def count_pods_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items)

    def count_services_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_service(namespace, label_selector=labels).items)

    def count_endpoints_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_endpoints(namespace, label_selector=labels).items)

    def count_secrets_with_label(self, labels, namespace='default'):
        return len(self.api.core_v1.list_namespaced_secret(namespace, label_selector=labels).items)

    def count_statefulsets_with_label(self, labels, namespace='default'):
        return len(self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=labels).items)

    def count_deployments_with_label(self, labels, namespace='default'):
        return len(self.api.apps_v1.list_namespaced_deployment(namespace, label_selector=labels).items)

    def count_pdbs_with_label(self, labels, namespace='default'):
        return len(self.api.policy_v1_beta1.list_namespaced_pod_disruption_budget(
            namespace, label_selector=labels).items)

    def count_running_pods(self, labels='application=spilo,cluster-name=acid-minimal-cluster', namespace='default'):
        pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
        return len(list(filter(lambda x: x.status.phase == 'Running', pods)))

    def count_pods_with_container_capabilities(self, capabilities, labels, namespace='default'):
        pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
        return len(list(filter(lambda x: x.spec.containers[0].security_context.capabilities.add == capabilities, pods)))

    def wait_for_pod_failover(self, failover_targets, labels, namespace='default'):
        pod_phase = 'Failing over'
        new_pod_node = ''

        while (pod_phase != 'Running') or (new_pod_node not in failover_targets):
            pods = self.api.core_v1.list_namespaced_pod(namespace, label_selector=labels).items
            if pods:
                new_pod_node = pods[0].spec.node_name
                pod_phase = pods[0].status.phase
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def get_logical_backup_job(self, namespace='default'):
        return self.api.batch_v1_beta1.list_namespaced_cron_job(namespace, label_selector="application=spilo")

    def wait_for_logical_backup_job(self, expected_num_of_jobs):
        while (len(self.get_logical_backup_job().items) != expected_num_of_jobs):
            time.sleep(self.RETRY_TIMEOUT_SEC)

    def wait_for_logical_backup_job_deletion(self):
        self.wait_for_logical_backup_job(expected_num_of_jobs=0)

    def wait_for_logical_backup_job_creation(self):
        self.wait_for_logical_backup_job(expected_num_of_jobs=1)

    def delete_operator_pod(self, step="Delete operator deplyment"):
        self.api.apps_v1.patch_namespaced_deployment("postgres-operator","default", {"spec":{"template":{"metadata":{"annotations":{"step":"{}-{}".format(step, time.time())}}}}})
        self.wait_for_operator_pod_start()

    def update_config(self, config_map_patch, step="Updating operator deployment"):
        self.api.core_v1.patch_namespaced_config_map("postgres-operator", "default", config_map_patch)
        self.delete_operator_pod(step=step)

    def create_with_kubectl(self, path):
        return subprocess.run(
            ["kubectl", "apply", "-f", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    def exec_with_kubectl(self, pod, cmd):
        return subprocess.run(["./exec.sh", pod, cmd],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    def patroni_rest(self, pod, path):
        r = self.exec_with_kubectl(pod, "curl localhost:8008/" + path)
        if not r.returncode == 0 or not r.stdout.decode()[0:1] == "{":
            return None

        return json.loads(r.stdout.decode())

    def get_patroni_state(self, pod):
        r = self.exec_with_kubectl(pod, "patronictl list -f json")
        if not r.returncode == 0 or not r.stdout.decode()[0:1] == "[":
            return []
        return json.loads(r.stdout.decode())

    def get_patroni_running_members(self, pod):
        result = self.get_patroni_state(pod)
        return list(filter(lambda x: x["State"] == "running", result))

    def get_statefulset_image(self, label_selector="application=spilo,cluster-name=acid-minimal-cluster", namespace='default'):
        ssets = self.api.apps_v1.list_namespaced_stateful_set(namespace, label_selector=label_selector, limit=1)
        if len(ssets.items) == 0:
            return None
        return ssets.items[0].spec.template.spec.containers[0].image

    def get_effective_pod_image(self, pod_name, namespace='default'):
        '''
        Get the Spilo image pod currently uses. In case of lazy rolling updates
        it may differ from the one specified in the stateful set.
        '''
        pod = self.api.core_v1.list_namespaced_pod(
            namespace, label_selector="statefulset.kubernetes.io/pod-name=" + pod_name)

        if len(pod.items) == 0:
            return None
        return pod.items[0].spec.containers[0].image


"""
  Inspiriational classes towards easier writing of end to end tests with one cluster per test case
"""


class K8sOperator(K8sBase):
    def __init__(self, labels="name=postgres-operator", namespace="default"):
        super().__init__(labels, namespace)


class K8sPostgres(K8sBase):
    def __init__(self, labels="cluster-name=acid-minimal-cluster", namespace="default"):
        super().__init__(labels, namespace)

    def get_pg_nodes(self):
        master_pod_node = ''
        replica_pod_nodes = []
        podsList = self.api.core_v1.list_namespaced_pod(self.namespace, label_selector=self.labels)
        for pod in podsList.items:
            if pod.metadata.labels.get('spilo-role') == 'master':
                master_pod_node = pod.spec.node_name
            elif pod.metadata.labels.get('spilo-role') == 'replica':
                replica_pod_nodes.append(pod.spec.node_name)

        return master_pod_node, replica_pod_nodes
