import json
import unittest
import time
import timeout_decorator
import os
import yaml

from datetime import datetime
from kubernetes import client

from tests.k8s_api import K8s
from kubernetes.client.rest import ApiException

SPILO_CURRENT = "registry.opensource.zalan.do/acid/spilo-13-e2e:0.3"
SPILO_LAZY = "registry.opensource.zalan.do/acid/spilo-13-e2e:0.4"


def to_selector(labels):
    return ",".join(["=".join(lbl) for lbl in labels.items()])


def clean_list(values):
    # value is not stripped bytes, strip and convert to a string
    clean = lambda v: v.strip().decode()
    notNone = lambda v: v

    return list(filter(notNone, map(clean, values)))


class EndToEndTestCase(unittest.TestCase):
    '''
    Test interaction of the operator with multiple K8s components.
    '''

    # `kind` pods may stuck in the `Terminating` phase for a few minutes; hence high test timeout
    TEST_TIMEOUT_SEC = 600

    def eventuallyEqual(self, f, x, m, retries=60, interval=2):
        while True:
            try:
                y = f()
                self.assertEqual(y, x, m.format(y))
                return True
            except AssertionError:
                retries = retries - 1
                if not retries > 0:
                    raise
                time.sleep(interval)

    def eventuallyNotEqual(self, f, x, m, retries=60, interval=2):
        while True:
            try:
                y = f()
                self.assertNotEqual(y, x, m.format(y))
                return True
            except AssertionError:
                retries = retries - 1
                if not retries > 0:
                    raise
                time.sleep(interval)

    def eventuallyTrue(self, f, m, retries=60, interval=2):
        while True:
            try:
                self.assertTrue(f(), m)
                return True
            except AssertionError:
                retries = retries - 1
                if not retries > 0:
                    raise
                time.sleep(interval)

    @classmethod
    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def setUpClass(cls):
        '''
        Deploy operator to a "kind" cluster created by run.sh using examples from /manifests.
        This operator deployment is to be shared among all tests.

        run.sh deletes the 'kind' cluster after successful run along with all operator-related entities.
        In the case of test failure the cluster will stay to enable manual examination;
        next invocation of "make test" will re-create it.
        '''
        print("Test Setup being executed")

        # set a single K8s wrapper for all tests
        k8s = cls.k8s = K8s()

        # remove existing local storage class and create hostpath class
        try:
            k8s.api.storage_v1_api.delete_storage_class("standard")
        except ApiException as e:
            print("Failed to delete the 'standard' storage class: {0}".format(e))

        # operator deploys pod service account there on start up
        # needed for test_multi_namespace_support()
        cls.test_namespace = "test"
        try:
            v1_namespace = client.V1Namespace(metadata=client.V1ObjectMeta(name=cls.test_namespace))
            k8s.api.core_v1.create_namespace(v1_namespace)
        except ApiException as e:
            print("Failed to create the '{0}' namespace: {1}".format(cls.test_namespace, e))

        # submit the most recent operator image built on the Docker host
        with open("manifests/postgres-operator.yaml", 'r+') as f:
            operator_deployment = yaml.safe_load(f)
            operator_deployment["spec"]["template"]["spec"]["containers"][0]["image"] = os.environ['OPERATOR_IMAGE']

        with open("manifests/postgres-operator.yaml", 'w') as f:
            yaml.dump(operator_deployment, f, Dumper=yaml.Dumper)

        with open("manifests/configmap.yaml", 'r+') as f:
            configmap = yaml.safe_load(f)
            configmap["data"]["workers"] = "1"
            configmap["data"]["docker_image"] = SPILO_CURRENT

        with open("manifests/configmap.yaml", 'w') as f:
            yaml.dump(configmap, f, Dumper=yaml.Dumper)

        for filename in ["operator-service-account-rbac.yaml",
                         "postgresql.crd.yaml",
                         "operatorconfiguration.crd.yaml",
                         "postgresteam.crd.yaml",
                         "configmap.yaml",
                         "postgres-operator.yaml",
                         "api-service.yaml",
                         "infrastructure-roles.yaml",
                         "infrastructure-roles-new.yaml",
                         "custom-team-membership.yaml",
                         "e2e-storage-class.yaml"]:
            result = k8s.create_with_kubectl("manifests/" + filename)
            print("stdout: {}, stderr: {}".format(result.stdout, result.stderr))

        k8s.wait_for_operator_pod_start()

        # reset taints and tolerations
        k8s.api.core_v1.patch_node("postgres-operator-e2e-tests-worker", {"spec": {"taints": []}})
        k8s.api.core_v1.patch_node("postgres-operator-e2e-tests-worker2", {"spec": {"taints": []}})

        # make sure we start a new operator on every new run,
        # this tackles the problem when kind is reused
        # and the Docker image is in fact changed (dirty one)

        k8s.update_config({}, step="TestSuite Startup")

        actual_operator_image = k8s.api.core_v1.list_namespaced_pod(
            'default', label_selector='name=postgres-operator').items[0].spec.containers[0].image
        print("Tested operator image: {}".format(actual_operator_image))  # shows up after tests finish

        result = k8s.create_with_kubectl("manifests/minimal-postgres-manifest.yaml")
        print('stdout: {}, stderr: {}'.format(result.stdout, result.stderr))
        try:
            k8s.wait_for_pod_start('spilo-role=master')
            k8s.wait_for_pod_start('spilo-role=replica')
        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_additional_pod_capabilities(self):
        '''
           Extend postgres container capabilities
        '''
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        capabilities = ["SYS_NICE","CHOWN"]
        patch_capabilities = {
            "data": {
                "additional_pod_capabilities": ','.join(capabilities),
            },
        }

        # get node and replica (expected target of new master)
        _, replica_nodes = self.k8s.get_pg_nodes(cluster_label)

        try:
            self.k8s.update_config(patch_capabilities)
            self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"},
                                "Operator does not get in sync")

            # changed security context of postrges container should trigger a rolling update
            self.k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)
            self.k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            self.eventuallyEqual(lambda: self.k8s.count_pods_with_container_capabilities(capabilities, cluster_label),
                                2, "Container capabilities not updated")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_additional_teams_and_members(self):
        '''
           Test PostgresTeam CRD with extra teams and members
        '''
        # enable PostgresTeam CRD and lower resync
        enable_postgres_team_crd = {
            "data": {
                "enable_postgres_team_crd": "true",
                "enable_team_member_deprecation": "true",
                "role_deletion_suffix": "_delete_me",
                "resync_period": "15s"
            },
        }
        self.k8s.update_config(enable_postgres_team_crd)
        self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresteams', 'custom-team-membership',
        {
            'spec': {
                'additionalTeams': {
                    'acid': [
                        'e2e'
                    ]
                },
                'additionalMembers': {
                    'e2e': [
                        'kind'
                    ]
                }
            }
        })

        leader = self.k8s.get_cluster_leader_pod()
        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname IN ('elephant', 'kind');
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Not all additional users found in database", 10, 5)

        # replace additional member and check if the removed member's role is renamed
        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresteams', 'custom-team-membership',
        {
            'spec': {
                'additionalMembers': {
                    'e2e': [
                        'tester'
                    ]
                },
            }
        })

        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE (rolname = 'tester' AND rolcanlogin)
                OR (rolname = 'kind_delete_me' AND NOT rolcanlogin);
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Database role of replaced member in PostgresTeam not renamed", 10, 5)

        # re-add additional member and check if the role is renamed back
        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresteams', 'custom-team-membership',
        {
            'spec': {
                'additionalMembers': {
                    'e2e': [
                        'kind'
                    ]
                },
            }
        })

        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE (rolname = 'kind' AND rolcanlogin)
                OR (rolname = 'tester_delete_me' AND NOT rolcanlogin);
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Database role of recreated member in PostgresTeam not renamed back to original name", 10, 5)

        # revert config change
        revert_resync = {
            "data": {
                "resync_period": "30m",
            },
        }
        self.k8s.update_config(revert_resync)
        self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_overwrite_pooler_deployment(self):
        self.k8s.create_with_kubectl("manifests/minimal-fake-pooler-deployment.yaml")
        self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: self.k8s.get_deployment_replica_count(name="acid-minimal-cluster-pooler"), 1,
                             "Initial broken deployment not rolled out")

        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresqls', 'acid-minimal-cluster',
        {
            'spec': {
                'enableConnectionPooler': True
            }
        })

        self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: self.k8s.get_deployment_replica_count(name="acid-minimal-cluster-pooler"), 2,
                             "Operator did not succeed in overwriting labels")

        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresqls', 'acid-minimal-cluster',
        {
            'spec': {
                'enableConnectionPooler': False
            }
        })

        self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: self.k8s.count_running_pods("connection-pooler=acid-minimal-cluster-pooler"),
                             0, "Pooler pods not scaled down")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_enable_disable_connection_pooler(self):
        '''
        For a database without connection pooler, then turns it on, scale up,
        turn off and on again. Test with different ways of doing this (via
        enableConnectionPooler or connectionPooler configuration section). At
        the end turn connection pooler off to not interfere with other tests.
        '''
        k8s = self.k8s
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': True,
                    'enableReplicaConnectionPooler': True,
                }
            })

        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(), 2,
                             "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods(
                            "connection-pooler=acid-minimal-cluster-pooler"),
                            2, "No pooler pods found")
        self.eventuallyEqual(lambda: k8s.count_running_pods(
                            "connection-pooler=acid-minimal-cluster-pooler-repl"),
                            2, "No pooler replica pods found")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(
                            'application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                            2, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label('application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             1, "Pooler secret not created")

        # Turn off only master connection pooler
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': False,
                    'enableReplicaConnectionPooler': True,
                }
            })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(name="acid-minimal-cluster-pooler-repl"), 2,
                             "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods(
                             "connection-pooler=acid-minimal-cluster-pooler"),
                             0, "Master pooler pods not deleted")
        self.eventuallyEqual(lambda: k8s.count_running_pods(
                             "connection-pooler=acid-minimal-cluster-pooler-repl"),
                             2, "Pooler replica pods not found")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(
                             'application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             1, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label('application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             1, "Secret not created")

        # Turn off only replica connection pooler
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': True,
                    'enableReplicaConnectionPooler': False,
                }
            })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(), 2,
                             "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods("connection-pooler=acid-minimal-cluster-pooler"),
                             2, "Master pooler pods not found")
        self.eventuallyEqual(lambda: k8s.count_running_pods("connection-pooler=acid-minimal-cluster-pooler-repl"),
                             0, "Pooler replica pods not deleted")
        self.eventuallyEqual(lambda: k8s.count_services_with_label('application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             1, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label('application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             1, "Secret not created")

        # scale up connection pooler deployment
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'connectionPooler': {
                        'numberOfInstances': 3,
                    },
                }
            })

        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(), 3,
                             "Deployment replicas is scaled to 3")
        self.eventuallyEqual(lambda: k8s.count_running_pods("connection-pooler=acid-minimal-cluster-pooler"),
                             3, "Scale up of pooler pods does not work")

        # turn it off, keeping config should be overwritten by false
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': False,
                    'enableReplicaConnectionPooler': False,
                }
            })

        self.eventuallyEqual(lambda: k8s.count_running_pods("connection-pooler=acid-minimal-cluster-pooler"),
                             0, "Pooler pods not scaled down")
        self.eventuallyEqual(lambda: k8s.count_services_with_label('application=db-connection-pooler,cluster-name=acid-minimal-cluster'),
                             0, "Pooler service not removed")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label('application=spilo,cluster-name=acid-minimal-cluster'),
                             4, "Secrets not deleted")

        # Verify that all the databases have pooler schema installed.
        # Do this via psql, since otherwise we need to deal with
        # credentials.
        db_list = []

        leader = k8s.get_cluster_leader_pod()
        schemas_query = """
            SELECT schema_name
              FROM information_schema.schemata
             WHERE schema_name = 'pooler'
        """

        db_list = self.list_databases(leader.metadata.name)
        for db in db_list:
            self.eventuallyNotEqual(lambda: len(self.query_database(leader.metadata.name, db, schemas_query)), 0,
                "Pooler schema not found in database {}".format(db))

        # remove config section to make test work next time
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'connectionPooler': None,
                    'EnableReplicaConnectionPooler': False,
                }
            })

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_enable_load_balancer(self):
        '''
        Test if services are updated when enabling/disabling load balancers in Postgres manifest
        '''

        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster,spilo-role={}'

        self.eventuallyEqual(lambda: k8s.get_service_type(cluster_label.format("master")),
                             'ClusterIP',
                             "Expected ClusterIP type initially, found {}")

        try:
            # enable load balancer services
            pg_patch_enable_lbs = {
                "spec": {
                    "enableMasterLoadBalancer": True,
                    "enableReplicaLoadBalancer": True
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_enable_lbs)

            self.eventuallyEqual(lambda: k8s.get_service_type(cluster_label.format("master")),
                                 'LoadBalancer',
                                 "Expected LoadBalancer service type for master, found {}")

            self.eventuallyEqual(lambda: k8s.get_service_type(cluster_label.format("replica")),
                                 'LoadBalancer',
                                 "Expected LoadBalancer service type for master, found {}")

            # disable load balancer services again
            pg_patch_disable_lbs = {
                "spec": {
                    "enableMasterLoadBalancer": False,
                    "enableReplicaLoadBalancer": False
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_disable_lbs)

            self.eventuallyEqual(lambda: k8s.get_service_type(cluster_label.format("master")),
                                 'ClusterIP',
                                 "Expected LoadBalancer service type for master, found {}")

            self.eventuallyEqual(lambda: k8s.get_service_type(cluster_label.format("replica")),
                                 'ClusterIP',
                                 "Expected LoadBalancer service type for master, found {}")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_infrastructure_roles(self):
        '''
            Test using external secrets for infrastructure roles
        '''
        k8s = self.k8s
        # update infrastructure roles description
        secret_name = "postgresql-infrastructure-roles"
        roles = "secretname: postgresql-infrastructure-roles-new, userkey: user,"\
                "rolekey: memberof, passwordkey: password, defaultrolevalue: robot_zmon"
        patch_infrastructure_roles = {
            "data": {
                "infrastructure_roles_secret_name": secret_name,
                "infrastructure_roles_secrets": roles,
            },
        }
        k8s.update_config(patch_infrastructure_roles)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        try:
            # check that new roles are represented in the config by requesting the
            # operator configuration via API

            def verify_role():
                try:
                    operator_pod = k8s.get_operator_pod()
                    get_config_cmd = "wget --quiet -O - localhost:8080/config"
                    result = k8s.exec_with_kubectl(operator_pod.metadata.name,
                                                   get_config_cmd)
                    try:
                        roles_dict = (json.loads(result.stdout)
                                          .get("controller", {})
                                          .get("InfrastructureRoles"))
                    except:
                        return False

                    if "robot_zmon_acid_monitoring_new" in roles_dict:
                        role = roles_dict["robot_zmon_acid_monitoring_new"]
                        role.pop("Password", None)
                        self.assertDictEqual(role, {
                            "Name": "robot_zmon_acid_monitoring_new",
                            "Namespace":"",
                            "Flags": None,
                            "MemberOf": ["robot_zmon"],
                            "Parameters": None,
                            "AdminRole": "",
                            "Origin": 2,
                            "Deleted": False
                        })
                        return True
                except:
                    pass

                return False

            self.eventuallyTrue(verify_role, "infrastructure role setup is not loaded")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_zz_cross_namespace_secrets(self):
        '''
            Test secrets in different namespace
        '''
        app_namespace = "appspace"

        v1_appnamespace = client.V1Namespace(metadata=client.V1ObjectMeta(name=app_namespace))
        self.k8s.api.core_v1.create_namespace(v1_appnamespace)
        self.k8s.wait_for_namespace_creation(app_namespace)

        patch_cross_namespace_secret = {
            "data": {
                "enable_cross_namespace_secret": "true"
            }
        }
        self.k8s.update_config(patch_cross_namespace_secret,
                          step="cross namespace secrets enabled")

        self.k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'users':{
                        'appspace.db_user': [],
                    }
                }
            })

        self.eventuallyEqual(lambda: self.k8s.count_secrets_with_label("cluster-name=acid-minimal-cluster,application=spilo", app_namespace),
                             1, "Secret not created for user in namespace")

        #reset the flag
        unpatch_cross_namespace_secret = {
                "data": {
                    "enable_cross_namespace_secret": "false",
                }
            }
        self.k8s.update_config(unpatch_cross_namespace_secret, step="disable cross namespace secrets")


    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_lazy_spilo_upgrade(self):
        '''
        Test lazy upgrade for the Spilo image: operator changes a stateful set
        but lets pods run with the old image until they are recreated for
        reasons other than operator's activity. That works because the operator
        configures stateful sets to use "onDelete" pod update policy.

        The test covers:
        1) enabling lazy upgrade in existing operator deployment
        2) forcing the normal rolling upgrade by changing the operator
        configmap and restarting its pod
        '''

        k8s = self.k8s

        pod0 = 'acid-minimal-cluster-0'
        pod1 = 'acid-minimal-cluster-1'

        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                             "No 2 pods running")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                             2, "Postgres status did not enter running")

        patch_lazy_spilo_upgrade = {
            "data": {
                "docker_image": SPILO_CURRENT,
                "enable_lazy_spilo_upgrade": "false"
            }
        }
        k8s.update_config(patch_lazy_spilo_upgrade,
                          step="Init baseline image version")

        self.eventuallyEqual(lambda: k8s.get_statefulset_image(), SPILO_CURRENT,
                             "Statefulset not updated initially")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                             "No 2 pods running")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                             2, "Postgres status did not enter running")

        self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod0),
                             SPILO_CURRENT, "Rolling upgrade was not executed")
        self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod1),
                             SPILO_CURRENT, "Rolling upgrade was not executed")

        # update docker image in config and enable the lazy upgrade
        conf_image = SPILO_LAZY
        patch_lazy_spilo_upgrade = {
            "data": {
                "docker_image": conf_image,
                "enable_lazy_spilo_upgrade": "true"
            }
        }
        k8s.update_config(patch_lazy_spilo_upgrade,
                          step="patch image and lazy upgrade")
        self.eventuallyEqual(lambda: k8s.get_statefulset_image(), conf_image,
                             "Statefulset not updated to next Docker image")

        try:
            # restart the pod to get a container with the new image
            k8s.api.core_v1.delete_namespaced_pod(pod0, 'default')

            # verify only pod-0 which was deleted got new image from statefulset
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod0),
                                 conf_image, "Delete pod-0 did not get new spilo image")
            self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                                 "No two pods running after lazy rolling upgrade")
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                                 2, "Postgres status did not enter running")
            self.assertNotEqual(lambda: k8s.get_effective_pod_image(pod1),
                                SPILO_CURRENT,
                                "pod-1 should not have change Docker image to {}".format(SPILO_CURRENT))

            # clean up
            unpatch_lazy_spilo_upgrade = {
                "data": {
                    "enable_lazy_spilo_upgrade": "false",
                }
            }
            k8s.update_config(unpatch_lazy_spilo_upgrade, step="patch lazy upgrade")

            # at this point operator will complete the normal rolling upgrade
            # so we additionally test if disabling the lazy upgrade - forcing the normal rolling upgrade - works
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod0),
                                 conf_image, "Rolling upgrade was not executed",
                                 50, 3)
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod1),
                                 conf_image, "Rolling upgrade was not executed",
                                 50, 3)
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                                 2, "Postgres status did not enter running")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_logical_backup_cron_job(self):
        '''
        Ensure we can (a) create the cron job at user request for a specific PG cluster
                      (b) update the cluster-wide image for the logical backup pod
                      (c) delete the job at user request

        Limitations:
        (a) Does not run the actual batch job because there is no S3 mock to upload backups to
        (b) Assumes 'acid-minimal-cluster' exists as defined in setUp
        '''

        k8s = self.k8s

        # create the cron job
        schedule = "7 7 7 7 *"
        pg_patch_enable_backup = {
            "spec": {
                "enableLogicalBackup": True,
                "logicalBackupSchedule": schedule
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_enable_backup)

        try:
            self.eventuallyEqual(lambda: len(k8s.get_logical_backup_job().items), 1, "failed to create logical backup job")

            job = k8s.get_logical_backup_job().items[0]
            self.assertEqual(job.metadata.name, "logical-backup-acid-minimal-cluster",
                             "Expected job name {}, found {}"
                             .format("logical-backup-acid-minimal-cluster", job.metadata.name))
            self.assertEqual(job.spec.schedule, schedule,
                             "Expected {} schedule, found {}"
                             .format(schedule, job.spec.schedule))

            # update the cluster-wide image of the logical backup pod
            image = "test-image-name"
            patch_logical_backup_image = {
                "data": {
                    "logical_backup_docker_image": image,
                }
            }
            k8s.update_config(patch_logical_backup_image, step="patch logical backup image")

            def get_docker_image():
                jobs = k8s.get_logical_backup_job().items
                return jobs[0].spec.job_template.spec.template.spec.containers[0].image

            self.eventuallyEqual(get_docker_image, image,
                                 "Expected job image {}, found {}".format(image, "{}"))

            # delete the logical backup cron job
            pg_patch_disable_backup = {
                "spec": {
                    "enableLogicalBackup": False,
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_disable_backup)

            self.eventuallyEqual(lambda: len(k8s.get_logical_backup_job().items), 0, "failed to create logical backup job")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # ensure cluster is healthy after tests
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_min_resource_limits(self):
        '''
        Lower resource limits below configured minimum and let operator fix it
        '''
        k8s = self.k8s
        # self.eventuallyEqual(lambda: k8s.pg_get_status(), "Running", "Cluster not healthy at start")

        # configure minimum boundaries for CPU and memory limits
        minCPULimit = '503m'
        minMemoryLimit = '502Mi'

        patch_min_resource_limits = {
            "data": {
                "min_cpu_limit": minCPULimit,
                "min_memory_limit": minMemoryLimit
            }
        }
        k8s.update_config(patch_min_resource_limits, "Minimum resource test")

        # lower resource limits below minimum
        pg_patch_resources = {
            "spec": {
                "resources": {
                    "requests": {
                        "cpu": "10m",
                        "memory": "50Mi"
                    },
                    "limits": {
                        "cpu": "200m",
                        "memory": "200Mi"
                    }
                }
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_resources)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No two pods running after lazy rolling upgrade")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members()), 2, "Postgres status did not enter running")

        def verify_pod_limits():
            pods = k8s.api.core_v1.list_namespaced_pod('default', label_selector="cluster-name=acid-minimal-cluster,application=spilo").items
            if len(pods) < 2:
                return False

            r = pods[0].spec.containers[0].resources.limits['memory'] == minMemoryLimit
            r = r and pods[0].spec.containers[0].resources.limits['cpu'] == minCPULimit
            r = r and pods[1].spec.containers[0].resources.limits['memory'] == minMemoryLimit
            r = r and pods[1].spec.containers[0].resources.limits['cpu'] == minCPULimit
            return r

        self.eventuallyTrue(verify_pod_limits, "Pod limits where not adjusted")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_zzz_multi_namespace_support(self):
        '''
        Create a customized Postgres cluster in a non-default namespace.
        '''
        k8s = self.k8s

        with open("manifests/complete-postgres-manifest.yaml", 'r+') as f:
            pg_manifest = yaml.safe_load(f)
            pg_manifest["metadata"]["namespace"] = self.test_namespace
            yaml.dump(pg_manifest, f, Dumper=yaml.Dumper)

        try:
            k8s.create_with_kubectl("manifests/complete-postgres-manifest.yaml")
            k8s.wait_for_pod_start("spilo-role=master", self.test_namespace)
            k8s.wait_for_pod_start('spilo-role=replica', self.test_namespace)
            self.assert_master_is_unique(self.test_namespace, "acid-test-cluster")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise
        finally:
            # delete the new cluster so that the k8s_api.get_operator_state works correctly in subsequent tests
            # ideally we should delete the 'test' namespace here but
            # the pods inside the namespace stuck in the Terminating state making the test time out
            k8s.api.custom_objects_api.delete_namespaced_custom_object(
                "acid.zalan.do", "v1", self.test_namespace, "postgresqls", "acid-test-cluster")
            time.sleep(5)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_rolling_update_flag(self):
        '''
            Add rolling update flag to only the master and see it failing over
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")

        # get node and replica (expected target of new master)
        _, replica_nodes = k8s.get_pg_nodes(cluster_label)

        # rolling update annotation
        flag = {
            "metadata": {
                "annotations": {
                    "zalando-postgres-operator-rolling-update-required": "true",
                }
            }
        }

        try:
            podsList = k8s.api.core_v1.list_namespaced_pod('default', label_selector=cluster_label)
            for pod in podsList.items:
                # add flag only to the master to make it appear to the operator as a leftover from a rolling update
                if pod.metadata.labels.get('spilo-role') == 'master':
                    old_creation_timestamp = pod.metadata.creation_timestamp
                    k8s.patch_pod(flag, pod.metadata.name, pod.metadata.namespace)
                else:
                    # remember replica name to check if operator does a switchover
                    switchover_target = pod.metadata.name

            # do not wait until the next sync
            k8s.delete_operator_pod()

            # operator should now recreate the master pod and do a switchover before
            k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)

            # check if the former replica is now the new master
            leader = k8s.get_cluster_leader_pod()
            self.eventuallyEqual(lambda: leader.metadata.name, switchover_target, "Rolling update flag did not trigger switchover")

            # check that the old master has been recreated
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
            replica = k8s.get_cluster_replica_pod()
            self.assertTrue(replica.metadata.creation_timestamp > old_creation_timestamp, "Old master pod was not recreated")


        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_rolling_update_label_timeout(self):
        '''
            Simulate case when replica does not receive label in time and rolling update does not finish
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        flag = "zalando-postgres-operator-rolling-update-required"

        # verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")

        # get node and replica (expected target of new master)
        _, replica_nodes = k8s.get_pg_nodes(cluster_label)

        # rolling update annotation
        rolling_update_patch = {
            "metadata": {
                "annotations": {
                    flag: "true",
                }
            }
        }

        # make pod_label_wait_timeout so short that rolling update fails on first try
        # temporarily lower resync interval to reduce waiting for further tests
        # pods should get healthy in the meantime
        patch_resync_config = {
            "data": {
                "pod_label_wait_timeout": "2s",
                "resync_period": "20s",
            }
        }

        try:
            # patch both pods for rolling update
            podList = k8s.api.core_v1.list_namespaced_pod('default', label_selector=cluster_label)
            for pod in podList.items:
                k8s.patch_pod(rolling_update_patch, pod.metadata.name, pod.metadata.namespace)
                if pod.metadata.labels.get('spilo-role') == 'replica':
                    switchover_target = pod.metadata.name

            # update config and restart operator
            k8s.update_config(patch_resync_config, "update resync interval and pod_label_wait_timeout")

            # operator should now recreate the replica pod first and do a switchover after
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # pod_label_wait_timeout should have been exceeded hence the rolling update is continued on next sync
            # check if the cluster state is "SyncFailed"
            self.eventuallyEqual(lambda: k8s.pg_get_status(), "SyncFailed", "Expected SYNC event to fail")

            # wait for next sync, replica should be running normally by now and be ready for switchover
            k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)

            # check if the former replica is now the new master
            leader = k8s.get_cluster_leader_pod()
            self.eventuallyEqual(lambda: leader.metadata.name, switchover_target, "Rolling update flag did not trigger switchover")

            # wait for the old master to get restarted
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # status should again be "SyncFailed" but turn into "Running" on the next sync
            time.sleep(10)
            self.eventuallyEqual(lambda: k8s.pg_get_status(), "Running", "Expected running cluster after two syncs")

            # revert config changes
            patch_resync_config = {
                "data": {
                    "pod_label_wait_timeout": "10m",
                    "resync_period": "30m",
                }
            }
            k8s.update_config(patch_resync_config, "revert resync interval and pod_label_wait_timeout")


        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_zz_node_readiness_label(self):
        '''
           Remove node readiness label from master node. This must cause a failover.
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        readiness_label = 'lifecycle-status'
        readiness_value = 'ready'

        try:
            # get nodes of master and replica(s) (expected target of new master)
            current_master_node, current_replica_nodes = k8s.get_pg_nodes(cluster_label)
            num_replicas = len(current_replica_nodes)
            failover_targets = self.get_failover_targets(current_master_node, current_replica_nodes)

            # add node_readiness_label to potential failover nodes
            patch_readiness_label = {
                "metadata": {
                    "labels": {
                        readiness_label: readiness_value
                    }
                }
            }
            self.assertTrue(len(failover_targets) > 0, "No failover targets available")
            for failover_target in failover_targets:
                k8s.api.core_v1.patch_node(failover_target, patch_readiness_label)

            # define node_readiness_label in config map which should trigger a failover of the master
            patch_readiness_label_config = {
                "data": {
                    "node_readiness_label": readiness_label + ':' + readiness_value,
                }
            }
            k8s.update_config(patch_readiness_label_config, "setting readiness label")
            new_master_node, new_replica_nodes = self.assert_failover(
                current_master_node, num_replicas, failover_targets, cluster_label)

            # patch also node where master ran before
            k8s.api.core_v1.patch_node(current_master_node, patch_readiness_label)

            # toggle pod anti affinity to move replica away from master node
            self.eventuallyTrue(lambda: self.assert_distributed_pods(new_master_node, new_replica_nodes, cluster_label), "Pods are redistributed")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_scaling(self):
        '''
           Scale up from 2 to 3 and back to 2 pods by updating the Postgres manifest at runtime.
        '''
        k8s = self.k8s
        pod = "acid-minimal-cluster-0"

        k8s.scale_cluster(3)
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 3, "Scale up to 3 failed")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod)), 3, "Not all 3 nodes healthy")

        k8s.scale_cluster(2)
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "Scale down to 2 failed")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod)), 2, "Not all members 2 healthy")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_service_annotations(self):
        '''
            Create a Postgres cluster with service annotations and check them.
        '''
        k8s = self.k8s
        patch_custom_service_annotations = {
            "data": {
                "custom_service_annotations": "foo:bar",
            }
        }
        k8s.update_config(patch_custom_service_annotations)

        pg_patch_custom_annotations = {
            "spec": {
                "serviceAnnotations": {
                    "annotation.key": "value",
                    "alice": "bob",
                }
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_custom_annotations)

        annotations = {
            "annotation.key": "value",
            "foo": "bar",
            "alice": "bob"
        }

        self.eventuallyTrue(lambda: k8s.check_service_annotations("cluster-name=acid-minimal-cluster,spilo-role=master", annotations), "Wrong annotations")
        self.eventuallyTrue(lambda: k8s.check_service_annotations("cluster-name=acid-minimal-cluster,spilo-role=replica", annotations), "Wrong annotations")

        # clean up
        unpatch_custom_service_annotations = {
            "data": {
                "custom_service_annotations": "",
            }
        }
        k8s.update_config(unpatch_custom_service_annotations)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_statefulset_annotation_propagation(self):
        '''
           Inject annotation to Postgresql CRD and check it's propagation to stateful set
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        patch_sset_propagate_annotations = {
            "data": {
                "downscaler_annotations": "deployment-time,downscaler/*",
                "inherited_annotations": "owned-by",
            }
        }
        k8s.update_config(patch_sset_propagate_annotations)

        pg_crd_annotations = {
            "metadata": {
                "annotations": {
                    "deployment-time": "2020-04-30 12:00:00",
                    "downscaler/downtime_replicas": "0",
                    "owned-by": "acid",
                },
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_crd_annotations)

        annotations = {
            "deployment-time": "2020-04-30 12:00:00",
            "downscaler/downtime_replicas": "0",
            "owned-by": "acid",
        }
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyTrue(lambda: k8s.check_statefulset_annotations(cluster_label, annotations), "Annotations missing")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    @unittest.skip("Skipping this test until fixed")
    def test_zaa_test_major_version_upgrade(self):
        k8s = self.k8s
        result = k8s.create_with_kubectl("manifests/minimal-postgres-manifest-12.yaml")
        self.eventuallyEqual(lambda: k8s.count_running_pods(labels="application=spilo,cluster-name=acid-upgrade-test"), 2, "No 2 pods running")
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        pg_patch_version = {
            "spec": {
                "postgres": {
                    "version": "13"
                }
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version)

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        def check_version_13():
            p = k8s.get_patroni_state("acid-upgrade-test-0")
            version = p["server_version"][0:2]
            return version

        self.evantuallyEqual(check_version_13, "13", "Version was not upgrade to 13")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    @unittest.skip("Skipping this test until fixed")
    def test_zzz_taint_based_eviction(self):
        '''
           Add taint "postgres=:NoExecute" to node with master. This must cause a failover.
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

        # get nodes of master and replica(s) (expected target of new master)
        master_nodes, replica_nodes = k8s.get_cluster_nodes()

        self.assertNotEqual(master_nodes, [])
        self.assertNotEqual(replica_nodes, [])

        # taint node with postgres=:NoExecute to force failover
        body = {
            "spec": {
                "taints": [
                    {
                        "effect": "NoExecute",
                        "key": "postgres"
                    }
                ]
            }
        }

        k8s.api.core_v1.patch_node(master_nodes[0], body)
        self.eventuallyTrue(lambda: k8s.get_cluster_nodes()[0], replica_nodes)
        self.assertNotEqual(lambda: k8s.get_cluster_nodes()[0], master_nodes)

        # add toleration to pods
        patch_toleration_config = {
            "data": {
                "toleration": "key:postgres,operator:Exists,effect:NoExecute"
            }
        }

        k8s.update_config(patch_toleration_config, step="allow tainted nodes")

        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

        # toggle pod anti affinity to move replica away from master node
        nm, new_replica_nodes = k8s.get_cluster_nodes()
        new_master_node = nm[0]
        self.assert_distributed_pods(new_master_node, new_replica_nodes, cluster_label)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    @unittest.skip("Skipping this test until fixed")
    def test_node_affinity(self):
        '''
           Add label to a node and update postgres cluster spec to deploy only on a node with that label
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")
        self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

        # get nodes of master and replica(s)
        master_node, replica_nodes = k8s.get_pg_nodes(cluster_label)

        self.assertNotEqual(master_node, [])
        self.assertNotEqual(replica_nodes, [])

        # label node with environment=postgres
        node_label_body = {
            "metadata": {
                "labels": {
                    "node-affinity-test": "postgres"
                }
            }
        }

        try:
            # patch current master node with the label
            print('patching master node: {}'.format(master_node))
            k8s.api.core_v1.patch_node(master_node, node_label_body)

            # add node affinity to cluster
            patch_node_affinity_config = {
                "spec": {
                    "nodeAffinity" : {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "node-affinity-test",
                                            "operator": "In",
                                            "values": [
                                                "postgres"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            }

            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                group="acid.zalan.do",
                version="v1",
                namespace="default",
                plural="postgresqls",
                name="acid-minimal-cluster",
                body=patch_node_affinity_config)
            self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # node affinity change should cause replica to relocate from replica node to master node due to node affinity requirement
            k8s.wait_for_pod_failover(master_node, 'spilo-role=replica,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            podsList = k8s.api.core_v1.list_namespaced_pod('default', label_selector=cluster_label)
            for pod in podsList.items:
                if pod.metadata.labels.get('spilo-role') == 'replica':
                    self.assertEqual(master_node, pod.spec.node_name,
                         "Sanity check: expected replica to relocate to master node {}, but found on {}".format(master_node, pod.spec.node_name))

                    # check that pod has correct node affinity
                    key = pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms[0].match_expressions[0].key
                    value = pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms[0].match_expressions[0].values[0]
                    self.assertEqual("node-affinity-test", key,
                        "Sanity check: expect node selector key to be equal to 'node-affinity-test' but got {}".format(key))
                    self.assertEqual("postgres", value,
                        "Sanity check: expect node selector value to be equal to 'postgres' but got {}".format(value))

            patch_node_remove_affinity_config = {
                "spec": {
                    "nodeAffinity" : None
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                group="acid.zalan.do",
                version="v1",
                namespace="default",
                plural="postgresqls",
                name="acid-minimal-cluster",
                body=patch_node_remove_affinity_config)
            self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

            # remove node affinity to move replica away from master node
            nm, new_replica_nodes = k8s.get_cluster_nodes()
            new_master_node = nm[0]
            self.assert_distributed_pods(new_master_node, new_replica_nodes, cluster_label)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_zzzz_cluster_deletion(self):
        '''
           Test deletion with configured protection
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # configure delete protection
        patch_delete_annotations = {
            "data": {
                "delete_annotation_date_key": "delete-date",
                "delete_annotation_name_key": "delete-clustername"
            }
        }
        k8s.update_config(patch_delete_annotations)
        time.sleep(25)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        try:
            # this delete attempt should be omitted because of missing annotations
            k8s.api.custom_objects_api.delete_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster")
            time.sleep(15)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # check that pods and services are still there
            k8s.wait_for_running_pods(cluster_label, 2)
            k8s.wait_for_service(cluster_label)

            # recreate Postgres cluster resource
            k8s.create_with_kubectl("manifests/minimal-postgres-manifest.yaml")

            # wait a little before proceeding
            time.sleep(10)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # add annotations to manifest
            delete_date = datetime.today().strftime('%Y-%m-%d')
            pg_patch_delete_annotations = {
                "metadata": {
                    "annotations": {
                        "delete-date": delete_date,
                        "delete-clustername": "acid-minimal-cluster",
                    }
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_delete_annotations)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # wait a little before proceeding
            time.sleep(20)
            k8s.wait_for_running_pods(cluster_label, 2)
            k8s.wait_for_service(cluster_label)

            # now delete process should be triggered
            k8s.api.custom_objects_api.delete_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster")

            self.eventuallyEqual(lambda: len(k8s.api.custom_objects_api.list_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", label_selector="cluster-name=acid-minimal-cluster")["items"]), 0, "Manifest not deleted")

            # check if everything has been deleted
            self.eventuallyEqual(lambda: k8s.count_pods_with_label(cluster_label), 0, "Pods not deleted")
            self.eventuallyEqual(lambda: k8s.count_services_with_label(cluster_label), 0, "Service not deleted")
            self.eventuallyEqual(lambda: k8s.count_endpoints_with_label(cluster_label), 0, "Endpoints not deleted")
            self.eventuallyEqual(lambda: k8s.count_statefulsets_with_label(cluster_label), 0, "Statefulset not deleted")
            self.eventuallyEqual(lambda: k8s.count_deployments_with_label(cluster_label), 0, "Deployments not deleted")
            self.eventuallyEqual(lambda: k8s.count_pdbs_with_label(cluster_label), 0, "Pod disruption budget not deleted")
            self.eventuallyEqual(lambda: k8s.count_secrets_with_label(cluster_label), 0, "Secrets not deleted")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # reset configmap
        patch_delete_annotations = {
            "data": {
                "delete_annotation_date_key": "",
                "delete_annotation_name_key": ""
            }
        }
        k8s.update_config(patch_delete_annotations)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_patroni_config_update(self):
        '''
            Change Postgres config under Spec.Postgresql.Parameters and Spec.Patroni
            and query Patroni config endpoint to check if manifest changes got applied
            via restarting cluster through Patroni's rest api
        '''
        k8s = self.k8s
        masterPod = k8s.get_cluster_leader_pod()
        labels = 'application=spilo,cluster-name=acid-minimal-cluster,spilo-role=master'
        creationTimestamp = masterPod.metadata.creation_timestamp
        new_max_connections_value = "50"

        # adjust max_connection
        pg_patch_config = {
            "spec": {
                "postgresql": {
                    "parameters": {
                        "max_connections": new_max_connections_value
                     }
                 },
                 "patroni": {
                    "slots": {
                        "test_slot": {
                            "type": "physical"
                        }
                    },
                    "ttl": 29,
                    "loop_wait": 9,
                    "retry_timeout": 9,
                    "synchronous_mode": True
                 }
            }
        }

        try:
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_config)
            
            self.eventuallyEqual(lambda: self.k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            def compare_config():
                effective_config = k8s.patroni_rest(masterPod.metadata.name, "config")
                desired_patroni = pg_patch_config["spec"]["patroni"]
                desired_parameters = pg_patch_config["spec"]["postgresql"]["parameters"]
                effective_parameters = effective_config["postgresql"]["parameters"]
                self.assertEqual(desired_parameters["max_connections"], effective_parameters["max_connections"],
                            "max_connections not updated")
                self.assertTrue(effective_config["slots"] is not None, "physical replication slot not added")
                self.assertEqual(desired_patroni["ttl"], effective_config["ttl"],
                            "ttl not updated")
                self.assertEqual(desired_patroni["loop_wait"], effective_config["loop_wait"],
                            "loop_wait not updated")
                self.assertEqual(desired_patroni["retry_timeout"], effective_config["retry_timeout"],
                            "retry_timeout not updated")
                self.assertEqual(desired_patroni["synchronous_mode"], effective_config["synchronous_mode"],
                            "synchronous_mode not updated")
                return True

            self.eventuallyTrue(compare_config, "Postgres config not applied")

            setting_query = """
               SELECT setting
                 FROM pg_settings
                WHERE name = 'max_connections';
            """
            self.eventuallyEqual(lambda: self.query_database(masterPod.metadata.name, "postgres", setting_query)[0], new_max_connections_value,
                "New max_connections setting not applied", 10, 5)

            # make sure that pod wasn't recreated
            self.assertEqual(creationTimestamp, masterPod.metadata.creation_timestamp,
                            "Master pod creation timestamp is updated")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # make sure cluster is in a good state for further tests
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                             "No 2 pods running")

    def get_failover_targets(self, master_node, replica_nodes):
        '''
           If all pods live on the same node, failover will happen to other worker(s)
        '''
        k8s = self.k8s
        k8s_master_exclusion = 'kubernetes.io/hostname!=postgres-operator-e2e-tests-control-plane'

        failover_targets = [x for x in replica_nodes if x != master_node]
        if len(failover_targets) == 0:
            nodes = k8s.api.core_v1.list_node(label_selector=k8s_master_exclusion)
            for n in nodes.items:
                if n.metadata.name != master_node:
                    failover_targets.append(n.metadata.name)

        return failover_targets

    def assert_failover(self, current_master_node, num_replicas, failover_targets, cluster_label):
        '''
           Check if master is failing over. The replica should move first to be the switchover target
        '''
        k8s = self.k8s
        k8s.wait_for_pod_failover(failover_targets, 'spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

        new_master_node, new_replica_nodes = k8s.get_pg_nodes(cluster_label)
        self.assertNotEqual(current_master_node, new_master_node,
                            "Master on {} did not fail over to one of {}".format(current_master_node, failover_targets))
        self.assertEqual(num_replicas, len(new_replica_nodes),
                         "Expected {} replicas, found {}".format(num_replicas, len(new_replica_nodes)))
        self.assert_master_is_unique()

        return new_master_node, new_replica_nodes

    def assert_master_is_unique(self, namespace='default', clusterName="acid-minimal-cluster"):
        '''
           Check that there is a single pod in the k8s cluster with the label "spilo-role=master"
           To be called manually after operations that affect pods
        '''
        k8s = self.k8s
        labels = 'spilo-role=master,cluster-name=' + clusterName

        num_of_master_pods = k8s.count_pods_with_label(labels, namespace)
        self.assertEqual(num_of_master_pods, 1, "Expected 1 master pod, found {}".format(num_of_master_pods))

    def assert_distributed_pods(self, master_node, replica_nodes, cluster_label):
        '''
           Other tests can lead to the situation that master and replica are on the same node.
           Toggle pod anti affinty to distribute pods accross nodes (replica in particular).
        '''
        k8s = self.k8s
        failover_targets = self.get_failover_targets(master_node, replica_nodes)

        # enable pod anti affintiy in config map which should trigger movement of replica
        patch_enable_antiaffinity = {
            "data": {
                "enable_pod_antiaffinity": "true"
            }
        }
        k8s.update_config(patch_enable_antiaffinity, "enable antiaffinity")
        self.assert_failover(master_node, len(replica_nodes), failover_targets, cluster_label)

        # now disable pod anti affintiy again which will cause yet another failover
        patch_disable_antiaffinity = {
            "data": {
                "enable_pod_antiaffinity": "false"
            }
        }
        k8s.update_config(patch_disable_antiaffinity, "disable antiaffinity")
        k8s.wait_for_pod_start('spilo-role=master')
        k8s.wait_for_pod_start('spilo-role=replica')
        return True

    def list_databases(self, pod_name):
        '''
           Get list of databases we might want to iterate over
        '''
        k8s = self.k8s
        result_set = []
        db_list = []
        db_list_query = "SELECT datname FROM pg_database"
        exec_query = r"psql -tAq -c \"{}\" -d {}"

        try:
            q = exec_query.format(db_list_query, "postgres")
            q = "su postgres -c \"{}\"".format(q)
            print('Get databases: {}'.format(q))
            result = k8s.exec_with_kubectl(pod_name, q)
            db_list = clean_list(result.stdout.split(b'\n'))
            print('db_list: {}, stdout: {}, stderr {}'.format(
                db_list, result.stdout, result.stderr
            ))
        except Exception as ex:
            print('Could not get databases: {}'.format(ex))
            print('Stdout: {}'.format(result.stdout))
            print('Stderr: {}'.format(result.stderr))

        for db in db_list:
            if db in ('template0', 'template1'):
                continue
            result_set.append(db)

        return result_set

    def query_database(self, pod_name, db_name, query):
        '''
           Query database and return result as a list
        '''
        k8s = self.k8s
        result_set = []
        exec_query = r"psql -tAq -c \"{}\" -d {}"

        try:
            q = exec_query.format(query, db_name)
            q = "su postgres -c \"{}\"".format(q)
            print('Send query: {}'.format(q))
            result = k8s.exec_with_kubectl(pod_name, q)
            result_set = clean_list(result.stdout.split(b'\n'))
            print('result: {}, stdout: {}, stderr {}'.format(
                result_set, result.stdout, result.stderr
            ))
        except Exception as ex:
            print('Error on query execution: {}'.format(ex))
            print('Stdout: {}'.format(result.stdout))
            print('Stderr: {}'.format(result.stderr))

        return result_set

if __name__ == '__main__':
    unittest.main()
