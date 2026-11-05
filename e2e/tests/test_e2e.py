import json
import unittest
import time
import timeout_decorator
import os
import yaml
import base64

from datetime import datetime, date, timedelta
from kubernetes import client

from tests.k8s_api import K8s
from kubernetes.client.rest import ApiException

SPILO_CURRENT = "ghcr.io/zalando/spilo-e2e:dev-18.3"
SPILO_LAZY = "ghcr.io/zalando/spilo-e2e:dev-18.4"
SPILO_FULL_IMAGE = "ghcr.io/zalando/spilo-18:4.1-p2"

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
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # remove existing local storage class and create hostpath class
        try:
            k8s.api.storage_v1_api.delete_storage_class("standard")
        except ApiException as e:
            print("Failed to delete the 'standard' storage class: {0}".format(e))

        # operator deploys pod service account there on start up
        # needed for test_multi_namespace_support and test_owner_references
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
            configmap["data"]["major_version_upgrade_mode"] = "full"
            configmap["data"]["connection_pooler_image"] = os.environ['POOLER_IMAGE']

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
                         "e2e-storage-class.yaml",
                         "fes.crd.yaml"]:
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

        # load minimal Postgres manifest and wait for cluster to be up and running
        result = k8s.create_with_kubectl("manifests/minimal-postgres-manifest.yaml")
        print('stdout: {}, stderr: {}'.format(result.stdout, result.stderr))
        try:
            k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_additional_owner_roles(self):
        '''
           Test granting additional roles to existing database owners
        '''
        k8s = self.k8s

        # first test - wait for the operator to get in sync and set everything up
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
            "Operator does not get in sync")
        leader = k8s.get_cluster_leader_pod()

        # produce wrong membership for cron_admin
        grant_dbowner = """
            GRANT bar_owner TO cron_admin;
        """
        self.query_database(leader.metadata.name, "postgres", grant_dbowner)

        # enable PostgresTeam CRD and lower resync
        owner_roles = {
            "data": {
                "additional_owner_roles": "cron_admin",
            },
        }
        k8s.update_config(owner_roles)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        owner_query = """
            SELECT a2.rolname
              FROM pg_catalog.pg_authid a
              JOIN pg_catalog.pg_auth_members am
                ON a.oid = am.member
               AND a.rolname IN ('zalando', 'bar_owner', 'bar_data_owner')
              JOIN pg_catalog.pg_authid a2
                ON a2.oid = am.roleid
             WHERE a2.rolname = 'cron_admin';
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", owner_query)), 3,
            "Not all additional users found in database", 10, 5)


    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_additional_pod_capabilities(self):
        '''
           Extend postgres container capabilities
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        capabilities = ["SYS_NICE","CHOWN"]
        patch_capabilities = {
            "data": {
                "additional_pod_capabilities": ','.join(capabilities),
            },
        }

        # get node and replica (expected target of new master)
        _, replica_nodes = k8s.get_pg_nodes(cluster_label)

        try:
            k8s.update_config(patch_capabilities)

            # changed security context of postgres container should trigger a rolling update
            k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
            self.eventuallyEqual(lambda: k8s.count_pods_with_container_capabilities(capabilities, cluster_label),
                                2, "Container capabilities not updated")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_additional_teams_and_members(self):
        '''
           Test PostgresTeam CRD with extra teams and members
        '''
        k8s = self.k8s

        # enable PostgresTeam CRD and lower resync
        enable_postgres_team_crd = {
            "data": {
                "enable_postgres_team_crd": "true",
                "enable_team_member_deprecation": "true",
                "role_deletion_suffix": "_delete_me",
                "resync_period": "15s",
                "repair_period": "15s",
            },
        }
        k8s.update_config(enable_postgres_team_crd)

        # add team and member to custom-team-membership
        # contains already elephant user
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
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

        leader = k8s.get_cluster_leader_pod()
        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname IN ('elephant', 'kind');
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Not all additional users found in database", 10, 5)

        # replace additional member and check if the removed member's role is renamed
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
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

        # create fake deletion user so operator fails renaming
        # but altering role to NOLOGIN will succeed
        create_fake_deletion_user = """
            CREATE USER tester_delete_me NOLOGIN;
        """
        self.query_database(leader.metadata.name, "postgres", create_fake_deletion_user)

        # re-add additional member and check if the role is renamed back
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
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
             WHERE rolname = 'kind' AND rolcanlogin;
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 1,
            "Database role of recreated member in PostgresTeam not renamed back to original name", 10, 5)

        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname IN ('tester','tester_delete_me') AND NOT rolcanlogin;
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Database role of replaced member in PostgresTeam not denied from login", 10, 5)

        # re-add other additional member, operator should grant LOGIN back to tester
        # but nothing happens to deleted role
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresteams', 'custom-team-membership',
        {
            'spec': {
                'additionalMembers': {
                    'e2e': [
                        'kind',
                        'tester'
                    ]
                },
            }
        })

        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE (rolname IN ('tester', 'kind')
               AND rolcanlogin)
                OR (rolname = 'tester_delete_me' AND NOT rolcanlogin);
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 3,
            "Database role of deleted member in PostgresTeam not removed when recreated manually", 10, 5)

        # revert config change
        revert_resync = {
            "data": {
                "resync_period": "4m",
                "repair_period": "1m",
            },
        }
        k8s.update_config(revert_resync)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_config_update(self):
        '''
            Change Postgres config under Spec.Postgresql.Parameters and Spec.Patroni
            and query Patroni config endpoint to check if manifest changes got applied
            via restarting cluster through Patroni's rest api
        '''
        k8s = self.k8s
        leader = k8s.get_cluster_leader_pod()
        replica = k8s.get_cluster_replica_pod()
        masterCreationTimestamp = leader.metadata.creation_timestamp
        replicaCreationTimestamp = replica.metadata.creation_timestamp
        new_max_connections_value = "50"

        # adjust Postgres config
        pg_patch_config = {
            "spec": {
                "postgresql": {
                    "parameters": {
                        "max_connections": new_max_connections_value,
                        "wal_level": "logical"
                     }
                },
                "patroni": {
                    "slots": {
                        "first_slot": {
                            "type": "physical"
                        }
                    },
                    "ttl": 29,
                    "loop_wait": 9,
                    "retry_timeout": 9,
                    "synchronous_mode": True,
                    "failsafe_mode": True,
                }
            }
        }

        try:
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_config)

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            def compare_config():
                effective_config = k8s.patroni_rest(leader.metadata.name, "config")
                desired_config = pg_patch_config["spec"]["patroni"]
                desired_parameters = pg_patch_config["spec"]["postgresql"]["parameters"]
                effective_parameters = effective_config["postgresql"]["parameters"]
                self.assertEqual(desired_parameters["max_connections"], effective_parameters["max_connections"],
                            "max_connections not updated")
                self.assertTrue(effective_config["slots"] is not None, "physical replication slot not added")
                self.assertEqual(desired_config["ttl"], effective_config["ttl"],
                            "ttl not updated")
                self.assertEqual(desired_config["loop_wait"], effective_config["loop_wait"],
                            "loop_wait not updated")
                self.assertEqual(desired_config["retry_timeout"], effective_config["retry_timeout"],
                            "retry_timeout not updated")
                self.assertEqual(desired_config["synchronous_mode"], effective_config["synchronous_mode"],
                            "synchronous_mode not updated")
                self.assertEqual(desired_config["failsafe_mode"], effective_config["failsafe_mode"],
                            "failsafe_mode not updated")
                self.assertEqual(desired_config["slots"], effective_config["slots"],
                            "slots not updated")
                return True

            # check if Patroni config has been updated
            self.eventuallyTrue(compare_config, "Postgres config not applied")

            # make sure that pods were not recreated
            leader = k8s.get_cluster_leader_pod()
            replica = k8s.get_cluster_replica_pod()
            self.assertEqual(masterCreationTimestamp, leader.metadata.creation_timestamp,
                            "Master pod creation timestamp is updated")
            self.assertEqual(replicaCreationTimestamp, replica.metadata.creation_timestamp,
                            "Master pod creation timestamp is updated")

            # query max_connections setting
            setting_query = """
               SELECT setting
                 FROM pg_settings
                WHERE name = 'max_connections';
            """
            self.eventuallyEqual(lambda: self.query_database(leader.metadata.name, "postgres", setting_query)[0], new_max_connections_value,
                "New max_connections setting not applied on master", 10, 5)
            self.eventuallyNotEqual(lambda: self.query_database(replica.metadata.name, "postgres", setting_query)[0], new_max_connections_value,
                "Expected max_connections not to be updated on replica since Postgres was restarted there first", 10, 5)

            # the next sync should restart the replica because it has pending_restart flag set
            # force next sync by deleting the operator pod
            k8s.delete_operator_pod()
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            self.eventuallyEqual(lambda: self.query_database(replica.metadata.name, "postgres", setting_query)[0], new_max_connections_value,
                "New max_connections setting not applied on replica", 10, 5)

            # decrease max_connections again
            # this time restart will be correct and new value should appear on both instances
            lower_max_connections_value = "30"
            pg_patch_max_connections = {
                "spec": {
                    "postgresql": {
                        "parameters": {
                            "max_connections": lower_max_connections_value
                        }
                    }
                }
            }

            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_max_connections)

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # check Patroni config again
            pg_patch_config["spec"]["postgresql"]["parameters"]["max_connections"] = lower_max_connections_value
            self.eventuallyTrue(compare_config, "Postgres config not applied")

            # and query max_connections setting again
            self.eventuallyEqual(lambda: self.query_database(leader.metadata.name, "postgres", setting_query)[0], lower_max_connections_value,
                "Previous max_connections setting not applied on master", 10, 5)
            self.eventuallyEqual(lambda: self.query_database(replica.metadata.name, "postgres", setting_query)[0], lower_max_connections_value,
                "Previous max_connections setting not applied on replica", 10, 5)

            # patch new slot via Patroni REST
            patroni_slot = "test_patroni_slot"
            patch_slot_command = """curl -s -XPATCH -d '{"slots": {"test_patroni_slot": {"type": "physical"}}}' localhost:8008/config"""
            pg_patch_config["spec"]["patroni"]["slots"][patroni_slot] = {"type": "physical"}

            k8s.exec_with_kubectl(leader.metadata.name, patch_slot_command)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
            self.eventuallyTrue(compare_config, "Postgres config not applied")

            # test adding new slots
            pg_add_new_slots_patch = {
                "spec": {
                    "patroni": {
                        "slots": {
                            "test_slot": {
                                "type": "logical",
                                "database": "foo",
                                "plugin": "pgoutput"
                            },
                            "test_slot_2": {
                                "type": "physical"
                            }
                        }
                    }
                }
            }

            for slot_name, slot_details in pg_add_new_slots_patch["spec"]["patroni"]["slots"].items():
                pg_patch_config["spec"]["patroni"]["slots"][slot_name] = slot_details

            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_add_new_slots_patch)

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
            self.eventuallyTrue(compare_config, "Postgres config not applied")

            # delete test_slot_2 from config and change the database type for test_slot
            slot_to_change = "test_slot"
            slot_to_remove = "test_slot_2"
            pg_delete_slot_patch = {
                "spec": {
                    "patroni": {
                        "slots": {
                            "test_slot": {
                                "type": "logical",
                                "database": "bar",
                                "plugin": "pgoutput"
                            },
                            "test_slot_2": None
                        }
                    }
                }
            }

            pg_patch_config["spec"]["patroni"]["slots"][slot_to_change]["database"] = "bar"
            del pg_patch_config["spec"]["patroni"]["slots"][slot_to_remove]

            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_delete_slot_patch)

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
            self.eventuallyTrue(compare_config, "Postgres config not applied")

            get_slot_query = """
                SELECT %s
                  FROM pg_replication_slots
                 WHERE slot_name = '%s';
                """
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", get_slot_query%("slot_name", slot_to_remove))), 0,
                "The replication slot cannot be deleted", 10, 5)

            self.eventuallyEqual(lambda: self.query_database(leader.metadata.name, "postgres", get_slot_query%("database", slot_to_change))[0], "bar",
                "The replication slot cannot be updated", 10, 5)

            # make sure slot from Patroni didn't get deleted
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", get_slot_query%("slot_name", patroni_slot))), 1,
                "The replication slot from Patroni gets deleted", 10, 5)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # turn synchronous_mode off again - it makes the operator require a sync
        # standby for every switchover which other tests do not account for
        pg_patch_config["spec"]["patroni"]["synchronous_mode"] = False
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster",
            {"spec": {"patroni": {"synchronous_mode": False}}})

        # make sure cluster is in a good state for further tests
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyTrue(compare_config, "Postgres config not applied")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                             "No 2 pods running")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_cross_namespace_secrets(self):
        '''
            Test secrets in different namespace
        '''
        k8s = self.k8s

        # enable secret creation in separate namespace
        patch_cross_namespace_secret = {
            "data": {
                "enable_cross_namespace_secret": "true"
            }
        }
        k8s.update_config(patch_cross_namespace_secret,
                          step="cross namespace secrets enabled")
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        # create secret in test namespace
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'users':{
                        'test.db_user': [],
                    }
                }
            })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label("cluster-name=acid-minimal-cluster,application=spilo", self.test_namespace),
                             1, "Secret not created for user in namespace")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_custom_ssl_certificate(self):
        '''
        Test if spilo uses a custom SSL certificate
        '''

        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        tls_secret = "pg-tls"

        # get nodes of master and replica(s) (expected target of new master)
        _, replica_nodes = k8s.get_pg_nodes(cluster_label)
        self.assertNotEqual(replica_nodes, [])

        try:
            # create secret containing ssl certificate
            result = self.k8s.create_tls_secret_with_kubectl(tls_secret)
            print("stdout: {}, stderr: {}".format(result.stdout, result.stderr))

            # enable load balancer services
            pg_patch_tls = {
                "spec": {
                    "spiloFSGroup": 103,
                    "tls": {
                        "secretName": tls_secret
                    }
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_tls)

            # wait for switched over
            k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            self.eventuallyEqual(lambda: k8s.count_pods_with_env_variable("SSL_CERTIFICATE_FILE", cluster_label), 2, "TLS env variable SSL_CERTIFICATE_FILE missing in Spilo pods")
            self.eventuallyEqual(lambda: k8s.count_pods_with_env_variable("SSL_PRIVATE_KEY_FILE", cluster_label), 2, "TLS env variable SSL_PRIVATE_KEY_FILE missing in Spilo pods")
            self.eventuallyEqual(lambda: k8s.count_pods_with_volume_mount(tls_secret, cluster_label), 2, "TLS volume mount missing in Spilo pods")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_enable_disable_connection_pooler(self):
        '''
        For a database without connection pooler, then turns it on, scale up,
        turn off and on again. Test with different ways of doing this (via
        enableConnectionPooler or connectionPooler configuration section). At
        the end turn connection pooler off to not interfere with other tests.
        '''
        k8s = self.k8s
        pooler_label = 'application=db-connection-pooler,cluster-name=acid-minimal-cluster'
        master_pooler_label = 'connection-pooler=acid-minimal-cluster-pooler'
        replica_pooler_label = master_pooler_label + '-repl'
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
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(), 2, "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods(master_pooler_label), 2, "No pooler pods found")
        self.eventuallyEqual(lambda: k8s.count_running_pods(replica_pooler_label), 2, "No pooler replica pods found")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(pooler_label), 2, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label(pooler_label), 3, "Not all pooler secrets found")

        # TLS still enabled so check existing env variables and volume mounts
        self.eventuallyEqual(lambda: k8s.count_pods_with_env_variable("CONNECTION_POOLER_CLIENT_TLS_CRT", pooler_label), 4, "TLS env variable CONNECTION_POOLER_CLIENT_TLS_CRT missing in pooler pods")
        self.eventuallyEqual(lambda: k8s.count_pods_with_env_variable("CONNECTION_POOLER_CLIENT_TLS_KEY", pooler_label), 4, "TLS env variable CONNECTION_POOLER_CLIENT_TLS_KEY missing in pooler pods")
        self.eventuallyEqual(lambda: k8s.count_pods_with_volume_mount("pg-tls", pooler_label), 4, "TLS volume mount missing in pooler pods")

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableMasterPoolerLoadBalancer': True,
                    'enableReplicaPoolerLoadBalancer': True,
                }
            })
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.get_service_type(master_pooler_label+","+pooler_label),
                             'LoadBalancer',
                             "Expected LoadBalancer service type for master pooler pod, found {}")
        self.eventuallyEqual(lambda: k8s.get_service_type(replica_pooler_label+","+pooler_label),
                             'LoadBalancer',
                             "Expected LoadBalancer service type for replica pooler pod, found {}")

        master_annotations = {
            "external-dns.alpha.kubernetes.io/hostname": "acid-minimal-cluster-pooler.default.db.example.com",
        }
        self.eventuallyTrue(lambda: k8s.check_service_annotations(
            master_pooler_label+","+pooler_label, master_annotations), "Wrong annotations")

        replica_annotations = {
            "external-dns.alpha.kubernetes.io/hostname": "acid-minimal-cluster-pooler-repl.default.db.example.com",
        }
        self.eventuallyTrue(lambda: k8s.check_service_annotations(
            replica_pooler_label+","+pooler_label, replica_annotations), "Wrong annotations")

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

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(name="acid-minimal-cluster-pooler-repl"), 2,
                             "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods(master_pooler_label),
                             0, "Master pooler pods not deleted")
        self.eventuallyEqual(lambda: k8s.count_running_pods(replica_pooler_label),
                             2, "Pooler replica pods not found")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(pooler_label),
                             1, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label(pooler_label),
                             2, "Not all pooler secrets created")

        # Turn off only replica connection pooler
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': True,
                    'enableReplicaConnectionPooler': False,
                    'enableMasterPoolerLoadBalancer': False,
                }
            })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(), 2,
                             "Deployment replicas is 2 default")
        self.eventuallyEqual(lambda: k8s.count_running_pods(master_pooler_label),
                             2, "Master pooler pods not found")
        self.eventuallyEqual(lambda: k8s.count_running_pods(replica_pooler_label),
                             0, "Pooler replica pods not deleted")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(pooler_label),
                             1, "No pooler service found")
        self.eventuallyEqual(lambda: k8s.get_service_type(master_pooler_label+","+pooler_label),
                             'ClusterIP',
                             "Expected LoadBalancer service type for master, found {}")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label(pooler_label),
                             2, "Not all pooler secrets created")

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
        self.eventuallyEqual(lambda: k8s.count_running_pods(master_pooler_label),
                             3, "Scale up of pooler pods does not work")

        # turn it off, keeping config should be overwritten by false
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default',
            'postgresqls', 'acid-minimal-cluster',
            {
                'spec': {
                    'enableConnectionPooler': False,
                    'enableReplicaConnectionPooler': False,
                    'enableReplicaPoolerLoadBalancer': False,
                }
            })

        self.eventuallyEqual(lambda: k8s.count_running_pods(master_pooler_label),
                             0, "Pooler pods not scaled down")
        self.eventuallyEqual(lambda: k8s.count_services_with_label(pooler_label),
                             0, "Pooler service not removed")
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label(pooler_label),
                             0, "Not all pooler secrets deleted")

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
    def test_ignored_annotations(self):
        '''
           Test if injected annotation does not cause replacement of resources when listed under ignored_annotations
        '''
        k8s = self.k8s


        try:
            patch_config_ignored_annotations = {
                "data": {
                    "ignored_annotations": "k8s-status",
                }
            }
            k8s.update_config(patch_config_ignored_annotations)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            sts = k8s.api.apps_v1.read_namespaced_stateful_set('acid-minimal-cluster', 'default')
            svc = k8s.api.core_v1.read_namespaced_service('acid-minimal-cluster', 'default')

            annotation_patch = {
                "metadata": {
                    "annotations": {
                        "k8s-status": "healthy"
                    },
                }
            }

            old_sts_creation_timestamp = sts.metadata.creation_timestamp
            k8s.api.apps_v1.patch_namespaced_stateful_set(sts.metadata.name, sts.metadata.namespace, annotation_patch)
            old_svc_creation_timestamp = svc.metadata.creation_timestamp
            k8s.api.core_v1.patch_namespaced_service(svc.metadata.name, svc.metadata.namespace, annotation_patch)

            k8s.delete_operator_pod()
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            sts = k8s.api.apps_v1.read_namespaced_stateful_set('acid-minimal-cluster', 'default')
            new_sts_creation_timestamp = sts.metadata.creation_timestamp
            svc = k8s.api.core_v1.read_namespaced_service('acid-minimal-cluster', 'default')
            new_svc_creation_timestamp = svc.metadata.creation_timestamp

            self.assertEqual(old_sts_creation_timestamp, new_sts_creation_timestamp, "unexpected replacement of statefulset on sync")
            self.assertEqual(old_svc_creation_timestamp, new_svc_creation_timestamp, "unexpected replacement of master service on sync")

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
                            "IsDbOwner": False,
                            "Deleted": False,
                            "Rotated": False,
                            "Degraded": False,
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
    def test_lazy_spilo_upgrade(self):
        '''
        Test lazy upgrade for the Spilo image: operator changes a stateful set
        but lets pods run with the old image until they are recreated for
        reasons other than operator's activity. That works because the operator
        configures stateful sets to use "onDelete" pod update policy.
        The test covers:
        1) enabling lazy upgrade in existing operator deployment
        2) postponing the lazy upgrade when a maintenance window is defined
        but the current time is outside of it
        3) executing the lazy upgrade once the maintenance window is entered
        4) forcing the normal rolling upgrade by changing the operator
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
            self.assertEqual(k8s.get_effective_pod_image(pod1), SPILO_CURRENT,
                             "pod-1 should still run the old Docker image")

            # the pending rotation of pod-1 must be flagged as lazy, otherwise
            # the maintenance window has no effect on it
            self.eventuallyEqual(lambda: k8s.get_rolling_update_flag(pod1), "lazy upgrade",
                                 "pod-1 is not flagged for a lazy rotation, got {}")

            # define a maintenance window in the future - the pending lazy
            # upgrade of pod-1 must still be postponed
            current_time = datetime.now()
            future_hour = (current_time + timedelta(hours=2)).strftime('%H')
            maintenance_window_future = f"{future_hour}:00-{future_hour}:59"
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster",
                {"spec": {"maintenanceWindows": [maintenance_window_future]}})
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                                 "Operator does not get in sync")
            self.assertEqual(k8s.get_effective_pod_image(pod1), SPILO_CURRENT,
                             "pod-1 should not be rotated outside of the maintenance window")

            # move the maintenance window to now - the lazy upgrade of pod-1 must be executed
            maintenance_window_current = "00:00-23:59"
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster",
                {"spec": {"maintenanceWindows": [maintenance_window_current]}})
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod1),
                                 conf_image, "pod-1 was not rotated during the maintenance window",
                                 50, 3)
            self.eventuallyEqual(lambda: k8s.count_running_pods(), 2,
                                 "No two pods running after lazy rolling upgrade")
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                                 2, "Postgres status did not enter running")

            # roll back the image with lazy upgrade still enabled and the
            # maintenance window moved out of reach again - no pod is rotated
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster",
                {"spec": {"maintenanceWindows": [maintenance_window_future]}})
            k8s.update_config({"data": {"docker_image": SPILO_CURRENT}},
                              step="roll back image with lazy upgrade")
            self.eventuallyEqual(lambda: k8s.get_statefulset_image(), SPILO_CURRENT,
                                 "Statefulset not rolled back to previous Docker image")
            self.assertEqual(k8s.get_effective_pod_image(pod0), conf_image,
                             "pod-0 should not be rotated outside of the maintenance window")
            self.assertEqual(k8s.get_effective_pod_image(pod1), conf_image,
                             "pod-1 should not be rotated outside of the maintenance window")

            # clean up
            unpatch_lazy_spilo_upgrade = {
                "data": {
                    "enable_lazy_spilo_upgrade": "false",
                }
            }
            k8s.update_config(unpatch_lazy_spilo_upgrade, step="patch lazy upgrade")

            # at this point operator will complete the normal rolling upgrade
            # so we additionally test if disabling the lazy upgrade - forcing the normal rolling upgrade
            # outside of the maintenance window - works
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod0),
                                 SPILO_CURRENT, "Rolling upgrade was not executed",
                                 50, 3)
            self.eventuallyEqual(lambda: k8s.get_effective_pod_image(pod1),
                                 SPILO_CURRENT, "Rolling upgrade was not executed",
                                 50, 3)
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members(pod0)),
                                 2, "Postgres status did not enter running")

            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster",
                {"spec": {"maintenanceWindows": []}})
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                                 "Operator does not get in sync")

        except (timeout_decorator.TimeoutError, AssertionError):
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
    def test_major_version_upgrade(self):
        """
        Test major version upgrade: with full upgrade, maintenance window, and annotation
        """
        def check_version():
            p = k8s.patroni_rest("acid-upgrade-test-0", "") or {}
            version = p.get("server_version", 0) // 10000
            return version

        def get_annotations():
            pg_manifest = k8s.api.custom_objects_api.get_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test")
            annotations = pg_manifest["metadata"]["annotations"]
            return annotations

        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-upgrade-test'

        with open("manifests/minimal-postgres-lowest-version-manifest.yaml", 'r+') as f:
            upgrade_manifest = yaml.safe_load(f)
            upgrade_manifest["spec"]["dockerImage"] = SPILO_FULL_IMAGE

        with open("manifests/minimal-postgres-lowest-version-manifest.yaml", 'w') as f:
            yaml.dump(upgrade_manifest, f, Dumper=yaml.Dumper)

        k8s.create_with_kubectl("manifests/minimal-postgres-lowest-version-manifest.yaml")
        self.eventuallyEqual(lambda: k8s.count_running_pods(labels=cluster_label), 2, "No 2 pods running")
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(check_version, 14, "Version is not correct")

        master_nodes, _ = k8s.get_cluster_nodes(cluster_labels=cluster_label)
        # should upgrade immediately
        pg_patch_version_higher_version = {
            "spec": {
                "postgresql": {
                    "version": "15"
                }
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version_higher_version)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync",
                             retries=180)

        k8s.wait_for_pod_failover(master_nodes, 'spilo-role=replica,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        self.eventuallyEqual(check_version, 15, "Version should be upgraded from 14 to 15")

        # check if annotation for last upgrade's success is set
        annotations = get_annotations()
        self.assertIsNotNone(annotations.get("last-major-upgrade-success"), "Annotation for last upgrade's success is not set")

        # should not upgrade because current time is not in maintenanceWindow
        current_time = datetime.now()
        maintenance_window_future = f"{(current_time+timedelta(minutes=60)).strftime('%H:%M')}-{(current_time+timedelta(minutes=120)).strftime('%H:%M')}"
        pg_patch_version_higher_version_outside_mw = {
            "spec": {
                "postgresql": {
                    "version": "16"
                },
                "maintenanceWindows": [
                    maintenance_window_future
                ]
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version_higher_version_outside_mw)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        # no pod replacement outside of the maintenance window
        k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        self.eventuallyEqual(check_version, 15, "Version should not be upgraded")

        second_annotations = get_annotations()
        self.assertIsNone(second_annotations.get("last-major-upgrade-failure"), "Annotation for last upgrade's failure should not be set")

        # change maintenanceWindows to current
        maintenance_window_current = f"{(current_time-timedelta(minutes=30)).strftime('%H:%M')}-{(current_time+timedelta(minutes=30)).strftime('%H:%M')}"
        pg_patch_version_higher_version_in_mw = {
            "spec": {
                "postgresql": {
                    "version": "16"
                },
                "maintenanceWindows": [
                    maintenance_window_current
                ]
            }
        }

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version_higher_version_in_mw)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        k8s.wait_for_pod_failover(master_nodes, 'spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        self.eventuallyEqual(check_version, 16, "Version should be upgraded from 15 to 16")

        # check if annotation for last upgrade's success is updated after second upgrade
        third_annotations = get_annotations()
        self.assertIsNotNone(third_annotations.get("last-major-upgrade-success"), "Annotation for last upgrade's success is not set")
        self.assertNotEqual(annotations.get("last-major-upgrade-success"), third_annotations.get("last-major-upgrade-success"), "Annotation for last upgrade's success is not updated")

        # test upgrade with failed upgrade annotation
        pg_patch_version_highest_version = {
            "metadata": {
                "annotations": {
                    "last-major-upgrade-failure": "2024-01-02T15:04:05Z"
                },
            },
            "spec": {
                "postgresql": {
                    "version": "18"
                },
            },
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version_highest_version)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        k8s.wait_for_pod_failover(master_nodes, 'spilo-role=replica,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        self.eventuallyEqual(check_version, 16, "Version should not be upgraded because annotation for last upgrade's failure is set")

        # change the version back to 16 and should remove failure annotation
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-upgrade-test", pg_patch_version_higher_version_in_mw)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

        self.eventuallyEqual(check_version, 16, "Version should not be upgraded from 16")
        fourth_annotations = get_annotations()
        self.assertIsNone(fourth_annotations.get("last-major-upgrade-failure"), "Annotation for last upgrade's failure is not removed")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_persistent_volume_claim_retention_policy(self):
        '''
            Test the retention policy for persistent volume claim
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 2, "PVCs is not equal to number of instance")

        # patch the pvc retention policy to enable delete when scale down
        patch_scaled_policy_delete = {
            "data": {
                "persistent_volume_claim_retention_policy": "when_deleted:retain,when_scaled:delete"
            }
        }
        k8s.update_config(patch_scaled_policy_delete)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        pg_patch_scale_down_instances = {
            'spec': {
                'numberOfInstances': 1
            }
        }
        # decrease the number of instances
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', pg_patch_scale_down_instances)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},"Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 1, "PVCs is not deleted when scaled down")

        pg_patch_scale_up_instances = {
            'spec': {
                'numberOfInstances': 2
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', pg_patch_scale_up_instances)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},"Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 2, "PVCs is not equal to number of instances")

        # reset retention policy to retain
        patch_scaled_policy_retain = {
            "data": {
                "persistent_volume_claim_retention_policy": "when_deleted:retain,when_scaled:retain"
            }
        }
        k8s.update_config(patch_scaled_policy_retain)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        # decrease the number of instances
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', pg_patch_scale_down_instances)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},"Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 1, "Scale down to 1 failed")
        self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 2, "PVCs is deleted when scaled down")

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', pg_patch_scale_up_instances)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},"Operator does not get in sync")
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
        self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 2, "PVCs is not equal to number of instances")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_resource_generation(self):
        '''
        Lower resource limits below configured minimum and let operator fix it.
        It will try to raise requests to limits which is capped with max_memory_request.
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # get nodes of master and replica(s) (expected target of new master)
        _, replica_nodes = k8s.get_pg_nodes(cluster_label)
        self.assertNotEqual(replica_nodes, [])

        # configure maximum memory request and minimum boundaries for CPU and memory limits
        maxMemoryRequest = '300Mi'
        minCPULimit = '503m'
        minMemoryLimit = '502Mi'

        patch_pod_resources = {
            "data": {
                "max_memory_request": maxMemoryRequest,
                "min_cpu_limit": minCPULimit,
                "min_memory_limit": minMemoryLimit,
                "set_memory_request_to_limit": "true"
            }
        }
        k8s.update_config(patch_pod_resources, "Pod resource test")

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
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync", retries=120)

        # wait for switched over
        k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)
        k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

        def verify_pod_resources():
            pods = k8s.api.core_v1.list_namespaced_pod('default', label_selector="cluster-name=acid-minimal-cluster,application=spilo").items
            if len(pods) < 2:
                return False

            r = pods[0].spec.containers[0].resources.requests['memory'] == maxMemoryRequest
            r = r and pods[0].spec.containers[0].resources.limits['memory'] == minMemoryLimit
            r = r and pods[0].spec.containers[0].resources.limits['cpu'] == minCPULimit
            r = r and pods[1].spec.containers[0].resources.requests['memory'] == maxMemoryRequest
            r = r and pods[1].spec.containers[0].resources.limits['memory'] == minMemoryLimit
            r = r and pods[1].spec.containers[0].resources.limits['cpu'] == minCPULimit
            return r

        self.eventuallyTrue(verify_pod_resources, "Pod resources where not adjusted")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_multi_namespace_support(self):
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
            k8s.wait_for_pod_start("spilo-role=replica", self.test_namespace)
            self.assert_master_is_unique(self.test_namespace, "acid-test-cluster")
            # acid-test-cluster will be deleted in test_owner_references test

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

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

        # get nodes of master and replica(s)
        master_nodes, replica_nodes = k8s.get_cluster_nodes()
        self.assertNotEqual(master_nodes, [])
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
            # patch master node with the label
            k8s.api.core_v1.patch_node(master_nodes[0], node_label_body)

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
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                                 "Operator does not get in sync")

            # node affinity change should cause replica to relocate from replica node to master node due to node affinity requirement
            k8s.wait_for_pod_failover(master_nodes, 'spilo-role=replica,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
            # next master will be switched over and pod needs to be replaced as well to finish the rolling update
            k8s.wait_for_pod_failover(master_nodes, 'spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            podsList = k8s.api.core_v1.list_namespaced_pod('default', label_selector=cluster_label)
            for pod in podsList.items:
                if pod.metadata.labels.get('spilo-role') == 'replica':
                    self.assertEqual(master_nodes[0], pod.spec.node_name,
                         "Sanity check: expected replica to relocate to master node {}, but found on {}".format(master_nodes[0], pod.spec.node_name))

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
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # node affinity change should cause another rolling update and relocation of replica
            k8s.wait_for_pod_failover(master_nodes, 'spilo-role=replica,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # toggle pod anti affinity to make sure replica and master run on separate nodes
        self.assert_distributed_pods(master_nodes)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    @unittest.skip("Skipping this test until fixed")
    def test_node_readiness_label(self):
        '''
           Remove node readiness label from master node. This must cause a failover.
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        readiness_label = 'lifecycle-status'
        readiness_value = 'ready'

        # verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")

        # get nodes of master and replica(s) (expected target of new master)
        master_nodes, replica_nodes = k8s.get_cluster_nodes()
        self.assertNotEqual(master_nodes, [])
        self.assertNotEqual(replica_nodes, [])

        try:
            # add node_readiness_label to potential failover nodes
            patch_readiness_label = {
                "metadata": {
                    "labels": {
                        readiness_label: readiness_value
                    }
                }
            }
            for replica_node in replica_nodes:
                k8s.api.core_v1.patch_node(replica_node, patch_readiness_label)

            # define node_readiness_label in config map which should trigger a rolling update
            patch_readiness_label_config = {
                "data": {
                    "node_readiness_label": readiness_label + ':' + readiness_value,
                    "node_readiness_label_merge": "AND",
                }
            }
            k8s.update_config(patch_readiness_label_config, "setting readiness label")
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # first replica will be replaced and get the new affinity
            # however, it might not start due to a volume node affinity conflict
            # in this case only if the pvc and pod are deleted it can be scheduled
            replica = k8s.get_cluster_replica_pod()
            if replica.status.phase == 'Pending':
                k8s.api.core_v1.delete_namespaced_persistent_volume_claim('pgdata-' + replica.metadata.name, 'default')
                k8s.api.core_v1.delete_namespaced_pod(replica.metadata.name, 'default')
                k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # next master will be switched over and pod needs to be replaced as well to finish the rolling update
            k8s.wait_for_pod_failover(replica_nodes, 'spilo-role=master,' + cluster_label)
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # patch also node where master ran before
            k8s.api.core_v1.patch_node(master_nodes[0], patch_readiness_label)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # toggle pod anti affinity to move replica away from master node
        self.assert_distributed_pods(master_nodes)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_overwrite_pooler_deployment(self):
        pooler_name = 'acid-minimal-cluster-pooler'
        k8s = self.k8s
        k8s.create_with_kubectl("manifests/minimal-fake-pooler-deployment.yaml")
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(name=pooler_name), 1,
                             "Initial broken deployment not rolled out")

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresqls', 'acid-minimal-cluster',
        {
            'spec': {
                'enableConnectionPooler': True
            }
        })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.get_deployment_replica_count(name=pooler_name), 2,
                             "Operator did not succeed in overwriting labels")

        k8s.api.custom_objects_api.patch_namespaced_custom_object(
        'acid.zalan.do', 'v1', 'default',
        'postgresqls', 'acid-minimal-cluster',
        {
            'spec': {
                'enableConnectionPooler': False
            }
        })

        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_running_pods("connection-pooler="+pooler_name),
                             0, "Pooler pods not scaled down")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_owner_references(self):
        '''
           Enable owner references, test if resources get updated and test cascade deletion of test cluster.
        '''
        k8s = self.k8s
        cluster_name = 'acid-test-cluster'
        cluster_label = 'application=spilo,cluster-name={}'.format(cluster_name)
        default_test_cluster = 'acid-minimal-cluster'

        try:
            # enable owner references in config
            enable_owner_refs = {
                "data": {
                    "enable_owner_references": "true"
                }
            }
            k8s.update_config(enable_owner_refs)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            time.sleep(5)  # wait for the operator to sync the cluster and update resources

            # check if child resources were updated with owner references
            self.assertTrue(self.check_cluster_child_resources_owner_references(cluster_name, self.test_namespace), "Owner references not set on all child resources of {}".format(cluster_name))
            self.assertTrue(self.check_cluster_child_resources_owner_references(default_test_cluster), "Owner references not set on all child resources of {}".format(default_test_cluster))

            # delete the new cluster to test owner references
            # and also to make k8s_api.get_operator_state work better in subsequent tests
            # ideally we should delete the 'test' namespace here but the pods
            # inside the namespace stuck in the Terminating state making the test time out
            k8s.api.custom_objects_api.delete_namespaced_custom_object(
                "acid.zalan.do", "v1", self.test_namespace, "postgresqls", cluster_name)

            # child resources with owner references should be deleted via owner references
            self.eventuallyEqual(lambda: k8s.count_pods_with_label(cluster_label), 0, "Pods not deleted")
            self.eventuallyEqual(lambda: k8s.count_statefulsets_with_label(cluster_label), 0, "Statefulset not deleted")
            self.eventuallyEqual(lambda: k8s.count_services_with_label(cluster_label), 0, "Services not deleted")
            self.eventuallyEqual(lambda: k8s.count_endpoints_with_label(cluster_label), 0, "Endpoints not deleted")
            self.eventuallyEqual(lambda: k8s.count_pdbs_with_label(cluster_label), 0, "Pod disruption budget not deleted")
            self.eventuallyEqual(lambda: k8s.count_secrets_with_label(cluster_label), 0, "Secrets were not deleted")

            time.sleep(5)  # wait for the operator to also delete the PVCs

            # pvcs do not have an owner reference but will deleted by the operator almost immediately
            self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 0, "PVCs not deleted")

            # disable owner references in config
            disable_owner_refs = {
                "data": {
                    "enable_owner_references": "false"
                }
            }
            k8s.update_config(disable_owner_refs)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            time.sleep(5)  # wait for the operator to remove owner references

            # check if child resources were updated without Postgresql owner references
            self.assertTrue(self.check_cluster_child_resources_owner_references(default_test_cluster, "default", True), "Owner references still present on some child resources of {}".format(default_test_cluster))

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_password_rotation(self):
        '''
           Test password rotation and removal of users due to retention policy
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'
        leader = k8s.get_cluster_leader_pod()
        today = date.today()

        # remember number of secrets to make sure it stays the same
        secret_count = k8s.count_secrets_with_label(cluster_label)

        # enable password rotation for owner of foo database
        pg_patch_rotation_single_users = {
            "spec": {
                "usersIgnoringSecretRotation": [
                    "test.db_user"
                ],
                "usersWithInPlaceSecretRotation": [
                    "zalando"
                ]
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_patch_rotation_single_users)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        # check if next rotation date was set in secret
        zalando_secret = k8s.get_secret("zalando")
        next_rotation_timestamp = datetime.strptime(str(base64.b64decode(zalando_secret.data["nextRotation"]), 'utf-8'), "%Y-%m-%dT%H:%M:%SZ")
        today90days = today+timedelta(days=90)
        self.assertEqual(today90days, next_rotation_timestamp.date(),
                        "Unexpected rotation date in secret of zalando user: expected {}, got {}".format(today90days, next_rotation_timestamp.date()))

        # create fake rotation users that should be removed by operator
        # but have one that would still fit into the retention period
        create_fake_rotation_user = """
            CREATE USER foo_user201031 IN ROLE foo_user;
            CREATE USER foo_user211031 IN ROLE foo_user;
            CREATE USER foo_user"""+(today-timedelta(days=40)).strftime("%y%m%d")+""" IN ROLE foo_user;
        """
        self.query_database(leader.metadata.name, "postgres", create_fake_rotation_user)

        # patch foo_user secret with outdated rotation date
        fake_rotation_date = today.isoformat() + 'T00:00:00Z'
        fake_rotation_date_encoded = base64.b64encode(fake_rotation_date.encode('utf-8'))
        secret_fake_rotation = {
            "data": {
                "nextRotation": str(fake_rotation_date_encoded, 'utf-8'),
            },
        }
        k8s.api.core_v1.patch_namespaced_secret(
            name="foo-user.acid-minimal-cluster.credentials.postgresql.acid.zalan.do",
            namespace="default",
            body=secret_fake_rotation)

        # update rolconfig for foo_user that will be copied for new rotation user
        alter_foo_user_search_path = """
            ALTER ROLE foo_user SET search_path TO data;
        """
        self.query_database(leader.metadata.name, "postgres", alter_foo_user_search_path)

        # enable password rotation for all other users (foo_user)
        # this will force a sync of secrets for further assertions
        enable_password_rotation = {
            "data": {
                "enable_password_rotation": "true",
                "inherited_annotations": "environment",
                "password_rotation_interval": "30",
                "password_rotation_user_retention": "30",  # should be set to 60
            },
        }
        k8s.update_config(enable_password_rotation)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        # check if next rotation date and username have been replaced
        foo_user_secret = k8s.get_secret("foo_user")
        secret_username = str(base64.b64decode(foo_user_secret.data["username"]), 'utf-8')
        next_rotation_timestamp = datetime.strptime(str(base64.b64decode(foo_user_secret.data["nextRotation"]), 'utf-8'), "%Y-%m-%dT%H:%M:%SZ")
        rotation_user = "foo_user"+today.strftime("%y%m%d")
        today30days = today+timedelta(days=30)

        self.assertEqual(rotation_user, secret_username,
                        "Unexpected username in secret of foo_user: expected {}, got {}".format(rotation_user, secret_username))
        self.assertEqual(today30days, next_rotation_timestamp.date(),
                        "Unexpected rotation date in secret of foo_user: expected {}, got {}".format(today30days, next_rotation_timestamp.date()))

        # check if oldest fake rotation users were deleted
        # there should only be foo_user, foo_user+today and foo_user+today-40days
        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname LIKE 'foo_user%';
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 3,
            "Found incorrect number of rotation users", 10, 5)

        # check if rolconfig was passed from foo_user to foo_user+today
        # and that no foo_user has been deprecated (can still login)
        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname LIKE 'foo_user%'
               AND rolconfig = ARRAY['search_path=data']::text[]
               AND rolcanlogin;
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Rolconfig not applied to new rotation user", 10, 5)

        # test that rotation_user can connect to the database
        self.eventuallyEqual(lambda: len(self.query_database_with_user(leader.metadata.name, "postgres", "SELECT 1", "foo_user")), 1,
            "Could not connect to the database with rotation user {}".format(rotation_user), 10, 5)

        # add annotation which triggers syncSecrets call
        pg_annotation_patch = {
            "metadata": {
                "annotations": {
                    "environment": "test",
                }
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default", "postgresqls", "acid-minimal-cluster", pg_annotation_patch)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        time.sleep(10)
        self.eventuallyEqual(lambda: k8s.count_secrets_with_label(cluster_label), secret_count, "Unexpected number of secrets")

        # check if rotation has been ignored for user from test_cross_namespace_secrets test
        db_user_secret = k8s.get_secret(username="test.db_user", namespace="test")
        secret_username = str(base64.b64decode(db_user_secret.data["username"]), 'utf-8')
        self.assertEqual("test.db_user", secret_username,
                        "Unexpected username in secret of test.db_user: expected {}, got {}".format("test.db_user", secret_username))

        # check if annotation for secret has been updated
        self.assertTrue("environment" in db_user_secret.metadata.annotations, "Added annotation was not propagated to secret")

        # disable password rotation for all other users (foo_user)
        # and pick smaller intervals to see if the third fake rotation user is dropped
        enable_password_rotation = {
            "data": {
                "enable_password_rotation": "false",
                "password_rotation_interval": "15",
                "password_rotation_user_retention": "30",  # 2 * rotation interval
            },
        }
        k8s.update_config(enable_password_rotation)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                             "Operator does not get in sync")

        # check if username in foo_user secret is reset
        foo_user_secret = k8s.get_secret("foo_user")
        secret_username = str(base64.b64decode(foo_user_secret.data["username"]), 'utf-8')
        next_rotation_timestamp = str(base64.b64decode(foo_user_secret.data["nextRotation"]), 'utf-8')
        self.assertEqual("foo_user", secret_username,
                        "Unexpected username in secret of foo_user: expected {}, got {}".format("foo_user", secret_username))
        self.assertEqual('', next_rotation_timestamp,
                        "Unexpected rotation date in secret of foo_user: expected empty string, got {}".format(next_rotation_timestamp))

        # check roles again, there should only be foo_user and foo_user+today
        user_query = """
            SELECT rolname
              FROM pg_catalog.pg_roles
             WHERE rolname LIKE 'foo_user%';
        """
        self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", user_query)), 2,
            "Found incorrect number of rotation users after disabling password rotation", 10, 5)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_rolling_update_annotation(self):
        '''
            Add rolling update annotation to only the master and see it failing over
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
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # check if the former replica is now the new master
            leader = k8s.get_cluster_leader_pod()
            self.eventuallyEqual(lambda: leader.metadata.name, switchover_target, "Rolling update annotation did not trigger switchover")

            # check that the old master has been recreated
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)
            replica = k8s.get_cluster_replica_pod()
            self.assertTrue(replica.metadata.creation_timestamp > old_creation_timestamp, "Old master pod was not recreated")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    @unittest.skip("Skipping this test until fixed")
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
                "resync_period": "30s",
                "repair_period": "30s",
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
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # check if the former replica is now the new master
            leader = k8s.get_cluster_leader_pod()
            self.eventuallyEqual(lambda: leader.metadata.name, switchover_target, "Rolling update annotation did not trigger switchover")

            # wait for the old master to get restarted
            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_label)

            # status should again be "SyncFailed" but turn into "Running" on the next sync
            time.sleep(30)
            self.eventuallyEqual(lambda: k8s.pg_get_status(), "Running", "Expected running cluster after two syncs")

            # revert config changes
            patch_resync_config = {
                "data": {
                    "pod_label_wait_timeout": "10m",
                    "resync_period": "4m",
                    "repair_period": "2m",
                }
            }
            k8s.update_config(patch_resync_config, "revert resync interval and pod_label_wait_timeout")


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
                "inherited_annotations": "environment,owned-by",
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
    def test_standby_cluster(self):
        '''
        Create standby cluster streaming from remote primary
        '''
        k8s = self.k8s
        standby_cluster_name = 'acid-standby-cluster'
        cluster_name_label = 'cluster-name'
        cluster_label = 'application=spilo,{}={}'.format(cluster_name_label, standby_cluster_name)
        superuser_name = 'postgres'
        replication_user = 'standby'
        secret_suffix = 'credentials.postgresql.acid.zalan.do'

        # copy secrets from remote cluster before operator creates them when bootstrapping the standby cluster
        postgres_secret = k8s.get_secret(superuser_name)
        postgres_secret.metadata.name = '{}.{}.{}'.format(superuser_name, standby_cluster_name, secret_suffix)
        postgres_secret.metadata.labels[cluster_name_label] = standby_cluster_name
        k8s.create_secret(postgres_secret)
        standby_secret = k8s.get_secret(replication_user)
        standby_secret.metadata.name = '{}.{}.{}'.format(replication_user, standby_cluster_name, secret_suffix)
        standby_secret.metadata.labels[cluster_name_label] = standby_cluster_name
        k8s.create_secret(standby_secret)

        try:
            k8s.create_with_kubectl("manifests/standby-manifest.yaml")
            k8s.wait_for_pod_start("spilo-role=master," + cluster_label)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise
        finally:
            # delete the standby cluster so that the k8s_api.get_operator_state works correctly in subsequent tests
            k8s.api.custom_objects_api.delete_namespaced_custom_object(
                "acid.zalan.do", "v1", "default", "postgresqls", "acid-standby-cluster")
            time.sleep(5)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_stream_resources(self):
        '''
           Create and delete fabric event streaming resources.
        '''
        k8s = self.k8s
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
            "Operator does not get in sync")
        leader = k8s.get_cluster_leader_pod()

        # patch ClusterRole with CRUD privileges on FES resources
        cluster_role = k8s.api.rbac_api.read_cluster_role("postgres-operator")
        fes_cluster_role_rule = client.V1PolicyRule(
            api_groups=["zalando.org"],
            resources=["fabriceventstreams"],
            verbs=["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]
        )
        cluster_role.rules.append(fes_cluster_role_rule)

        try:
            k8s.api.rbac_api.patch_cluster_role("postgres-operator", cluster_role)

            # create a table in one of the database of acid-minimal-cluster
            create_stream_table = """
                CREATE TABLE test_table (id int, payload jsonb);
            """
            self.query_database(leader.metadata.name, "foo", create_stream_table)

            # update the manifest with the streams section
            patch_streaming_config = {
                "spec": {
                    "patroni": {
                        "slots": {
                            "manual_slot": {
                                "type": "physical"
                            }
                        }
                    },
                    "streams": [
                        {
                            "applicationId": "test-app",
                            "batchSize": 100,
                            "cpu": "100m",
                            "memory": "200Mi",
                            "database": "foo",
                            "enableRecovery": True,
                            "tables": {
                                "test_table": {
                                    "eventType": "test-event",
                                    "idColumn": "id",
                                    "payloadColumn": "payload",
                                    "recoveryEventType": "test-event-dlq"
                                }
                            }
                        },
                        {
                            "applicationId": "test-app2",
                            "batchSize": 100,
                            "database": "foo",
                            "enableRecovery": True,
                            "tables": {
                                "test_non_exist_table": {
                                    "eventType": "test-event",
                                    "idColumn": "id",
                                    "payloadColumn": "payload",
                                    "ignoreRecovery": True
                                }
                            }
                        }
                    ]
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', patch_streaming_config)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # check if publication, slot, and fes resource are created
            get_publication_query = """
                SELECT * FROM pg_publication WHERE pubname = 'fes_foo_test_app';
            """
            get_slot_query = """
                SELECT * FROM pg_replication_slots WHERE slot_name = 'fes_foo_test_app';
            """
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_publication_query)), 1,
                "Publication is not created", 10, 5)
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_slot_query)), 1,
                "Replication slot is not created", 10, 5)
            self.eventuallyEqual(lambda: len(k8s.api.custom_objects_api.list_namespaced_custom_object(
                "zalando.org", "v1", "default", "fabriceventstreams", label_selector="cluster-name=acid-minimal-cluster")["items"]), 1,
                "Could not find Fabric Event Stream resource", 10, 5)

            # check if the non-existing table in the stream section does not create a publication and slot
            get_publication_query_not_exist_table = """
                SELECT * FROM pg_publication WHERE pubname = 'fes_foo_test_app2';
            """
            get_slot_query_not_exist_table = """
                SELECT * FROM pg_replication_slots WHERE slot_name = 'fes_foo_test_app2';
            """
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_publication_query_not_exist_table)), 0,
                "Publication is created for non-existing tables", 10, 5)
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_slot_query_not_exist_table)), 0,
                "Replication slot is created for non-existing tables", 10, 5)

            # grant create and ownership of test_table to foo_user, reset search path to default
            grant_permission_foo_user = """
                GRANT CREATE ON DATABASE foo TO foo_user;
                ALTER TABLE test_table OWNER TO foo_user;
                ALTER ROLE foo_user RESET search_path;
            """
            self.query_database(leader.metadata.name, "foo", grant_permission_foo_user)
            # non-postgres user creates a publication
            create_nonstream_publication = """
                CREATE PUBLICATION mypublication FOR TABLE test_table;
            """
            self.query_database_with_user(leader.metadata.name, "foo", create_nonstream_publication, "foo_user")

            # remove the streams section from the manifest
            patch_streaming_config_removal = {
                "spec": {
                    "streams": []
                }
            }
            k8s.api.custom_objects_api.patch_namespaced_custom_object(
                'acid.zalan.do', 'v1', 'default', 'postgresqls', 'acid-minimal-cluster', patch_streaming_config_removal)
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # check if publication, slot, and fes resource are removed
            self.eventuallyEqual(lambda: len(k8s.api.custom_objects_api.list_namespaced_custom_object(
                "zalando.org", "v1", "default", "fabriceventstreams", label_selector="cluster-name=acid-minimal-cluster")["items"]), 0,
                'Could not delete Fabric Event Stream resource', 10, 5)
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_publication_query)), 0,
                "Publication is not deleted", 10, 5)
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_slot_query)), 0,
                "Replication slot is not deleted", 10, 5)

            # check the manual_slot and mypublication should not get deleted
            get_manual_slot_query = """
                SELECT * FROM pg_replication_slots WHERE slot_name = 'manual_slot';
            """
            get_nonstream_publication_query = """
                SELECT * FROM pg_publication WHERE pubname = 'mypublication';
            """
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "postgres", get_manual_slot_query)), 1,
                "Slot defined in patroni config is deleted", 10, 5)
            self.eventuallyEqual(lambda: len(self.query_database(leader.metadata.name, "foo", get_nonstream_publication_query)), 1,
                "Publication defined not in stream section is deleted", 10, 5)

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_taint_based_eviction(self):
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

        # Keep the operator running if it shares the master's node.
        operator_pod = k8s.get_operator_pod()
        operator_tolerations = [
            k8s.api.k8s_client.sanitize_for_serialization(toleration)
            for toleration in (operator_pod.spec.tolerations or [])
        ]
        operator_tolerations.append({
            "key": "postgres",
            "operator": "Exists",
            "effect": "NoExecute"
        })
        k8s.patch_pod({"spec": {"tolerations": operator_tolerations}}, operator_pod.metadata.name)

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

        # add toleration to pods
        patch_toleration_config = {
            "data": {
                "toleration": "key:postgres,operator:Exists,effect:NoExecute"
            }
        }

        try:
            k8s.update_config(patch_toleration_config, step="allow tainted nodes")
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"},
                        "Operator does not get in sync", retries=180)

            self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")
            self.eventuallyEqual(lambda: len(k8s.get_patroni_running_members("acid-minimal-cluster-0")), 2, "Postgres status did not enter running")

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        # toggle pod anti affinity to move replica away from master node
        self.assert_distributed_pods(master_nodes)

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_topology_spread_constraints(self):
        '''
            Enable topologySpreadConstraints for pods
        '''
        k8s = self.k8s
        cluster_labels = "application=spilo,cluster-name=acid-minimal-cluster"

        # Verify we are in good state from potential previous tests
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "No 2 pods running")

        # patch the pvc retention policy to enable delete when scale down
        patch_scaled_policy_delete = {
            "data": {
                "persistent_volume_claim_retention_policy": "when_deleted:retain,when_scaled:delete"
            }
        }
        k8s.update_config(patch_scaled_policy_delete)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

        master_nodes, replica_nodes = k8s.get_cluster_nodes()
        self.assertNotEqual(master_nodes, [])
        self.assertNotEqual(replica_nodes, [])

        # Patch label to nodes for topologySpreadConstraints
        patch_node_label = {
            "metadata": {
                "labels": {
                    "topology.kubernetes.io/zone": "zalando"
                }
            }
        }
        k8s.api.core_v1.patch_node(master_nodes[0], patch_node_label)
        k8s.api.core_v1.patch_node(replica_nodes[0], patch_node_label)

        # Patch topologySpreadConstraint and scale-out postgresql pods to postgresqls manifest.
        patch_topologySpreadConstraint_config = {
            "spec": {
                "numberOfInstances": 6,
                "topologySpreadConstraint": [
                    {
                        "maxskew": 1,
                        "topologyKey": "topology.kubernetes.io/zone",
                        "whenUnsatisfiable": "DoNotSchedule"
                    }
                ]
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default",
            "postgresqls", "acid-minimal-cluster",
            patch_topologySpreadConstraint_config)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_pods_with_label(cluster_labels), 6, "Postgresql StatefulSet are scale to 6")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 6, "All pods are running")

        worker_node_1 = 0
        worker_node_2 = 0
        pods = k8s.api.core_v1.list_namespaced_pod('default', label_selector=cluster_labels)
        for pod in pods.items:
            if pod.spec.node_name == 'postgres-operator-e2e-tests-worker':
                worker_node_1 += 1
            elif pod.spec.node_name == 'postgres-operator-e2e-tests-worker2':
                worker_node_2 += 1

        self.assertEqual(worker_node_1, worker_node_2)
        self.assertEqual(worker_node_1, 3)
        self.assertEqual(worker_node_2, 3)

        # Reset configurations
        patch_topologySpreadConstraint_config = {
            "spec": {
                "numberOfInstances": 2,
                "topologySpreadConstraint": []
            }
        }
        k8s.api.custom_objects_api.patch_namespaced_custom_object(
            "acid.zalan.do", "v1", "default",
            "postgresqls", "acid-minimal-cluster",
            patch_topologySpreadConstraint_config)
        self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")
        self.eventuallyEqual(lambda: k8s.count_pods_with_label(cluster_labels), 2, "Postgresql StatefulSet are scale to 2")
        self.eventuallyEqual(lambda: k8s.count_running_pods(), 2, "All pods are running")

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def test_zz_cluster_deletion(self):
        '''
           Test deletion with configured protection
        '''
        k8s = self.k8s
        cluster_label = 'application=spilo,cluster-name=acid-minimal-cluster'

        # configure delete protection
        patch_delete_annotations = {
            "data": {
                "delete_annotation_date_key": "delete-date",
                "delete_annotation_name_key": "delete-clustername",
                "enable_secrets_deletion": "false",
                "enable_persistent_volume_claim_deletion": "false"
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

            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            # check if everything has been deleted
            self.eventuallyEqual(lambda: k8s.count_pods_with_label(cluster_label), 0, "Pods not deleted")
            self.eventuallyEqual(lambda: k8s.count_services_with_label(cluster_label), 0, "Service not deleted")
            self.eventuallyEqual(lambda: k8s.count_endpoints_with_label(cluster_label), 0, "Endpoints not deleted")
            self.eventuallyEqual(lambda: k8s.count_statefulsets_with_label(cluster_label), 0, "Statefulset not deleted")
            self.eventuallyEqual(lambda: k8s.count_deployments_with_label(cluster_label), 0, "Deployments not deleted")
            self.eventuallyEqual(lambda: k8s.count_pdbs_with_label(cluster_label), 0, "Pod disruption budget not deleted")
            self.eventuallyEqual(lambda: k8s.count_secrets_with_label(cluster_label), 8, "Secrets were deleted although disabled in config")
            self.eventuallyEqual(lambda: k8s.count_pvcs_with_label(cluster_label), 2, "PVCs were deleted although disabled in config")

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

    def assert_master_is_unique(self, namespace='default', clusterName="acid-minimal-cluster"):
        '''
           Check that there is a single pod in the k8s cluster with the label "spilo-role=master"
           To be called manually after operations that affect pods
        '''
        k8s = self.k8s
        labels = 'spilo-role=master,cluster-name=' + clusterName

        num_of_master_pods = k8s.count_pods_with_label(labels, namespace)
        self.assertEqual(num_of_master_pods, 1, "Expected 1 master pod, found {}".format(num_of_master_pods))

    @timeout_decorator.timeout(TEST_TIMEOUT_SEC)
    def assert_distributed_pods(self, target_nodes, cluster_labels='cluster-name=acid-minimal-cluster'):
        '''
           Other tests can lead to the situation that master and replica are on the same node.
           Toggle pod anti affinty to distribute pods accross nodes (replica in particular).
        '''
        k8s = self.k8s
        cluster_labels = 'application=spilo,cluster-name=acid-minimal-cluster'

        # get nodes of master and replica(s)
        master_nodes, replica_nodes = k8s.get_cluster_nodes()
        self.assertNotEqual(master_nodes, [])
        self.assertNotEqual(replica_nodes, [])

        # if nodes are different we can quit here
        if master_nodes[0] not in replica_nodes:
            return True

        # enable pod anti affintiy in config map which should trigger movement of replica
        patch_enable_antiaffinity = {
            "data": {
                "enable_pod_antiaffinity": "true"
            }
        }

        try:
            k8s.update_config(patch_enable_antiaffinity, "enable antiaffinity")
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_labels)
            k8s.wait_for_running_pods(cluster_labels, 2)

            # now disable pod anti affintiy again which will cause yet another failover
            patch_disable_antiaffinity = {
                "data": {
                    "enable_pod_antiaffinity": "false"
                }
            }
            k8s.update_config(patch_disable_antiaffinity, "disable antiaffinity")
            self.eventuallyEqual(lambda: k8s.get_operator_state(), {"0": "idle"}, "Operator does not get in sync")

            k8s.wait_for_pod_start('spilo-role=replica,' + cluster_labels)
            k8s.wait_for_running_pods(cluster_labels, 2)

            master_nodes, replica_nodes = k8s.get_cluster_nodes()
            self.assertNotEqual(master_nodes, [])
            self.assertNotEqual(replica_nodes, [])

            # if nodes are different we can quit here
            for target_node in target_nodes:
                if (target_node not in master_nodes or target_node not in replica_nodes) and master_nodes[0] in replica_nodes:
                    print('Pods run on the same node')
                    return False

        except timeout_decorator.TimeoutError:
            print('Operator log: {}'.format(k8s.get_operator_log()))
            raise

        return True

    def check_cluster_child_resources_owner_references(self, cluster_name, cluster_namespace='default', inverse=False):
        k8s = self.k8s

        # check if child resources were updated with owner references
        sset = k8s.api.apps_v1.read_namespaced_stateful_set(cluster_name, cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(sset.metadata.owner_references, inverse), "statefulset owner reference check failed")

        svc = k8s.api.core_v1.read_namespaced_service(cluster_name, cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(svc.metadata.owner_references, inverse), "primary service owner reference check failed")
        replica_svc = k8s.api.core_v1.read_namespaced_service(cluster_name + "-repl", cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(replica_svc.metadata.owner_references, inverse), "replica service owner reference check failed")
        config_svc = k8s.api.core_v1.read_namespaced_service(cluster_name + "-config", cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(config_svc.metadata.owner_references, inverse), "config service owner reference check failed")

        ep = k8s.api.core_v1.read_namespaced_endpoints(cluster_name, cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(ep.metadata.owner_references, inverse), "primary endpoint owner reference check failed")
        replica_ep = k8s.api.core_v1.read_namespaced_endpoints(cluster_name + "-repl", cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(replica_ep.metadata.owner_references, inverse), "replica endpoint owner reference check failed")
        config_ep = k8s.api.core_v1.read_namespaced_endpoints(cluster_name + "-config", cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(config_ep.metadata.owner_references, inverse), "config endpoint owner reference check failed")

        pdb = k8s.api.policy_v1.read_namespaced_pod_disruption_budget("postgres-{}-pdb".format(cluster_name), cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(pdb.metadata.owner_references, inverse), "primary pod disruption budget owner reference check failed")

        pdb = k8s.api.policy_v1.read_namespaced_pod_disruption_budget("postgres-{}-critical-op-pdb".format(cluster_name), cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(pdb.metadata.owner_references, inverse), "pod disruption budget for critical operations owner reference check failed")

        pg_secret = k8s.api.core_v1.read_namespaced_secret("postgres.{}.credentials.postgresql.acid.zalan.do".format(cluster_name), cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(pg_secret.metadata.owner_references, inverse), "postgres secret owner reference check failed")
        standby_secret = k8s.api.core_v1.read_namespaced_secret("standby.{}.credentials.postgresql.acid.zalan.do".format(cluster_name), cluster_namespace)
        self.assertTrue(self.has_postgresql_owner_reference(standby_secret.metadata.owner_references, inverse), "standby secret owner reference check failed")

        return True

    def has_postgresql_owner_reference(self, owner_references, inverse):
        if inverse:
            return owner_references is None or owner_references[0].kind != 'postgresql'

        return owner_references is not None and owner_references[0].kind == 'postgresql' and owner_references[0].controller

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
            result = k8s.exec_with_kubectl(pod_name, q)
            db_list = clean_list(result.stdout.split(b'\n'))
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
            result = k8s.exec_with_kubectl(pod_name, q)
            result_set = clean_list(result.stdout.split(b'\n'))
        except Exception as ex:
            print('Error on query execution: {}'.format(ex))
            print('Stdout: {}'.format(result.stdout))
            print('Stderr: {}'.format(result.stderr))

        return result_set

    def query_database_with_user(self, pod_name, db_name, query, user_name):
        '''
           Query database and return result as a list
        '''
        k8s = self.k8s
        result_set = []
        exec_query = r"PGPASSWORD={} psql -h localhost -U {} -tAq -c \"{}\" -d {}"

        try:
            user_secret = k8s.get_secret(user_name)
            secret_user = str(base64.b64decode(user_secret.data["username"]), 'utf-8')
            secret_pw = str(base64.b64decode(user_secret.data["password"]), 'utf-8')
            q = exec_query.format(secret_pw, secret_user, query, db_name)
            q = "su postgres -c \"{}\"".format(q)
            result = k8s.exec_with_kubectl(pod_name, q)
            result_set = clean_list(result.stdout.split(b'\n'))
        except Exception as ex:
            print('Error on query execution: {}'.format(ex))
            print('Stdout: {}'.format(result.stdout))
            print('Stderr: {}'.format(result.stderr))

        return result_set

if __name__ == '__main__':
    unittest.main()
