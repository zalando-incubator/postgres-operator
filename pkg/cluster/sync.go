package cluster

import (
	"fmt"

	"k8s.io/api/core/v1"
	policybeta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/volumes"
)

// Sync syncs the cluster, making sure the actual Kubernetes objects correspond to what is defined in the manifest.
// Unlike the update, sync does not error out if some objects do not exist and takes care of creating them.
func (c *Cluster) Sync(newSpec *acidv1.Postgresql) error {
	var err error
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setSpec(newSpec)

	defer func() {
		if err != nil {
			c.logger.Warningf("error while syncing cluster state: %v", err)
			c.setStatus(acidv1.ClusterStatusSyncFailed)
		} else if c.Status != acidv1.ClusterStatusRunning {
			c.setStatus(acidv1.ClusterStatusRunning)
		}
	}()

	if err = c.initUsers(); err != nil {
		err = fmt.Errorf("could not init users: %v", err)
		return err
	}

	c.logger.Debugf("syncing secrets")

	//TODO: mind the secrets of the deleted/new users
	if err = c.syncSecrets(); err != nil {
		err = fmt.Errorf("could not sync secrets: %v", err)
		return err
	}

	c.logger.Debugf("syncing services")
	if err = c.syncServices(); err != nil {
		err = fmt.Errorf("could not sync services: %v", err)
		return err
	}

	// potentially enlarge volumes before changing the statefulset. By doing that
	// in this order we make sure the operator is not stuck waiting for a pod that
	// cannot start because it ran out of disk space.
	// TODO: handle the case of the cluster that is downsized and enlarged again
	// (there will be a volume from the old pod for which we can't act before the
	//  the statefulset modification is concluded)
	c.logger.Debugf("syncing persistent volumes")
	if err = c.syncVolumes(); err != nil {
		err = fmt.Errorf("could not sync persistent volumes: %v", err)
		return err
	}

	c.logger.Debugf("syncing statefulsets")
	if err = c.syncStatefulSet(); err != nil {
		if !k8sutil.ResourceAlreadyExists(err) {
			err = fmt.Errorf("could not sync statefulsets: %v", err)
			return err
		}
	}

	// create database objects unless we are running without pods or disabled that feature explicitly
	if !(c.databaseAccessDisabled() || c.getNumberOfInstances(&newSpec.Spec) <= 0) {
		c.logger.Debugf("syncing roles")
		if err = c.syncRoles(); err != nil {
			err = fmt.Errorf("could not sync roles: %v", err)
			return err
		}
		c.logger.Debugf("syncing databases")
		if err = c.syncDatabases(); err != nil {
			err = fmt.Errorf("could not sync databases: %v", err)
			return err
		}
	}

	c.logger.Debug("syncing pod disruption budgets")
	if err = c.syncPodDisruptionBudget(false); err != nil {
		err = fmt.Errorf("could not sync pod disruption budget: %v", err)
		return err
	}

	return err
}

func (c *Cluster) syncServices() error {
	for _, role := range []PostgresRole{Master, Replica} {
		c.logger.Debugf("syncing %s service", role)

		if err := c.syncEndpoint(role); err != nil {
			return fmt.Errorf("could not sync %s endpont: %v", role, err)
		}

		if err := c.syncService(role); err != nil {
			return fmt.Errorf("could not sync %s service: %v", role, err)
		}
	}

	return nil
}

func (c *Cluster) syncService(role PostgresRole) error {
	var (
		svc *v1.Service
		err error
	)
	c.setProcessName("syncing %s service", role)

	if svc, err = c.KubeClient.Services(c.Namespace).Get(c.serviceName(role), metav1.GetOptions{}); err == nil {
		c.Services[role] = svc
		desiredSvc := c.generateService(role, &c.Spec)
		if match, reason := k8sutil.SameService(svc, desiredSvc); !match {
			c.logServiceChanges(role, svc, desiredSvc, false, reason)
			if err = c.updateService(role, desiredSvc); err != nil {
				return fmt.Errorf("could not update %s service to match desired state: %v", role, err)
			}
			c.logger.Infof("%s service %q is in the desired state now", role, util.NameFromMeta(desiredSvc.ObjectMeta))
		}
		return nil
	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get %s service: %v", role, err)
	}
	// no existing service, create new one
	c.Services[role] = nil
	c.logger.Infof("could not find the cluster's %s service", role)

	if svc, err = c.createService(role); err == nil {
		c.logger.Infof("created missing %s service %q", role, util.NameFromMeta(svc.ObjectMeta))
	} else {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create missing %s service: %v", role, err)
		}
		c.logger.Infof("%s service %q already exists", role, util.NameFromMeta(svc.ObjectMeta))
		if svc, err = c.KubeClient.Services(c.Namespace).Get(c.serviceName(role), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %s service: %v", role, err)
		}
	}
	c.Services[role] = svc
	return nil
}

func (c *Cluster) syncEndpoint(role PostgresRole) error {
	var (
		ep  *v1.Endpoints
		err error
	)
	c.setProcessName("syncing %s endpoint", role)

	if ep, err = c.KubeClient.Endpoints(c.Namespace).Get(c.endpointName(role), metav1.GetOptions{}); err == nil {
		// TODO: No syncing of endpoints here, is this covered completely by updateService?
		c.Endpoints[role] = ep
		return nil
	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get %s endpoint: %v", role, err)
	}
	// no existing endpoint, create new one
	c.Endpoints[role] = nil
	c.logger.Infof("could not find the cluster's %s endpoint", role)

	if ep, err = c.createEndpoint(role); err == nil {
		c.logger.Infof("created missing %s endpoint %q", role, util.NameFromMeta(ep.ObjectMeta))
	} else {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create missing %s endpoint: %v", role, err)
		}
		c.logger.Infof("%s endpoint %q already exists", role, util.NameFromMeta(ep.ObjectMeta))
		if ep, err = c.KubeClient.Endpoints(c.Namespace).Get(c.endpointName(role), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %s endpoint: %v", role, err)
		}
	}
	c.Endpoints[role] = ep
	return nil
}

func (c *Cluster) syncPodDisruptionBudget(isUpdate bool) error {
	var (
		pdb *policybeta1.PodDisruptionBudget
		err error
	)
	if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(c.podDisruptionBudgetName(), metav1.GetOptions{}); err == nil {
		c.PodDisruptionBudget = pdb
		newPDB := c.generatePodDisruptionBudget()
		if match, reason := k8sutil.SamePDB(pdb, newPDB); !match {
			c.logPDBChanges(pdb, newPDB, isUpdate, reason)
			if err = c.updatePodDisruptionBudget(newPDB); err != nil {
				return err
			}
		} else {
			c.PodDisruptionBudget = pdb
		}
		return nil

	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get pod disruption budget: %v", err)
	}
	// no existing pod disruption budget, create new one
	c.PodDisruptionBudget = nil
	c.logger.Infof("could not find the cluster's pod disruption budget")

	if pdb, err = c.createPodDisruptionBudget(); err != nil {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create pod disruption budget: %v", err)
		}
		c.logger.Infof("pod disruption budget %q already exists", util.NameFromMeta(pdb.ObjectMeta))
		if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(c.podDisruptionBudgetName(), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %q pod disruption budget", util.NameFromMeta(pdb.ObjectMeta))
		}
	}

	c.logger.Infof("created missing pod disruption budget %q", util.NameFromMeta(pdb.ObjectMeta))
	c.PodDisruptionBudget = pdb

	return nil
}

func (c *Cluster) syncStatefulSet() error {
	var (
		podsRollingUpdateRequired bool
	)
	// NB: Be careful to consider the codepath that acts on podsRollingUpdateRequired before returning early.
	sset, err := c.KubeClient.StatefulSets(c.Namespace).Get(c.statefulSetName(), metav1.GetOptions{})
	if err != nil {
		if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not get statefulset: %v", err)
		}
		// statefulset does not exist, try to re-create it
		c.Statefulset = nil
		c.logger.Infof("could not find the cluster's statefulset")
		pods, err := c.listPods()
		if err != nil {
			return fmt.Errorf("could not list pods of the statefulset: %v", err)
		}

		sset, err = c.createStatefulSet()
		if err != nil {
			return fmt.Errorf("could not create missing statefulset: %v", err)
		}

		if err = c.waitStatefulsetPodsReady(); err != nil {
			return fmt.Errorf("cluster is not ready: %v", err)
		}

		podsRollingUpdateRequired = (len(pods) > 0)
		if podsRollingUpdateRequired {
			c.logger.Warningf("found pods from the previous statefulset: trigger rolling update")
			if err := c.applyRollingUpdateFlagforStatefulSet(podsRollingUpdateRequired); err != nil {
				return fmt.Errorf("could not set rolling update flag for the statefulset: %v", err)
			}
		}
		c.logger.Infof("created missing statefulset %q", util.NameFromMeta(sset.ObjectMeta))

	} else {
		podsRollingUpdateRequired = c.mergeRollingUpdateFlagUsingCache(sset)
		// statefulset is already there, make sure we use its definition in order to compare with the spec.
		c.Statefulset = sset

		desiredSS, err := c.generateStatefulSet(&c.Spec)
		if err != nil {
			return fmt.Errorf("could not generate statefulset: %v", err)
		}
		c.setRollingUpdateFlagForStatefulSet(desiredSS, podsRollingUpdateRequired)

		cmp := c.compareStatefulSetWith(desiredSS)
		if !cmp.match {
			if cmp.rollingUpdate && !podsRollingUpdateRequired {
				podsRollingUpdateRequired = true
				c.setRollingUpdateFlagForStatefulSet(desiredSS, podsRollingUpdateRequired)
			}

			c.logStatefulSetChanges(c.Statefulset, desiredSS, false, cmp.reasons)

			if !cmp.replace {
				if err := c.updateStatefulSet(desiredSS); err != nil {
					return fmt.Errorf("could not update statefulset: %v", err)
				}
			} else {
				if err := c.replaceStatefulSet(desiredSS); err != nil {
					return fmt.Errorf("could not replace statefulset: %v", err)
				}
			}
		}
	}

	// Apply special PostgreSQL parameters that can only be set via the Patroni API.
	// it is important to do it after the statefulset pods are there, but before the rolling update
	// since those parameters require PostgreSQL restart.
	if err := c.checkAndSetGlobalPostgreSQLConfiguration(); err != nil {
		return fmt.Errorf("could not set cluster-wide PostgreSQL configuration options: %v", err)
	}

	// if we get here we also need to re-create the pods (either leftovers from the old
	// statefulset or those that got their configuration from the outdated statefulset)
	if podsRollingUpdateRequired {
		c.logger.Debugln("performing rolling update")
		if err := c.recreatePods(); err != nil {
			return fmt.Errorf("could not recreate pods: %v", err)
		}
		c.logger.Infof("pods have been recreated")
		if err := c.applyRollingUpdateFlagforStatefulSet(false); err != nil {
			c.logger.Warningf("could not clear rolling update for the statefulset: %v", err)
		}
	}
	return nil
}

// checkAndSetGlobalPostgreSQLConfiguration checks whether cluster-wide API parameters
// (like max_connections) has changed and if necessary sets it via the Patroni API
func (c *Cluster) checkAndSetGlobalPostgreSQLConfiguration() error {
	var (
		err  error
		pods []v1.Pod
	)

	// we need to extract those options from the cluster manifest.
	optionsToSet := make(map[string]string)
	pgOptions := c.Spec.Parameters

	for k, v := range pgOptions {
		if isBootstrapOnlyParameter(k) {
			optionsToSet[k] = v
		}
	}

	if len(optionsToSet) == 0 {
		return nil
	}

	if pods, err = c.listPods(); err != nil {
		return err
	}
	if len(pods) == 0 {
		return fmt.Errorf("could not call Patroni API: cluster has no pods")
	}
	// try all pods until the first one that is successful, as it doesn't matter which pod
	// carries the request to change configuration through
	for _, pod := range pods {
		podName := util.NameFromMeta(pod.ObjectMeta)
		c.logger.Debugf("calling Patroni API on a pod %s to set the following Postgres options: %v",
			podName, optionsToSet)
		if err = c.patroni.SetPostgresParameters(&pod, optionsToSet); err == nil {
			return nil
		}
		c.logger.Warningf("could not patch postgres parameters with a pod %s: %v", podName, err)
	}
	return fmt.Errorf("could not reach Patroni API to set Postgres options: failed on every pod (%d total)",
		len(pods))
}

func (c *Cluster) syncSecrets() error {
	var (
		err    error
		secret *v1.Secret
	)
	c.setProcessName("syncing secrets")
	secrets := c.generateUserSecrets()

	for secretUsername, secretSpec := range secrets {
		if secret, err = c.KubeClient.Secrets(secretSpec.Namespace).Create(secretSpec); err == nil {
			c.Secrets[secret.UID] = secret
			c.logger.Debugf("created new secret %q, uid: %q", util.NameFromMeta(secret.ObjectMeta), secret.UID)
			continue
		}
		if k8sutil.ResourceAlreadyExists(err) {
			var userMap map[string]spec.PgUser
			if secret, err = c.KubeClient.Secrets(secretSpec.Namespace).Get(secretSpec.Name, metav1.GetOptions{}); err != nil {
				return fmt.Errorf("could not get current secret: %v", err)
			}
			if secretUsername != string(secret.Data["username"]) {
				c.logger.Warningf("secret %q does not contain the role %q", secretSpec.Name, secretUsername)
				continue
			}
			c.logger.Debugf("secret %q already exists, fetching its password", util.NameFromMeta(secret.ObjectMeta))
			if secretUsername == c.systemUsers[constants.SuperuserKeyName].Name {
				secretUsername = constants.SuperuserKeyName
				userMap = c.systemUsers
			} else if secretUsername == c.systemUsers[constants.ReplicationUserKeyName].Name {
				secretUsername = constants.ReplicationUserKeyName
				userMap = c.systemUsers
			} else {
				userMap = c.pgUsers
			}
			pwdUser := userMap[secretUsername]
			// if this secret belongs to the infrastructure role and the password has changed - replace it in the secret
			if pwdUser.Password != string(secret.Data["password"]) && pwdUser.Origin == spec.RoleOriginInfrastructure {
				c.logger.Debugf("updating the secret %q from the infrastructure roles", secretSpec.Name)
				if _, err = c.KubeClient.Secrets(secretSpec.Namespace).Update(secretSpec); err != nil {
					return fmt.Errorf("could not update infrastructure role secret for role %q: %v", secretUsername, err)
				}
			} else {
				// for non-infrastructure role - update the role with the password from the secret
				pwdUser.Password = string(secret.Data["password"])
				userMap[secretUsername] = pwdUser
			}
		} else {
			return fmt.Errorf("could not create secret for user %q: %v", secretUsername, err)
		}
	}

	return nil
}

func (c *Cluster) syncRoles() (err error) {
	c.setProcessName("syncing roles")

	var (
		dbUsers   spec.PgUserMap
		userNames []string
	)

	err = c.initDbConn()
	if err != nil {
		return fmt.Errorf("could not init db connection: %v", err)
	}

	defer func() {
		if err2 := c.closeDbConn(); err2 != nil {
			if err == nil {
				err = fmt.Errorf("could not close database connection: %v", err2)
			} else {
				err = fmt.Errorf("could not close database connection: %v (prior error: %v)", err2, err)
			}
		}
	}()

	for _, u := range c.pgUsers {
		userNames = append(userNames, u.Name)
	}
	dbUsers, err = c.readPgUsersFromDatabase(userNames)
	if err != nil {
		return fmt.Errorf("error getting users from the database: %v", err)
	}

	pgSyncRequests := c.userSyncStrategy.ProduceSyncRequests(dbUsers, c.pgUsers)
	if err = c.userSyncStrategy.ExecuteSyncRequests(pgSyncRequests, c.pgDb); err != nil {
		return fmt.Errorf("error executing sync statements: %v", err)
	}

	return nil
}

// syncVolumes reads all persistent volumes and checks that their size matches the one declared in the statefulset.
func (c *Cluster) syncVolumes() error {
	c.setProcessName("syncing volumes")

	act, err := c.volumesNeedResizing(c.Spec.Volume)
	if err != nil {
		return fmt.Errorf("could not compare size of the volumes: %v", err)
	}
	if !act {
		return nil
	}
	if err := c.resizeVolumes(c.Spec.Volume, []volumes.VolumeResizer{&volumes.EBSVolumeResizer{AWSRegion: c.OpConfig.AWSRegion}}); err != nil {
		return fmt.Errorf("could not sync volumes: %v", err)
	}

	c.logger.Infof("volumes have been synced successfully")

	return nil
}

func (c *Cluster) syncDatabases() error {
	c.setProcessName("syncing databases")

	createDatabases := make(map[string]string)
	alterOwnerDatabases := make(map[string]string)

	if err := c.initDbConn(); err != nil {
		return fmt.Errorf("could not init database connection")
	}
	defer func() {
		if err := c.closeDbConn(); err != nil {
			c.logger.Errorf("could not close database connection: %v", err)
		}
	}()

	currentDatabases, err := c.getDatabases()
	if err != nil {
		return fmt.Errorf("could not get current databases: %v", err)
	}

	for datname, newOwner := range c.Spec.Databases {
		currentOwner, exists := currentDatabases[datname]
		if !exists {
			createDatabases[datname] = newOwner
		} else if currentOwner != newOwner {
			alterOwnerDatabases[datname] = newOwner
		}
	}

	if len(createDatabases)+len(alterOwnerDatabases) == 0 {
		return nil
	}

	for datname, owner := range createDatabases {
		if err = c.executeCreateDatabase(datname, owner); err != nil {
			return err
		}
	}
	for datname, owner := range alterOwnerDatabases {
		if err = c.executeAlterDatabaseOwner(datname, owner); err != nil {
			return err
		}
	}

	return nil
}
