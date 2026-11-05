package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/sirupsen/logrus"
	acidzalando "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/nicediff"
	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
)

// OAuthTokenGetter provides the method for fetching OAuth tokens
type OAuthTokenGetter interface {
	getOAuthToken() (string, error)
}

// SecretOauthTokenGetter enables fetching OAuth tokens by reading Kubernetes secrets
type SecretOauthTokenGetter struct {
	kubeClient           *k8sutil.KubernetesClient
	OAuthTokenSecretName spec.NamespacedName
}

func newSecretOauthTokenGetter(kubeClient *k8sutil.KubernetesClient,
	OAuthTokenSecretName spec.NamespacedName) *SecretOauthTokenGetter {
	return &SecretOauthTokenGetter{kubeClient, OAuthTokenSecretName}
}

func (g *SecretOauthTokenGetter) getOAuthToken() (string, error) {
	//TODO: we can move this function to the Controller in case it will be needed there. As for now we use it only in the Cluster
	// Temporary getting postgresql-operator secret from the NamespaceDefault
	credentialsSecret, err := g.kubeClient.
		Secrets(g.OAuthTokenSecretName.Namespace).
		Get(context.TODO(), g.OAuthTokenSecretName.Name, metav1.GetOptions{})

	if err != nil {
		return "", fmt.Errorf("could not get credentials secret: %v", err)
	}
	data := credentialsSecret.Data

	if string(data["read-only-token-type"]) != "Bearer" {
		return "", fmt.Errorf("wrong token type: %v", data["read-only-token-type"])
	}

	return string(data["read-only-token-secret"]), nil
}

func isValidUsername(username string) bool {
	return userRegexp.MatchString(username)
}

func (c *Cluster) isProtectedUsername(username string) bool {
	for _, protected := range c.OpConfig.ProtectedRoles {
		if username == protected {
			return true
		}
	}
	return false
}

func (c *Cluster) isSystemUsername(username string) bool {
	// is there a pooler system user defined
	for _, systemUser := range c.systemUsers {
		if username == systemUser.Name {
			return true
		}
	}

	return false
}

func isValidFlag(flag string) bool {
	for _, validFlag := range []string{constants.RoleFlagSuperuser, constants.RoleFlagLogin, constants.RoleFlagCreateDB,
		constants.RoleFlagInherit, constants.RoleFlagReplication, constants.RoleFlagByPassRLS,
		constants.RoleFlagCreateRole} {
		if flag == validFlag || flag == "NO"+validFlag {
			return true
		}
	}
	return false
}

func invertFlag(flag string) string {
	if flag[:2] == "NO" {
		return flag[2:]
	}
	return "NO" + flag
}

func normalizeUserFlags(userFlags []string) ([]string, error) {
	uniqueFlags := make(map[string]bool)
	addLogin := true

	for _, flag := range userFlags {
		if !alphaNumericRegexp.MatchString(flag) {
			return nil, fmt.Errorf("user flag %q is not alphanumeric", flag)
		}

		flag = strings.ToUpper(flag)
		if _, ok := uniqueFlags[flag]; !ok {
			if !isValidFlag(flag) {
				return nil, fmt.Errorf("user flag %q is not valid", flag)
			}
			invFlag := invertFlag(flag)
			if uniqueFlags[invFlag] {
				return nil, fmt.Errorf("conflicting user flags: %q and %q", flag, invFlag)
			}
			uniqueFlags[flag] = true
		}
	}

	flags := []string{}
	for k := range uniqueFlags {
		if k == constants.RoleFlagNoLogin || k == constants.RoleFlagLogin {
			addLogin = false
			if k == constants.RoleFlagNoLogin {
				// we don't add NOLOGIN to the list of flags to be consistent with what we get
				// from the readPgUsersFromDatabase in SyncUsers
				continue
			}
		}
		flags = append(flags, k)
	}
	if addLogin {
		flags = append(flags, constants.RoleFlagLogin)
	}
	sort.Strings(flags)
	return flags, nil
}

// specPatch produces a JSON of the Kubernetes object specification passed (typically service or
// statefulset) to use it in a MergePatch.
func specPatch(spec interface{}) ([]byte, error) {
	return json.Marshal(struct {
		Spec interface{} `json:"spec"`
	}{spec})
}

// metaAnnotationsPatch produces a JSON of the object metadata that has only the annotation
// field in order to use it in a MergePatch. Note that we don't patch the complete metadata, since
// it contains the current revision of the object that could be outdated at the time we patch.
func metaAnnotationsPatch(annotations map[string]string) ([]byte, error) {
	var meta metav1.ObjectMeta
	meta.Annotations = annotations
	return json.Marshal(struct {
		ObjMeta interface{} `json:"metadata"`
	}{&meta})
}

func metaLabelsPatch(labels map[string]string) ([]byte, error) {
	var meta metav1.ObjectMeta
	meta.Labels = labels
	return json.Marshal(struct {
		ObjMeta interface{} `json:"metadata"`
	}{&meta})
}

func (c *Cluster) logPDBChanges(old, new *policyv1.PodDisruptionBudget, isUpdate bool, reason string) {
	if isUpdate {
		c.logger.Infof("pod disruption budget %q has been changed", util.NameFromMeta(old.ObjectMeta))
	} else {
		c.logger.Infof("pod disruption budget %q is not in the desired state and needs to be updated",
			util.NameFromMeta(old.ObjectMeta),
		)
	}

	logNiceDiff(c.logger, old.Spec, new.Spec)

	if reason != "" {
		c.logger.Infof("reason: %s", reason)
	}
}

func logNiceDiff(log *logrus.Entry, old, new interface{}) {
	o, erro := json.MarshalIndent(old, "", "  ")
	n, errn := json.MarshalIndent(new, "", "  ")

	if erro != nil || errn != nil {
		panic("could not marshal API objects, should not happen")
	}

	nice := nicediff.Diff(string(o), string(n), true)
	for _, s := range strings.Split(nice, "\n") {
		// " is not needed in the value to understand
		log.Debug(strings.ReplaceAll(s, "\"", ""))
	}
}

func (c *Cluster) logStatefulSetChanges(old, new *appsv1.StatefulSet, isUpdate bool, reasons []string) {
	if isUpdate {
		c.logger.Infof("statefulset %s has been changed", util.NameFromMeta(old.ObjectMeta))
	} else {
		c.logger.Infof("statefulset %s is not in the desired state and needs to be updated",
			util.NameFromMeta(old.ObjectMeta),
		)
	}

	logNiceDiff(c.logger, old.Spec, new.Spec)

	if !reflect.DeepEqual(old.Annotations, new.Annotations) {
		c.logger.Debug("metadata.annotation are different")
		logNiceDiff(c.logger, old.Annotations, new.Annotations)
	}

	if len(reasons) > 0 {
		for _, reason := range reasons {
			c.logger.Infof("reason: %s", reason)
		}
	}
}

func (c *Cluster) logServiceChanges(role PostgresRole, old, new *v1.Service, isUpdate bool, reason string) {
	if isUpdate {
		c.logger.Infof("%s service %s has been changed",
			role, util.NameFromMeta(old.ObjectMeta),
		)
	} else {
		c.logger.Infof("%s service %s is not in the desired state and needs to be updated",
			role, util.NameFromMeta(old.ObjectMeta),
		)
	}

	logNiceDiff(c.logger, old.Spec, new.Spec)

	if reason != "" {
		c.logger.Infof("reason: %s", reason)
	}
}

func getPostgresContainer(podSpec *v1.PodSpec) (pgContainer v1.Container) {
	for _, container := range podSpec.Containers {
		if container.Name == constants.PostgresContainerName {
			pgContainer = container
		}
	}

	// if no postgres container was found, take the first one in the podSpec
	if reflect.DeepEqual(pgContainer, v1.Container{}) && len(podSpec.Containers) > 0 {
		pgContainer = podSpec.Containers[0]
	}
	return pgContainer
}

func (c *Cluster) getTeamMembers(teamID string) ([]string, error) {

	if teamID == "" {
		msg := "no teamId specified"
		if c.OpConfig.EnableTeamIdClusternamePrefix {
			return nil, fmt.Errorf("%s", msg)
		}
		c.logger.Warnf("%s", msg)
		return nil, nil
	}

	members := []string{}

	if c.OpConfig.EnablePostgresTeamCRD && c.Config.PgTeamMap != nil {
		c.logger.Debugf("fetching possible additional team members for team %q", teamID)
		additionalMembers := []string{}

		for team, membership := range *c.Config.PgTeamMap {
			if team == teamID {
				additionalMembers = membership.AdditionalMembers
				c.logger.Debugf("found %d additional members for team %q", len(additionalMembers), teamID)
			}
		}

		members = append(members, additionalMembers...)
	}

	if !c.OpConfig.EnableTeamsAPI {
		c.logger.Debug("team API is disabled")
		return members, nil
	}

	token, err := c.oauthTokenGetter.getOAuthToken()
	if err != nil {
		return nil, fmt.Errorf("could not get oauth token to authenticate to team service API: %v", err)
	}

	teamInfo, statusCode, err := c.teamsAPIClient.TeamInfo(teamID, token)

	if err != nil {
		if statusCode == http.StatusNotFound {
			c.logger.Warningf("could not get team info for team %q: %v", teamID, err)
		} else {
			return nil, fmt.Errorf("could not get team info for team %q: %v", teamID, err)
		}
	} else {
		for _, member := range teamInfo.Members {
			if !(util.SliceContains(members, member)) {
				members = append(members, member)
			}
		}
	}
	return members, nil
}

// Returns annotations to be passed to child objects
func (c *Cluster) annotationsSet(annotations map[string]string) map[string]string {

	if annotations == nil {
		annotations = make(map[string]string)
	}

	pgCRDAnnotations := c.ObjectMeta.Annotations

	// allow to inherit certain labels from the 'postgres' object
	for k, v := range pgCRDAnnotations {
		for _, match := range c.OpConfig.InheritedAnnotations {
			if k == match {
				annotations[k] = v
			}
		}
	}

	if len(annotations) > 0 {
		return annotations
	}

	return nil
}

func (c *Cluster) waitForPodLabel(podEvents chan PodEvent, stopCh chan struct{}, role *PostgresRole) (*v1.Pod, error) {
	timeout := time.After(c.OpConfig.PodLabelWaitTimeout.Duration)
	for {
		select {
		case podEvent := <-podEvents:
			podRole := PostgresRole(podEvent.CurPod.Labels[c.OpConfig.PodRoleLabel])

			if role == nil {
				if podRole == Master || podRole == Replica {
					return podEvent.CurPod, nil
				}
			} else if *role == podRole {
				return podEvent.CurPod, nil
			}
		case <-timeout:
			return nil, fmt.Errorf("pod label wait timeout")
		case <-stopCh:
			return nil, fmt.Errorf("pod label wait cancelled")
		}
	}
}

func (c *Cluster) waitForPodDeletion(podEvents chan PodEvent) error {
	timeout := time.After(c.OpConfig.PodDeletionWaitTimeout.Duration)
	for {
		select {
		case podEvent := <-podEvents:
			if podEvent.EventType == PodEventDelete {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("pod deletion wait timeout")
		}
	}
}

func (c *Cluster) waitStatefulsetReady() error {
	return retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			listOptions := metav1.ListOptions{
				LabelSelector: c.labelsSet(false).String(),
			}
			ss, err := c.KubeClient.StatefulSets(c.Namespace).List(context.TODO(), listOptions)
			if err != nil {
				return false, err
			}

			if len(ss.Items) != 1 {
				return false, fmt.Errorf("statefulset is not found")
			}

			return *ss.Items[0].Spec.Replicas == ss.Items[0].Status.Replicas, nil
		})
}

func (c *Cluster) _waitPodLabelsReady(anyReplica bool) error {
	var (
		podsNumber int
	)
	ls := c.labelsSet(false)
	namespace := c.Namespace

	listOptions := metav1.ListOptions{
		LabelSelector: ls.String(),
	}
	masterListOption := metav1.ListOptions{
		LabelSelector: labels.Merge(ls, labels.Set{
			c.OpConfig.PodRoleLabel: string(Master),
		}).String(),
	}
	replicaListOption := metav1.ListOptions{
		LabelSelector: labels.Merge(ls, labels.Set{
			c.OpConfig.PodRoleLabel: string(Replica),
		}).String(),
	}
	podsNumber = 1
	if !anyReplica {
		pods, err := c.KubeClient.Pods(namespace).List(context.TODO(), listOptions)
		if err != nil {
			return err
		}
		podsNumber = len(pods.Items)
		c.logger.Debugf("Waiting for %d pods to become ready", podsNumber)
	} else {
		c.logger.Debug("Waiting for any replica pod to become ready")
	}

	err := retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			masterCount := 0
			if !anyReplica {
				masterPods, err2 := c.KubeClient.Pods(namespace).List(context.TODO(), masterListOption)
				if err2 != nil {
					return false, err2
				}
				if len(masterPods.Items) > 1 {
					return false, fmt.Errorf("too many masters (%d pods with the master label found)",
						len(masterPods.Items))
				}
				masterCount = len(masterPods.Items)
			}
			replicaPods, err2 := c.KubeClient.Pods(namespace).List(context.TODO(), replicaListOption)
			if err2 != nil {
				return false, err2
			}
			replicaCount := len(replicaPods.Items)
			if anyReplica && replicaCount > 0 {
				c.logger.Debugf("Found %d running replica pods", replicaCount)
				return true, nil
			}

			return masterCount+replicaCount >= podsNumber, nil
		})

	return err
}

func (c *Cluster) waitForAllPodsLabelReady() error {
	return c._waitPodLabelsReady(false)
}

func (c *Cluster) waitStatefulsetPodsReady() error {
	c.setProcessName("waiting for the pods of the statefulset")
	// TODO: wait for the first Pod only
	if err := c.waitStatefulsetReady(); err != nil {
		return fmt.Errorf("stateful set error: %v", err)
	}

	// TODO: wait only for master
	if err := c.waitForAllPodsLabelReady(); err != nil {
		return fmt.Errorf("pod labels error: %v", err)
	}

	return nil
}

// Returns labels used to create or list k8s objects such as pods
// For backward compatibility, shouldAddExtraLabels must be false
// when listing k8s objects. See operator PR #252
func (c *Cluster) labelsSet(shouldAddExtraLabels bool) labels.Set {
	lbls := make(map[string]string)
	for k, v := range c.OpConfig.ClusterLabels {
		lbls[k] = v
	}
	lbls[c.OpConfig.ClusterNameLabel] = c.Name

	if shouldAddExtraLabels {
		// enables filtering resources owned by a team
		lbls["team"] = c.Postgresql.Spec.TeamID

		// allow to inherit certain labels from the 'postgres' object
		if spec, err := c.GetSpec(); err == nil {
			for k, v := range spec.ObjectMeta.Labels {
				for _, match := range c.OpConfig.InheritedLabels {
					if k == match {
						lbls[k] = v
					}
				}
			}
		} else {
			c.logger.Warningf("could not get the list of InheritedLabels for cluster %q: %v", c.Name, err)
		}
	}

	return labels.Set(lbls)
}

func (c *Cluster) labelsSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchLabels:      c.labelsSet(false),
		MatchExpressions: nil,
	}
}

func (c *Cluster) roleLabelsSet(shouldAddExtraLabels bool, role PostgresRole) labels.Set {
	lbls := c.labelsSet(shouldAddExtraLabels)
	lbls[c.OpConfig.PodRoleLabel] = string(role)
	return lbls
}

func (c *Cluster) dnsName(role PostgresRole) string {
	var dnsString, oldDnsString string

	if role == Master {
		dnsString = c.masterDNSName(c.Name)
	} else {
		dnsString = c.replicaDNSName(c.Name)
	}

	// if cluster name starts with teamID we might need to provide backwards compatibility
	clusterNameWithoutTeamPrefix, _ := acidv1.ExtractClusterName(c.Name, c.Spec.TeamID)
	if clusterNameWithoutTeamPrefix != "" {
		if role == Master {
			oldDnsString = c.oldMasterDNSName(clusterNameWithoutTeamPrefix)
		} else {
			oldDnsString = c.oldReplicaDNSName(clusterNameWithoutTeamPrefix)
		}
		dnsString = fmt.Sprintf("%s,%s", dnsString, oldDnsString)
	}

	return dnsString
}

func (c *Cluster) masterDNSName(clusterName string) string {
	return strings.ToLower(c.OpConfig.MasterDNSNameFormat.Format(
		"cluster", clusterName,
		"namespace", c.Namespace,
		"team", c.teamName(),
		"hostedzone", c.OpConfig.DbHostedZone))
}

func (c *Cluster) replicaDNSName(clusterName string) string {
	return strings.ToLower(c.OpConfig.ReplicaDNSNameFormat.Format(
		"cluster", clusterName,
		"namespace", c.Namespace,
		"team", c.teamName(),
		"hostedzone", c.OpConfig.DbHostedZone))
}

func (c *Cluster) oldMasterDNSName(clusterName string) string {
	return strings.ToLower(c.OpConfig.MasterLegacyDNSNameFormat.Format(
		"cluster", clusterName,
		"team", c.teamName(),
		"hostedzone", c.OpConfig.DbHostedZone))
}

func (c *Cluster) oldReplicaDNSName(clusterName string) string {
	return strings.ToLower(c.OpConfig.ReplicaLegacyDNSNameFormat.Format(
		"cluster", clusterName,
		"team", c.teamName(),
		"hostedzone", c.OpConfig.DbHostedZone))
}

func (c *Cluster) credentialSecretName(username string) string {
	return c.credentialSecretNameForCluster(username, c.Name)
}

func (c *Cluster) credentialSecretNameForCluster(username string, clusterName string) string {
	// secret  must consist of lower case alphanumeric characters, '-' or '.',
	// and must start and end with an alphanumeric character

	return c.OpConfig.SecretNameTemplate.Format(
		"username", strings.Replace(username, "_", "-", -1),
		"cluster", clusterName,
		"tprkind", acidv1.PostgresCRDResourceKind,
		"tprgroup", acidzalando.GroupName)
}

func cloneSpec(from *acidv1.Postgresql) (*acidv1.Postgresql, error) {
	var (
		buf    bytes.Buffer
		result *acidv1.Postgresql
		err    error
	)
	enc := gob.NewEncoder(&buf)
	if err = enc.Encode(*from); err != nil {
		return nil, fmt.Errorf("could not encode the spec: %v", err)
	}
	dec := gob.NewDecoder(&buf)
	if err = dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("could not decode the spec: %v", err)
	}
	return result, nil
}

func (c *Cluster) setSpec(newSpec *acidv1.Postgresql) {
	c.specMu.Lock()
	c.Postgresql = *newSpec
	c.specMu.Unlock()
}

// GetSpec returns a copy of the operator-side spec of a Postgres cluster in a thread-safe manner
func (c *Cluster) GetSpec() (*acidv1.Postgresql, error) {
	c.specMu.RLock()
	defer c.specMu.RUnlock()
	return cloneSpec(&c.Postgresql)
}

func (c *Cluster) patroniUsesKubernetes() bool {
	return c.OpConfig.EtcdHost == ""
}

func (c *Cluster) patroniKubernetesUseConfigMaps() bool {
	if !c.patroniUsesKubernetes() {
		return false
	}
	if c.OpConfig.KubernetesUseConfigMaps == nil {
		return true
	}

	// otherwise, follow the operator configuration
	return *c.OpConfig.KubernetesUseConfigMaps
}

// Earlier arguments take priority
func mergeContainers(containers ...[]v1.Container) ([]v1.Container, []string) {
	containerNameTaken := map[string]bool{}
	result := make([]v1.Container, 0)
	conflicts := make([]string, 0)

	for _, containerArray := range containers {
		for _, container := range containerArray {
			if _, taken := containerNameTaken[container.Name]; taken {
				conflicts = append(conflicts, container.Name)
			} else {
				containerNameTaken[container.Name] = true
				result = append(result, container)
			}
		}
	}
	return result, conflicts
}

func trimCronjobName(name string) string {
	maxLength := 52
	if len(name) > maxLength {
		name = name[0:maxLength]
		name = strings.TrimRight(name, "-")
	}
	return name
}

func parseResourceRequirements(resourcesRequirement v1.ResourceRequirements) (acidv1.Resources, error) {
	var resources acidv1.Resources
	resourcesJSON, err := json.Marshal(resourcesRequirement)
	if err != nil {
		return acidv1.Resources{}, fmt.Errorf("could not marshal K8s resources requirements")
	}
	if err = json.Unmarshal(resourcesJSON, &resources); err != nil {
		return acidv1.Resources{}, fmt.Errorf("could not unmarshal K8s resources requirements into acidv1.Resources struct")
	}
	return resources, nil
}

func isStandbyCluster(spec *acidv1.PostgresSpec) bool {
	for _, env := range spec.Env {
		hasStandbyEnv, _ := regexp.MatchString(`^STANDBY_WALE_(S3|GS|GSC|SWIFT)_PREFIX$`, env.Name)
		if hasStandbyEnv && env.Value != "" {
			return true
		}
	}
	return spec.StandbyCluster != nil
}

func (c *Cluster) isInMaintenanceWindow(specMaintenanceWindows []acidv1.MaintenanceWindow) bool {
	ignoreMaintenanceWindows := c.OpConfig.EnableMaintenanceWindows != nil && !*c.OpConfig.EnableMaintenanceWindows
	noWindowsDefined := len(specMaintenanceWindows) == 0 && len(c.OpConfig.MaintenanceWindows) == 0
	if noWindowsDefined || ignoreMaintenanceWindows {
		return true
	}
	now := time.Now()
	currentDay := now.Weekday()
	currentTime := now.Format("15:04")

	maintenanceWindows := specMaintenanceWindows
	if len(maintenanceWindows) == 0 {
		maintenanceWindows = make([]acidv1.MaintenanceWindow, 0, len(c.OpConfig.MaintenanceWindows))
		for _, windowStr := range c.OpConfig.MaintenanceWindows {
			var window acidv1.MaintenanceWindow
			if err := window.UnmarshalJSON([]byte(windowStr)); err != nil {
				c.logger.Errorf("could not parse default maintenance window %q: %v", windowStr, err)
				continue
			}
			maintenanceWindows = append(maintenanceWindows, window)
		}
	}

	for _, window := range maintenanceWindows {
		startTime := window.StartTime.Format("15:04")
		endTime := window.EndTime.Format("15:04")

		if window.Everyday || window.Weekday == currentDay {
			if currentTime >= startTime && currentTime <= endTime {
				return true
			}
		}
	}
	return false
}
