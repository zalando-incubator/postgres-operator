package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	majorVersionUpgradeSuccessAnnotation     = "last-major-upgrade-success"
	majorVersionUpgradeFailureAnnotation     = "last-major-upgrade-failure"
	logicalReplicationSlotLagThreshold       = 2 * 16 * 1024 * 1024
	logicalReplicationSlotLagCheckMinVersion = 17 * 10000
)

func parseLaggingLogicalReplicationSlots(output string) ([]string, error) {
	laggingSlots := []string{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 2 {
			return nil, fmt.Errorf("unexpected logical replication slot lag output: %q", line)
		}
		lagBytes, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid logical replication slot lag for slot %q: %w", fields[0], err)
		}
		if lagBytes > logicalReplicationSlotLagThreshold {
			laggingSlots = append(laggingSlots, line)
		}
	}
	return laggingSlots, nil
}

// IsBiggerPostgresVersion Compare two Postgres version numbers
func IsBiggerPostgresVersion(old string, new string) bool {
	oldN, _ := strconv.Atoi(old)
	newN, _ := strconv.Atoi(new)
	return newN > oldN
}

// GetDesiredMajorVersionAsInt Convert string to comparable integer of PG version
func (c *Cluster) GetDesiredMajorVersionAsInt() int {
	version, _ := strconv.Atoi(c.GetDesiredMajorVersion())
	return version * 10000
}

// GetDesiredMajorVersion returns major version to use, incl. potential auto upgrade
func (c *Cluster) GetDesiredMajorVersion() string {

	if c.Config.OpConfig.MajorVersionUpgradeMode == "full" {
		// e.g. current is 14, minimal is 14 allowing 14 to 18 clusters, everything below is upgraded
		if IsBiggerPostgresVersion(c.Spec.PgVersion, c.Config.OpConfig.MinimalMajorVersion) {
			c.logger.Infof("overwriting configured major version %s to %s", c.Spec.PgVersion, c.Config.OpConfig.TargetMajorVersion)
			return c.Config.OpConfig.TargetMajorVersion
		}
	}

	return c.Spec.PgVersion
}

func (c *Cluster) isUpgradeAllowedForTeam(owningTeam string) bool {
	allowedTeams := c.OpConfig.MajorVersionUpgradeTeamAllowList

	if len(allowedTeams) == 0 {
		return false
	}

	return util.SliceContains(allowedTeams, owningTeam)
}

func (c *Cluster) annotatePostgresResource(isSuccess bool) error {
	annotations := make(map[string]string)
	currentTime := metav1.Now().Format("2006-01-02T15:04:05Z")
	if isSuccess {
		annotations[majorVersionUpgradeSuccessAnnotation] = currentTime
	} else {
		annotations[majorVersionUpgradeFailureAnnotation] = currentTime
	}
	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		c.logger.Errorf("could not form patch for %s postgresql resource: %v", c.Name, err)
		return err
	}
	_, err = c.KubeClient.Postgresqls(c.Namespace).Patch(context.Background(), c.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		c.logger.Errorf("failed to patch annotations to postgresql resource: %v", err)
		return err
	}
	return nil
}

func (c *Cluster) removeFailuresAnnotation() error {
	annotationToRemove := []map[string]string{
		{
			"op":   "remove",
			"path": fmt.Sprintf("/metadata/annotations/%s", majorVersionUpgradeFailureAnnotation),
		},
	}
	removePatch, err := json.Marshal(annotationToRemove)
	if err != nil {
		c.logger.Errorf("could not form removal patch for %s postgresql resource: %v", c.Name, err)
		return err
	}
	_, err = c.KubeClient.Postgresqls(c.Namespace).Patch(context.Background(), c.Name, types.JSONPatchType, removePatch, metav1.PatchOptions{})
	if err != nil {
		c.logger.Errorf("failed to remove annotations from postgresql resource: %v", err)
		return err
	}
	return nil
}

func (c *Cluster) criticalOperationLabel(pods []v1.Pod, value *string) error {
	metadataReq := map[string]map[string]map[string]*string{"metadata": {"labels": {"critical-operation": value}}}

	patchReq, err := json.Marshal(metadataReq)
	if err != nil {
		return fmt.Errorf("could not marshal ObjectMeta: %v", err)
	}
	for _, pod := range pods {
		_, err = c.KubeClient.Pods(c.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patchReq, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

/*
Execute upgrade when mode is set to manual or full or when the owning team is allowed for upgrade (and mode is "off").

Manual upgrade means, it is triggered by the user via manifest version change
Full upgrade means, operator also determines the minimal version used accross all clusters and upgrades violators.
*/
func (c *Cluster) majorVersionUpgrade() error {

	if c.OpConfig.MajorVersionUpgradeMode == "off" && !c.isUpgradeAllowedForTeam(c.Spec.TeamID) {
		return nil
	}

	desiredVersion := c.GetDesiredMajorVersionAsInt()

	if c.currentMajorVersion >= desiredVersion {
		if _, exists := c.ObjectMeta.Annotations[majorVersionUpgradeFailureAnnotation]; exists { // if failure annotation exists, remove it
			c.removeFailuresAnnotation()
			c.logger.Infof("removing failure annotation as the cluster is already up to date")
		}
		c.logger.Infof("cluster version up to date. current: %d, min desired: %d", c.currentMajorVersion, desiredVersion)
		return nil
	}

	pods, err := c.listPods()
	if err != nil {
		return err
	}

	allRunning := true
	isStandbyCluster := false

	var masterPod *v1.Pod

	for i, pod := range pods {
		ps, _ := c.patroni.GetMemberData(&pod)

		if ps.Role == "standby_leader" {
			isStandbyCluster = true
			c.currentMajorVersion = ps.ServerVersion
			break
		}

		if ps.State != "running" {
			allRunning = false
			c.logger.Infof("identified non running pod, potentially skipping major version upgrade")
		}

		if ps.Role == "master" || ps.Role == "primary" {
			masterPod = &pods[i]
			c.currentMajorVersion = ps.ServerVersion
		}
	}

	if masterPod == nil {
		c.logger.Infof("no master in the cluster, skipping major version upgrade")
		return nil
	}

	// Recheck version with newest data from Patroni
	if c.currentMajorVersion >= desiredVersion {
		if _, exists := c.ObjectMeta.Annotations[majorVersionUpgradeFailureAnnotation]; exists { // if failure annotation exists, remove it
			c.removeFailuresAnnotation()
			c.logger.Infof("removing failure annotation as the cluster is already up to date")
		}
		c.logger.Infof("recheck cluster version is already up to date. current: %d, min desired: %d", c.currentMajorVersion, desiredVersion)
		return nil
	} else if isStandbyCluster {
		c.logger.Warnf("skipping major version upgrade for %s/%s standby cluster. Re-deploy standby cluster with the required Postgres version specified", c.Namespace, c.Name)
		return nil
	}

	if _, exists := c.ObjectMeta.Annotations[majorVersionUpgradeFailureAnnotation]; exists {
		c.logger.Infof("last major upgrade failed, skipping upgrade")
		return nil
	}

	if !c.isInMaintenanceWindow(c.Spec.MaintenanceWindows) {
		c.logger.Infof("skipping major version upgrade, not in maintenance window")
		return nil
	}

	members, err := c.patroni.GetClusterMembers(masterPod)
	if err != nil {
		c.logger.Error("could not get cluster members data from Patroni API, skipping major version upgrade")
		return err
	}
	patroniData, err := c.patroni.GetMemberData(masterPod)
	if err != nil {
		c.logger.Error("could not get members data from Patroni API, skipping major version upgrade")
		return err
	}
	patroniVer, err := semver.NewVersion(patroniData.Patroni.Version)
	if err != nil {
		c.logger.Error("error parsing Patroni version")
		patroniVer, _ = semver.NewVersion("3.0.4")
	}
	verConstraint, _ := semver.NewConstraint(">= 3.0.4")
	checkStreaming, _ := verConstraint.Validate(patroniVer)

	for _, member := range members {
		if PostgresRole(member.Role) == Leader {
			continue
		}
		if checkStreaming && member.State != "streaming" {
			c.logger.Infof("skipping major version upgrade, replica %s is not streaming from primary", member.Name)
			return nil
		}
		if member.Lag > 16*1024*1024 {
			c.logger.Infof("skipping major version upgrade, replication lag on member %s is too high", member.Name)
			return nil
		}
	}

	isUpgradeSuccess := true
	numberOfPods := len(pods)
	if allRunning {
		podName := &spec.NamespacedName{Namespace: masterPod.Namespace, Name: masterPod.Name}
		if c.currentMajorVersion >= logicalReplicationSlotLagCheckMinVersion {
			const logicalSlotLagQuery = `SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
			FROM pg_replication_slots WHERE slot_type = 'logical'`
			slotLagOutput, err := c.ExecCommand(podName, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", c.OpConfig.SuperUsername, "-d", "postgres", "-c", logicalSlotLagQuery)
			if err != nil {
				c.logger.Errorf("skipping major version upgrade, could not check logical replication slot lag: %v", err)
				return fmt.Errorf("could not check logical replication slot lag: %w", err)
			}
			laggingSlots, err := parseLaggingLogicalReplicationSlots(slotLagOutput)
			if err != nil {
				c.logger.Errorf("skipping major version upgrade, could not parse logical replication slot lag: %v", err)
				return fmt.Errorf("could not parse logical replication slot lag: %w", err)
			}
			if len(laggingSlots) > 0 {
				c.logger.Errorf("skipping major version upgrade, logical replication slot lag exceeds %d bytes (slot_name|lag_bytes): %s", logicalReplicationSlotLagThreshold, strings.Join(laggingSlots, ", "))
				return nil
			}
		}

		c.logger.Infof("healthy cluster ready to upgrade, current: %d desired: %d", c.currentMajorVersion, desiredVersion)
		if c.currentMajorVersion < desiredVersion {
			defer func() error {
				if err = c.criticalOperationLabel(pods, nil); err != nil {
					return fmt.Errorf("failed to remove critical-operation label: %s", err)
				}
				return nil
			}()
			val := "true"
			if err = c.criticalOperationLabel(pods, &val); err != nil {
				return fmt.Errorf("failed to assign critical-operation label: %s", err)
			}

			c.logger.Infof("triggering major version upgrade on pod %s of %d pods", masterPod.Name, numberOfPods)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Major Version Upgrade", "starting major version upgrade on pod %s of %d pods", masterPod.Name, numberOfPods)
			upgradeCommand := fmt.Sprintf("set -o pipefail && /usr/bin/python3 /scripts/inplace_upgrade.py %d 2>&1 | tee last_upgrade.log", numberOfPods)

			c.logger.Debug("checking if the spilo image runs with root or non-root (check for user id=0)")
			resultIdCheck, errIdCheck := c.ExecCommand(podName, "/bin/bash", "-c", "/usr/bin/id -u")
			if errIdCheck != nil {
				c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Major Version Upgrade", "checking user id to run upgrade from %d to %d FAILED: %v", c.currentMajorVersion, desiredVersion, errIdCheck)
			}

			resultIdCheck = strings.TrimSuffix(resultIdCheck, "\n")
			var result, scriptErrMsg string
			if resultIdCheck != "0" {
				c.logger.Infof("user id was identified as: %s, hence default user is non-root already", resultIdCheck)
				result, err = c.ExecCommand(podName, "/bin/bash", "-c", upgradeCommand)
				scriptErrMsg, _ = c.ExecCommand(podName, "/bin/bash", "-c", "tail -n 1 last_upgrade.log")
			} else {
				c.logger.Infof("user id was identified as: %s, using su to reach the postgres user", resultIdCheck)
				result, err = c.ExecCommand(podName, "/bin/su", "postgres", "-c", upgradeCommand)
				scriptErrMsg, _ = c.ExecCommand(podName, "/bin/bash", "-c", "tail -n 1 last_upgrade.log")
			}
			if err != nil {
				isUpgradeSuccess = false
				c.annotatePostgresResource(isUpgradeSuccess)
				c.logger.Errorf("upgrade action triggered but command failed: %v", err)
				if strings.TrimSpace(scriptErrMsg) == "" {
					scriptErrMsg = err.Error()
				}
				c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Major Version Upgrade", "upgrade from %d to %d FAILED: %v", c.currentMajorVersion, desiredVersion, scriptErrMsg)
				return fmt.Errorf("%s", scriptErrMsg)
			}

			c.annotatePostgresResource(isUpgradeSuccess)
			c.logger.Infof("upgrade action triggered and command completed: %s", result[:100])
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Major Version Upgrade", "upgrade from %d to %d finished", c.currentMajorVersion, desiredVersion)
		}
	}

	return nil
}
