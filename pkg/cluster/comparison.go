package cluster

import (
	"reflect"

	u "github.com/zalando-incubator/postgres-operator/pkg/util"
	"k8s.io/api/core/v1"
	"k8s.io/api/apps/v1beta1"
)

type ContainerCondition func(a, b *v1.Container) bool
type StatefulSetCondition func(a, b *v1beta1.StatefulSet) bool
type VolumeClaimCondition func(a, b *v1.PersistentVolumeClaim) bool

type ResourceCheck struct {
	containerCondition   ContainerCondition
	statefulSetCondition StatefulSetCondition
	volumeClaimCondition VolumeClaimCondition
	result               Result
	reason               string
}

type Result struct {
	needUpdate      *bool
	needsRollUpdate *bool
	needsReplace    *bool
}

func (c *Cluster) NewCheck(msg string, cond interface{}, result Result) ResourceCheck {
	switch cond.(type) {
	case ContainerCondition:
		return ResourceCheck{
			reason:             msg,
			containerCondition: cond.(ContainerCondition),
			result:             result,
		}
	case StatefulSetCondition:
		return ResourceCheck{
			reason:               msg,
			statefulSetCondition: cond.(StatefulSetCondition),
			result:               result,
		}
	case VolumeClaimCondition:
		return ResourceCheck{
			reason:               msg,
			volumeClaimCondition: cond.(VolumeClaimCondition),
			result:               result,
		}
	default:
		c.logger.Errorf("Undefined check condition type: %v", cond)
		return ResourceCheck{}
	}
}

func (c *Cluster) getStatefulSetChecks() []ResourceCheck {
	return []ResourceCheck{
		c.NewCheck("new statefulset's number of replicas doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool { return a.Spec.Replicas != b.Spec.Replicas },
			Result{needUpdate: u.True()}),

		c.NewCheck("new statefulset's annotations doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool { return !reflect.DeepEqual(a.Annotations, b.Annotations) },
			Result{needUpdate: u.True()}),

		c.NewCheck("new statefulset's serviceAccountName service asccount name doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return len(a.Spec.Template.Spec.Containers) != len(b.Spec.Template.Spec.Containers)
			}, Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's serviceAccountName service asccount name doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return a.Spec.Template.Spec.ServiceAccountName !=
					b.Spec.Template.Spec.ServiceAccountName
			}, Result{needsRollUpdate: u.True(), needsReplace: u.True()}),

		c.NewCheck("new statefulset's terminationGracePeriodSeconds  doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return a.Spec.Template.Spec.TerminationGracePeriodSeconds !=
					b.Spec.Template.Spec.TerminationGracePeriodSeconds
			}, Result{needsRollUpdate: u.True(), needsReplace: u.True()}),

		c.NewCheck("new statefulset's pod affinity doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return !reflect.DeepEqual(a.Spec.Template.Spec.Affinity,
					b.Spec.Template.Spec.Affinity)
			}, Result{needsRollUpdate: u.True(), needsReplace: u.True()}),

		// Some generated fields like creationTimestamp make it not possible to
		// use DeepCompare on Spec.Template.ObjectMeta
		c.NewCheck("new statefulset's metadata labels doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return !reflect.DeepEqual(a.Spec.Template.Labels, b.Spec.Template.Labels)
			}, Result{needsRollUpdate: u.True(), needsReplace: u.True()}),

		c.NewCheck("new statefulset's pod template metadata annotations doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				return !reflect.DeepEqual(a.Spec.Template.Annotations, b.Spec.Template.Annotations)
			}, Result{needUpdate: u.True(), needsRollUpdate: u.True(), needsReplace: u.True()}),

		c.NewCheck("new statefulset's volumeClaimTemplates contains different number of volumes to the old one",
			func(a, b *v1beta1.StatefulSet) bool {
				return len(a.Spec.VolumeClaimTemplates) != len(b.Spec.VolumeClaimTemplates)
			}, Result{needsReplace: u.True()}),

		c.NewCheck("new statefulset's selector doesn't match the current one",
			func(a, b *v1beta1.StatefulSet) bool {
				if a.Spec.Selector == nil || b.Spec.Selector == nil {
					return false
				}
				return !reflect.DeepEqual(a.Spec.Selector.MatchLabels, b.Spec.Selector.MatchLabels)
			}, Result{needsReplace: u.True()}),
	}
}

func (c *Cluster) getContainerChecks() []ResourceCheck {
	return []ResourceCheck{
		c.NewCheck("new statefulset's container %d name doesn't match the current one",
			func(a, b *v1.Container) bool { return a.Name != b.Name },
			Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's container %d image doesn't match the current one",
			func(a, b *v1.Container) bool { return a.Image != b.Image },
			Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's container %d ports don't match the current one",
			func(a, b *v1.Container) bool { return !reflect.DeepEqual(a.Ports, b.Ports) },
			Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's container %d resources don't match the current ones",
			func(a, b *v1.Container) bool { return !compareResources(&a.Resources, &b.Resources) },
			Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's container %d environment doesn't match the current one",
			func(a, b *v1.Container) bool { return !reflect.DeepEqual(a.Env, b.Env) },
			Result{needsRollUpdate: u.True()}),

		c.NewCheck("new statefulset's container %d environment sources don't match the current one",
			func(a, b *v1.Container) bool { return !reflect.DeepEqual(a.EnvFrom, b.EnvFrom) },
			Result{needsRollUpdate: u.True()}),
	}
}

func (c *Cluster) getVolumeClaimChecks() []ResourceCheck {
	return []ResourceCheck{
		c.NewCheck("new statefulset's name for volume %d doesn't match the current one",
			func(a, b *v1.PersistentVolumeClaim) bool { return a.Name != b.Name },
			Result{needsReplace: u.True()}),

		c.NewCheck("new statefulset's annotations for volume %q doesn't match the current one",
			func(a, b *v1.PersistentVolumeClaim) bool {
				return !reflect.DeepEqual(a.Annotations, b.Annotations)
			},
			Result{needsReplace: u.True()}),

		c.NewCheck("new statefulset's volumeClaimTemplates specification for volume %q doesn't match the current one",
			func(a, b *v1.PersistentVolumeClaim) bool { return !reflect.DeepEqual(a.Spec, b.Spec) },
			Result{needsRollUpdate: u.True()}),
	}
}
