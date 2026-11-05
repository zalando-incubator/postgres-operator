package cluster

import (
	"fmt"
	"testing"

	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/zalando/postgres-operator/v2/mocks"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/volumes"
	"k8s.io/client-go/kubernetes/fake"
)

type testVolume struct {
	size        int32
	iops        int32
	throughtput int32
	volType     string
}

var testVol = testVolume{
	size:        100,
	iops:        300,
	throughtput: 125,
	volType:     "gp2",
}

func newFakeK8sPVCclient() (k8sutil.KubernetesClient, *fake.Clientset) {
	clientSet := fake.NewSimpleClientset()

	return k8sutil.KubernetesClient{
		PersistentVolumeClaimsGetter: clientSet.CoreV1(),
		PersistentVolumesGetter:      clientSet.CoreV1(),
		PodsGetter:                   clientSet.CoreV1(),
	}, clientSet
}

func TestResizeVolumeClaim(t *testing.T) {
	testName := "test resizing of persistent volume claims"
	client, _ := newFakeK8sPVCclient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	newVolumeSize := "2Gi"

	storage1Gi, err := resource.ParseQuantity("1Gi")
	assert.NoError(t, err)

	// new cluster with pvc storage resize mode and configured labels
	var cluster = New(
		Config{
			OpConfig: config.Config{
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
				},
				StorageResizeMode: "pvc",
			},
		}, client, acidv1.Postgresql{}, logger, eventRecorder)

	// set metadata, so that labels will get correct values
	cluster.Name = clusterName
	cluster.Namespace = namespace
	filterLabels := cluster.labelsSet(false)
	cluster.Spec.Volume.Size = newVolumeSize

	// define and create PVCs for 1Gi volumes
	pvcList := CreatePVCs(namespace, clusterName, filterLabels, 2, "1Gi")
	// add another PVC with different cluster name
	pvcList.Items = append(pvcList.Items, CreatePVCs(namespace, clusterName+"-2", labels.Set{}, 1, "1Gi").Items[0])

	for _, pvc := range pvcList.Items {
		cluster.KubeClient.PersistentVolumeClaims(namespace).Create(context.TODO(), &pvc, metav1.CreateOptions{})
	}

	// test resizing
	cluster.syncVolumes()

	pvcs, err := cluster.listPersistentVolumeClaims()
	assert.NoError(t, err)

	// check if listPersistentVolumeClaims returns only the PVCs matching the filter
	if len(pvcs) != len(pvcList.Items)-1 {
		t.Errorf("%s: could not find all persistent volume claims, got %v, expected %v", testName, len(pvcs), len(pvcList.Items)-1)
	}

	// check if PVCs were correctly resized
	for _, pvc := range pvcs {
		newStorageSize := quantityToGigabyte(pvc.Spec.Resources.Requests[v1.ResourceStorage])
		expectedQuantity, err := resource.ParseQuantity(newVolumeSize)
		assert.NoError(t, err)
		expectedSize := quantityToGigabyte(expectedQuantity)
		if newStorageSize != expectedSize {
			t.Errorf("%s: resizing failed, got %v, expected %v", testName, newStorageSize, expectedSize)
		}
	}

	// check if other PVC was not resized
	pvc2, err := cluster.KubeClient.PersistentVolumeClaims(namespace).Get(context.TODO(), constants.DataVolumeName+"-"+clusterName+"-2-0", metav1.GetOptions{})
	assert.NoError(t, err)
	unchangedSize := quantityToGigabyte(pvc2.Spec.Resources.Requests[v1.ResourceStorage])
	expectedSize := quantityToGigabyte(storage1Gi)
	if unchangedSize != expectedSize {
		t.Errorf("%s: volume size changed, got %v, expected %v", testName, unchangedSize, expectedSize)
	}
}

func TestSyncVolumeAttributesClass(t *testing.T) {
	clusterName := "acid-test-cluster"
	namespace := "default"

	tests := []struct {
		subTest       string
		resizeMode    string
		manifestClass string
		pvcClass      *string
		wantClass     *string
	}{
		{
			subTest:       "class from manifest is set on existing volume claims",
			resizeMode:    "pvc",
			manifestClass: "gp3-fast",
			pvcClass:      nil,
			wantClass:     k8sutil.StringToPointer("gp3-fast"),
		},
		{
			subTest:       "changed class in manifest replaces the class of existing volume claims",
			resizeMode:    "pvc",
			manifestClass: "gp3-fast",
			pvcClass:      k8sutil.StringToPointer("gp3-slow"),
			wantClass:     k8sutil.StringToPointer("gp3-fast"),
		},
		{
			subTest:       "class is synced when storage resize mode is off",
			resizeMode:    "off",
			manifestClass: "gp3-fast",
			pvcClass:      nil,
			wantClass:     k8sutil.StringToPointer("gp3-fast"),
		},
		{
			subTest:       "class removed from manifest is kept on existing volume claims",
			resizeMode:    "pvc",
			manifestClass: "",
			pvcClass:      k8sutil.StringToPointer("gp3-slow"),
			wantClass:     k8sutil.StringToPointer("gp3-slow"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.subTest, func(t *testing.T) {
			client, _ := newFakeK8sPVCclient()
			cluster := New(
				Config{
					OpConfig: config.Config{
						Resources: config.Resources{
							ClusterLabels:    map[string]string{"application": "spilo"},
							ClusterNameLabel: "cluster-name",
						},
						StorageResizeMode: tt.resizeMode,
					},
				}, client, acidv1.Postgresql{}, logger, eventRecorder)
			cluster.Name = clusterName
			cluster.Namespace = namespace
			cluster.Spec.Volume.Size = "1Gi"
			cluster.Spec.Volume.VolumeAttributesClass = tt.manifestClass

			pvcList := CreatePVCs(namespace, clusterName, cluster.labelsSet(false), 2, "1Gi")
			otherClusterLabels := labels.Set{"application": "spilo", "cluster-name": clusterName + "-2"}
			otherCluster := CreatePVCs(namespace, clusterName+"-2", otherClusterLabels, 1, "1Gi").Items[0]
			for _, pvc := range append(pvcList.Items, otherCluster) {
				pvc.Spec.VolumeAttributesClassName = tt.pvcClass
				_, err := client.PersistentVolumeClaims(namespace).Create(context.TODO(), &pvc, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			assert.NoError(t, cluster.syncVolumes())

			for _, pvc := range pvcList.Items {
				got, err := client.PersistentVolumeClaims(namespace).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tt.wantClass, got.Spec.VolumeAttributesClassName, "volume claim %q", pvc.Name)
			}

			got, err := client.PersistentVolumeClaims(namespace).Get(context.TODO(), otherCluster.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.pvcClass, got.Spec.VolumeAttributesClassName, "volume claim of other cluster must not change")
		})
	}
}

func TestQuantityToGigabyte(t *testing.T) {
	tests := []struct {
		name        string
		quantityStr string
		expected    int32
	}{
		{
			"test with 1Gi",
			"1Gi",
			1,
		},
		{
			"test with float",
			"1.5Gi",
			int32(1),
		},
		{
			"test with 1000Mi",
			"1000Mi",
			int32(0),
		},
	}

	for _, tt := range tests {
		quantity, err := resource.ParseQuantity(tt.quantityStr)
		assert.NoError(t, err)
		gigabyte := quantityToGigabyte(quantity)
		if gigabyte != tt.expected {
			t.Errorf("%s: got %v, expected %v", tt.name, gigabyte, tt.expected)
		}
	}
}

func CreatePVCs(namespace string, clusterName string, labels labels.Set, n int, size string) v1.PersistentVolumeClaimList {
	// define and create PVCs for 1Gi volumes
	storage1Gi, _ := resource.ParseQuantity(size)
	pvcList := v1.PersistentVolumeClaimList{
		Items: []v1.PersistentVolumeClaim{},
	}

	for i := 0; i < n; i++ {
		pvc := v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%s-%d", constants.DataVolumeName, clusterName, i),
				Namespace: namespace,
				Labels:    labels,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: storage1Gi,
					},
				},
				VolumeName: fmt.Sprintf("persistent-volume-%d", i),
			},
		}
		pvcList.Items = append(pvcList.Items, pvc)
	}

	return pvcList
}

func initTestVolumesAndPods(client k8sutil.KubernetesClient, namespace, clustername string, labels labels.Set, volumes []testVolume) {
	i := 0
	for _, v := range volumes {
		storage1Gi, _ := resource.ParseQuantity(fmt.Sprintf("%d", v.size))

		ps := v1.PersistentVolumeSpec{}
		ps.AWSElasticBlockStore = &v1.AWSElasticBlockStoreVolumeSource{}
		ps.AWSElasticBlockStore.VolumeID = fmt.Sprintf("aws://eu-central-1b/ebs-volume-%d", i+1)

		pv := v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("persistent-volume-%d", i),
			},
			Spec: ps,
		}

		client.PersistentVolumes().Create(context.TODO(), &pv, metav1.CreateOptions{})

		pvc := v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%s-%d", constants.DataVolumeName, clustername, i),
				Namespace: namespace,
				Labels:    labels,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: storage1Gi,
					},
				},
				VolumeName: fmt.Sprintf("persistent-volume-%d", i),
			},
		}

		client.PersistentVolumeClaims(namespace).Create(context.TODO(), &pvc, metav1.CreateOptions{})

		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("%s-%d", clustername, i),
				Labels: labels,
			},
			Spec: v1.PodSpec{},
		}

		client.Pods(namespace).Create(context.TODO(), &pod, metav1.CreateOptions{})

		i = i + 1
	}
}

func TestGp2Gp3Migration(t *testing.T) {
	client, _ := newFakeK8sPVCclient()
	clusterName := "acid-test-cluster"
	namespace := "default"

	// new cluster with pvc storage resize mode and configured labels
	var cluster = New(
		Config{
			OpConfig: config.Config{
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
				},
				StorageResizeMode: "mixed",
			},
		}, client, acidv1.Postgresql{Spec: acidv1.PostgresSpec{Volume: acidv1.Volume{VolumeType: "gp3"}}}, logger, eventRecorder)

	cluster.Spec.Volume.Size = "150Gi"
	cluster.Spec.Volume.Iops = aws.Int32(6000)
	cluster.Spec.Volume.Throughput = aws.Int32(275)

	// set metadata, so that labels will get correct values
	cluster.Name = clusterName
	cluster.Namespace = namespace
	filterLabels := cluster.labelsSet(false)

	testVolumes := []testVolume{testVol, testVol}

	initTestVolumesAndPods(cluster.KubeClient, namespace, clusterName, filterLabels, testVolumes)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resizer := mocks.NewMockVolumeResizer(ctrl)

	resizer.EXPECT().ExtractVolumeID(gomock.Eq("aws://eu-central-1b/ebs-volume-1")).Return("ebs-volume-1", nil)
	resizer.EXPECT().ExtractVolumeID(gomock.Eq("aws://eu-central-1b/ebs-volume-2")).Return("ebs-volume-2", nil)

	resizer.EXPECT().GetProviderVolumeID(gomock.Any()).
		DoAndReturn(func(pv *v1.PersistentVolume) (string, error) {
			return resizer.ExtractVolumeID(pv.Spec.AWSElasticBlockStore.VolumeID)
		}).
		Times(2)

	resizer.EXPECT().DescribeVolumes(gomock.Eq([]string{"ebs-volume-1", "ebs-volume-2"})).Return(
		[]volumes.VolumeProperties{
			{VolumeID: "ebs-volume-1", VolumeType: "gp2", Size: 150, Iops: 3000},
			{VolumeID: "ebs-volume-2", VolumeType: "gp2", Size: 150, Iops: 4000},
		}, nil)

	// expect only gp2 volume to be modified
	resizer.EXPECT().ModifyVolume(gomock.Eq("ebs-volume-1"), gomock.Eq(aws.String("gp3")), gomock.Nil(), gomock.Eq(aws.Int32(6000)), gomock.Eq(aws.Int32(275))).Return(nil)
	resizer.EXPECT().ModifyVolume(gomock.Eq("ebs-volume-2"), gomock.Eq(aws.String("gp3")), gomock.Nil(), gomock.Eq(aws.Int32(6000)), gomock.Eq(aws.Int32(275))).Return(nil)

	cluster.VolumeResizer = resizer
	cluster.syncVolumes()
}

func TestNoVolumeTypeChange(t *testing.T) {
	client, _ := newFakeK8sPVCclient()
	clusterName := "acid-test-cluster"
	namespace := "default"

	// new cluster with pvc storage resize mode and configured labels
	var cluster = New(
		Config{
			OpConfig: config.Config{
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
				},
				StorageResizeMode: "mixed",
			},
		}, client, acidv1.Postgresql{}, logger, eventRecorder)

	cluster.Spec.Volume.Size = "177Gi"

	// set metadata, so that labels will get correct values
	cluster.Name = clusterName
	cluster.Namespace = namespace
	filterLabels := cluster.labelsSet(false)

	testVolumes := []testVolume{
		{
			size: 150,
		},
		{
			size: 150,
		},
	}

	initTestVolumesAndPods(cluster.KubeClient, namespace, clusterName, filterLabels, testVolumes)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resizer := mocks.NewMockVolumeResizer(ctrl)

	resizer.EXPECT().ExtractVolumeID(gomock.Eq("aws://eu-central-1b/ebs-volume-1")).Return("ebs-volume-1", nil)
	resizer.EXPECT().ExtractVolumeID(gomock.Eq("aws://eu-central-1b/ebs-volume-2")).Return("ebs-volume-2", nil)

	resizer.EXPECT().GetProviderVolumeID(gomock.Any()).
		DoAndReturn(func(pv *v1.PersistentVolume) (string, error) {
			return resizer.ExtractVolumeID(pv.Spec.AWSElasticBlockStore.VolumeID)
		}).
		Times(2)

	resizer.EXPECT().DescribeVolumes(gomock.Eq([]string{"ebs-volume-1", "ebs-volume-2"})).Return(
		[]volumes.VolumeProperties{
			{VolumeID: "ebs-volume-1", VolumeType: "gp2", Size: 150, Iops: 3000},
			{VolumeID: "ebs-volume-2", VolumeType: "gp2", Size: 150, Iops: 4000},
		}, nil)

	// expect only gp2 volume to be modified
	resizer.EXPECT().ModifyVolume(gomock.Eq("ebs-volume-1"), gomock.Nil(), gomock.Eq(aws.Int32(177)), gomock.Nil(), gomock.Nil()).Return(nil)
	resizer.EXPECT().ModifyVolume(gomock.Eq("ebs-volume-2"), gomock.Nil(), gomock.Eq(aws.Int32(177)), gomock.Nil(), gomock.Nil()).Return(nil)

	cluster.VolumeResizer = resizer
	cluster.syncVolumes()
}
