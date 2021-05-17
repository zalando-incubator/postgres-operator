package cluster

import (
	"strings"
	"testing"

	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"

	v1 "k8s.io/api/core/v1"
)

func newsService(ann map[string]string, svcT v1.ServiceType, lbSr []string) *v1.Service {
	svc := &v1.Service{
		Spec: v1.ServiceSpec{
			Type:                     svcT,
			LoadBalancerSourceRanges: lbSr,
		},
	}
	svc.Annotations = ann
	return svc
}

func TestSameService(t *testing.T) {
	cluster := Cluster{
		Config: Config{
			OpConfig: config.Config{
				IgnoredAnnotations: []string{
					"k8s.v1.cni.cncf.io/network-status",
				},
			},
		},
	}
	tests := []struct {
		about   string
		current *v1.Service
		new     *v1.Service
		reason  string
		match   bool
	}{
		{
			about: "two equal services",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeClusterIP,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeClusterIP,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match: true,
		},
		{
			about: "services differ on service type",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeClusterIP,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's type "LoadBalancer" does not match the current one "ClusterIP"`,
		},
		{
			about: "services differ on lb source ranges",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"185.249.56.0/22"}),
			match:  false,
			reason: `new service's LoadBalancerSourceRange does not match the current one`,
		},
		{
			about: "new service doesn't have lb source ranges",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{}),
			match:  false,
			reason: `new service's LoadBalancerSourceRange does not match the current one`,
		},
		{
			about: "services differ on DNS annotation",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "new_clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: 'external-dns.alpha.kubernetes.io/hostname' changed from 'clstr.acid.zalan.do' to 'new_clstr.acid.zalan.do'.`,
		},
		{
			about: "services differ on AWS ELB annotation",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: "1800",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: 'service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout' changed from '3600' to '1800'.`,
		},
		{
			about: "service changes existing annotation",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "baz",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: 'foo' changed from 'bar' to 'baz'.`,
		},
		{
			about: "service changes multiple existing annotations",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
					"bar":                              "foo",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "baz",
					"bar":                              "fooz",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match: false,
			// Test just the prefix to avoid flakiness and map sorting
			reason: `new service's annotations does not match the current one:`,
		},
		{
			about: "service adds a new custom annotation",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: Added 'foo' with value 'bar'.`,
		},
		{
			about: "service removes a custom annotation",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: Removed 'foo'.`,
		},
		{
			about: "service removes a custom annotation and adds a new one",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"bar":                              "foo",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match:  false,
			reason: `new service's annotations does not match the current one: Removed 'foo'. Added 'bar' with value 'foo'.`,
		},
		{
			about: "service removes a custom annotation, adds a new one and change another",
			current: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"foo":                              "bar",
					"zalan":                            "do",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
					"bar":                              "foo",
					"zalan":                            "do.com",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match: false,
			// Test just the prefix to avoid flakiness and map sorting
			reason: `new service's annotations does not match the current one: Removed 'foo'.`,
		},
		{
			about: "service add annotations",
			current: newsService(
				map[string]string{},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					constants.ZalandoDNSNameAnnotation: "clstr.acid.zalan.do",
					constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match: false,
			// Test just the prefix to avoid flakiness and map sorting
			reason: `new service's annotations does not match the current one: Added `,
		},
		{
			about: "ignored annotations",
			current: newsService(
				map[string]string{},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			new: newsService(
				map[string]string{
					"k8s.v1.cni.cncf.io/network-status": "up",
				},
				v1.ServiceTypeLoadBalancer,
				[]string{"128.141.0.0/16", "137.138.0.0/16"}),
			match: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.about, func(t *testing.T) {
			match, reason := cluster.SameService(tt.current, tt.new)
			if match && !tt.match {
				t.Logf("match=%v current=%v, old=%v reason=%s", match, tt.current.Annotations, tt.new.Annotations, reason)
				t.Errorf("expected services to do not match: '%q' and '%q'", tt.current, tt.new)
				return
			}
			if !match && tt.match {
				t.Errorf("expected services to be the same: '%q' and '%q'", tt.current, tt.new)
				return
			}
			if !match && !tt.match {
				if !strings.HasPrefix(reason, tt.reason) {
					t.Errorf("expected reason prefix '%s', found '%s'", tt.reason, reason)
					return
				}
			}
		})
	}
}
