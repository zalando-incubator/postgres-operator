module github.com/zalando/postgres-operator

go 1.15

require (
	github.com/aws/aws-sdk-go v1.36.29
	github.com/golang/mock v1.4.4
	github.com/lib/pq v1.9.0
	github.com/motomux/pretty v0.0.0-20161209205251-b2aad2c9a95d
	github.com/prometheus/client_golang v1.11.0
	github.com/r3labs/diff v1.1.0
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.6.1
	golang.org/x/crypto v0.0.0-20201203163018-be400aefbc4c
	golang.org/x/mod v0.4.0 // indirect
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.20.6
	k8s.io/apiextensions-apiserver v0.20.6
	k8s.io/apimachinery v0.20.6
	k8s.io/client-go v0.20.6
	k8s.io/code-generator v0.20.6
)
