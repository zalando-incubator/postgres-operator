.PHONY: clean local test linux macos docker push scm-source.json e2e

BINARY ?= postgres-operator
BUILD_FLAGS ?= -v
CGO_ENABLED ?= 0
ifeq ($(RACE),1)
	BUILD_FLAGS += -race -a
    CGO_ENABLED=1
endif

LOCAL_BUILD_FLAGS ?= $(BUILD_FLAGS)
LDFLAGS ?= -X=main.version=$(VERSION)
DOCKERDIR = docker

IMAGE ?= registry.opensource.zalan.do/acid/$(BINARY)
TAG ?= $(VERSION)
GITHEAD = $(shell git rev-parse --short HEAD)
GITURL = $(shell git config --get remote.origin.url)
GITSTATUS = $(shell git status --porcelain || echo "no changes")
SOURCES = cmd/main.go
VERSION ?= $(shell git describe --tags --always --dirty)
DIRS := cmd pkg
PKG := `go list ./... | grep -v /vendor/`

ifeq ($(DEBUG),1)
	DOCKERFILE = DebugDockerfile
	#DEBUG_POSTFIX := -debug
	BUILD_FLAGS += -gcflags "-N -l"
else
	DOCKERFILE = Dockerfile
endif

ifdef CDP_PULL_REQUEST_NUMBER
	CDP_TAG := -${CDP_BUILD_VERSION}
endif

ifndef GOPATH
	GOPATH := $(HOME)/go
endif

PATH := $(GOPATH)/bin:$(PATH)
SHELL := env PATH=$(PATH) $(SHELL)

default: local

clean:
	rm -rf build scm-source.json

local: ${SOURCES}
	hack/verify-codegen.sh
	CGO_ENABLED=${CGO_ENABLED} go build -o build/${BINARY} $(LOCAL_BUILD_FLAGS) -ldflags "$(LDFLAGS)" $^

linux: ${SOURCES}
	GOOS=linux GOARCH=amd64 CGO_ENABLED=${CGO_ENABLED} go build -o build/linux/${BINARY} ${BUILD_FLAGS} -ldflags "$(LDFLAGS)" $^

macos: ${SOURCES}
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=${CGO_ENABLED} go build -o build/macos/${BINARY} ${BUILD_FLAGS} -ldflags "$(LDFLAGS)" $^

docker-context: scm-source.json linux
	mkdir -p docker/build/
	cp build/linux/${BINARY} scm-source.json docker/build/

docker: ${DOCKERDIR}/${DOCKERFILE} docker-context
	echo `(env)`
	echo "Tag ${TAG}"
	echo "Version ${VERSION}"
	echo "CDP tag ${CDP_TAG}"
	echo "git describe $(shell git describe --tags --always --dirty)"
	cd "${DOCKERDIR}" && docker build --rm -t "$(IMAGE):$(TAG)$(CDP_TAG)$(DEBUG_POSTFIX)" -f "${DOCKERFILE}" .

indocker-race:
	docker run --rm -v "${GOPATH}":"${GOPATH}" -e GOPATH="${GOPATH}" -e RACE=1 -w ${PWD} golang:1.8.1 bash -c "make linux"

push:
	docker push "$(IMAGE):$(TAG)$(CDP_TAG)"

scm-source.json: .git
	echo '{\n "url": "git:$(GITURL)",\n "revision": "$(GITHEAD)",\n "author": "$(USER)",\n "status": "$(GITSTATUS)"\n}' > scm-source.json

tools:
	GO111MODULE=on go get -u honnef.co/go/tools/cmd/staticcheck
	GO111MODULE=on go get k8s.io/client-go@kubernetes-1.16.3
	GO111MODULE=on go mod tidy

fmt:
	@gofmt -l -w -s $(DIRS)

vet:
	@go vet $(PKG)
	@staticcheck $(PKG)

deps: tools
	GO111MODULE=on go mod vendor

test:
	hack/verify-codegen.sh
	GO111MODULE=on go test ./...

e2e: docker # build operator image to be tested
	cd e2e; make tools test clean
