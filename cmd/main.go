package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/zalando/postgres-operator/pkg/controller"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
)

var (
	kubeConfigFile string
	outOfCluster   bool
	version        string
	config         spec.ControllerConfig
)

func mustParseDuration(d string) time.Duration {
	duration, err := time.ParseDuration(d)
	if err != nil {
		panic(err)
	}
	return duration
}

func init() {
	flag.StringVar(&kubeConfigFile, "kubeconfig", "", "Path to kubeconfig file with authorization and master location information.")
	flag.BoolVar(&outOfCluster, "outofcluster", false, "Whether the operator runs in- our outside of the Kubernetes cluster.")
	flag.BoolVar(&config.NoDatabaseAccess, "nodatabaseaccess", false, "Disable all access to the database from the operator side.")
	flag.BoolVar(&config.NoTeamsAPI, "noteamsapi", false, "Disable all access to the teams API")
	flag.Parse()

	config.IgnoredAnnotations = strings.Split(os.Getenv("IGNORED_ANNOTATIONS"), ",")
	config.EnableJsonLogging = os.Getenv("ENABLE_JSON_LOGGING") == "true"

	configMapRawName := os.Getenv("CONFIG_MAP_NAME")
	if configMapRawName != "" {

		err := config.ConfigMapName.Decode(configMapRawName)
		if err != nil {
			log.Fatalf("incorrect config map name: %v", configMapRawName)
		}

		log.Printf("Fully qualified configmap name: %v", config.ConfigMapName)

	}
	if crdInterval := os.Getenv("CRD_READY_WAIT_INTERVAL"); crdInterval != "" {
		config.CRDReadyWaitInterval = mustParseDuration(crdInterval)
	} else {
		config.CRDReadyWaitInterval = 4 * time.Second
	}

	if crdTimeout := os.Getenv("CRD_READY_WAIT_TIMEOUT"); crdTimeout != "" {
		config.CRDReadyWaitTimeout = mustParseDuration(crdTimeout)
	} else {
		config.CRDReadyWaitTimeout = 30 * time.Second
	}
}

func main() {
	var err error

	if config.EnableJsonLogging {
		log.SetFormatter(&log.JSONFormatter{})
	}
	log.SetOutput(os.Stdout)
	log.Printf("Spilo operator %s\n", version)

	sigs := make(chan os.Signal, 1)
	stop := make(chan struct{})
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM) // Push signals into channel

	wg := &sync.WaitGroup{} // Goroutines can add themselves to this to be waited on

	config.RestConfig, err = k8sutil.RestConfig(kubeConfigFile, outOfCluster)
	if err != nil {
		log.Fatalf("couldn't get REST config: %v", err)
	}

	c := controller.NewController(&config, "")

	c.Run(stop, wg)

	sig := <-sigs
	log.Printf("Shutting down... %+v", sig)

	close(stop) // Tell goroutines to stop themselves
	wg.Wait()   // Wait for all to be stopped
}
