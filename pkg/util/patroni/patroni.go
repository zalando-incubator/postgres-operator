package patroni

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	httpclient "github.com/zalando/postgres-operator/pkg/util/httpclient"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

const (
	failoverPath = "/failover"
	configPath   = "/config"
	statusPath   = "/patroni"
	restartPath  = "/restart"
	apiPort      = 8008
	timeout      = 30 * time.Second
)

// Interface describe patroni methods
type Interface interface {
	Switchover(master *v1.Pod, candidate string) error
	SetPostgresParameters(server *v1.Pod, options map[string]string) error
	GetMemberData(server *v1.Pod) (MemberData, error)
	Restart(server *v1.Pod) error
	GetConfig(server *v1.Pod) (map[string]interface{}, error)
	SetConfig(server *v1.Pod, config map[string]interface{}) error
}

// Patroni API client
type Patroni struct {
	httpClient httpclient.HTTPClient
	logger     *logrus.Entry
}

// New create patroni
func New(logger *logrus.Entry, client httpclient.HTTPClient) *Patroni {
	if client == nil {

		client = &http.Client{
			Timeout: timeout,
		}

	}

	return &Patroni{
		logger:     logger,
		httpClient: client,
	}
}

func apiURL(masterPod *v1.Pod) (string, error) {
	ip := net.ParseIP(masterPod.Status.PodIP)
	if ip == nil {
		return "", fmt.Errorf("%s is not a valid IP", masterPod.Status.PodIP)
	}
	// Sanity check PodIP
	if ip.To4() == nil {
		if ip.To16() == nil {
			// Shouldn't ever get here, but library states it's possible.
			return "", fmt.Errorf("%s is not a valid IPv4/IPv6 address", masterPod.Status.PodIP)
		}
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(ip.String(), strconv.Itoa(apiPort))), nil
}

func (p *Patroni) httpPostOrPatch(method string, url string, body *bytes.Buffer) (err error) {
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return fmt.Errorf("could not create request: %v", err)
	}

	if p.logger != nil {
		p.logger.Debugf("making %s http request: %s", method, request.URL.String())
	}

	resp, err := p.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("could not make request: %v", err)
	}
	defer func() {
		if err2 := resp.Body.Close(); err2 != nil {
			if err != nil {
				err = fmt.Errorf("could not close request: %v, prior error: %v", err2, err)
			} else {
				err = fmt.Errorf("could not close request: %v", err2)
			}
			return
		}
	}()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read response: %v", err)
		}

		return fmt.Errorf("patroni returned '%s'", string(bodyBytes))
	}
	return nil
}

func (p *Patroni) httpGet(url string) (string, error) {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("could not create request: %v", err)
	}

	p.logger.Debugf("making GET http request: %s", request.URL.String())

	resp, err := p.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("could not make request: %v", err)
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("could not read response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		return "", fmt.Errorf("could not close request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return string(bodyBytes), fmt.Errorf("patroni returned '%d'", resp.StatusCode)
	}
	return string(bodyBytes), nil
}

// Switchover by calling Patroni REST API
func (p *Patroni) Switchover(master *v1.Pod, candidate string) error {
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(map[string]string{"leader": master.Name, "member": candidate})
	if err != nil {
		return fmt.Errorf("could not encode json: %v", err)
	}
	apiURLString, err := apiURL(master)
	if err != nil {
		return err
	}
	return p.httpPostOrPatch(http.MethodPost, apiURLString+failoverPath, buf)
}

//TODO: add an option call /patroni to check if it is necessary to restart the server

//SetPostgresParameters sets Postgres options via Patroni patch API call.
func (p *Patroni) SetPostgresParameters(server *v1.Pod, parameters map[string]string) error {
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(map[string]map[string]interface{}{"postgresql": {"parameters": parameters}})
	if err != nil {
		return fmt.Errorf("could not encode json: %v", err)
	}
	apiURLString, err := apiURL(server)
	if err != nil {
		return err
	}
	return p.httpPostOrPatch(http.MethodPatch, apiURLString+configPath, buf)
}

//SetConfig sets Patroni options via Patroni patch API call.
func (p *Patroni) SetConfig(server *v1.Pod, config map[string]interface{}) error {
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(config)
	if err != nil {
		return fmt.Errorf("could not encode json: %v", err)
	}
	apiURLString, err := apiURL(server)
	if err != nil {
		return err
	}
	return p.httpPostOrPatch(http.MethodPatch, apiURLString+configPath, buf)
}

// MemberDataPatroni child element
type MemberDataPatroni struct {
	Version string `json:"version"`
	Scope   string `json:"scope"`
}

// MemberData Patroni member data from Patroni API
type MemberData struct {
	State           string            `json:"state"`
	Role            string            `json:"role"`
	ServerVersion   int               `json:"server_version"`
	PendingRestart  bool              `json:"pending_restart"`
	ClusterUnlocked bool              `json:"cluster_unlocked"`
	Patroni         MemberDataPatroni `json:"patroni"`
}

func (p *Patroni) GetConfigOrStatus(server *v1.Pod, path string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	apiURLString, err := apiURL(server)
	if err != nil {
		return result, err
	}
	body, err := p.httpGet(apiURLString + path)
	err = json.Unmarshal([]byte(body), &result)
	if err != nil {
		return result, err
	}

	return result, err
}

func (p *Patroni) GetStatus(server *v1.Pod) (map[string]interface{}, error) {
	return p.GetConfigOrStatus(server, statusPath)
}

func (p *Patroni) GetConfig(server *v1.Pod) (map[string]interface{}, error) {
	return p.GetConfigOrStatus(server, configPath)
}

//Restart method restarts instance via Patroni POST API call.
func (p *Patroni) Restart(server *v1.Pod) error {
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(map[string]interface{}{"restart_pending": true})
	if err != nil {
		return fmt.Errorf("could not encode json: %v", err)
	}
	apiURLString, err := apiURL(server)
	if err != nil {
		return err
	}
	status, err := p.GetStatus(server)
	pending_restart, ok := status["pending_restart"]
	if !ok || !pending_restart.(bool) {
		return nil
	}
	return p.httpPostOrPatch(http.MethodPost, apiURLString+restartPath, buf)
}

// GetMemberData read member data from patroni API
func (p *Patroni) GetMemberData(server *v1.Pod) (MemberData, error) {

	apiURLString, err := apiURL(server)
	if err != nil {
		return MemberData{}, err
	}
	response, err := p.httpClient.Get(apiURLString)
	if err != nil {
		return MemberData{}, fmt.Errorf("could not perform Get request: %v", err)
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return MemberData{}, fmt.Errorf("could not read response: %v", err)
	}

	data := MemberData{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return MemberData{}, err
	}

	return data, nil
}
