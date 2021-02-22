package patroni

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

const (
	failoverPath = "/failover"
	configPath   = "/config"
	apiPort      = 8008
	timeout      = 30 * time.Second
)

// Interface describe patroni methods
type Interface interface {
	Switchover(master *v1.Pod, candidate string) error
	SetPostgresParameters(server *v1.Pod, options map[string]string) error
	SetStandbyClusterParameters(server *v1.Pod, options map[string]string) error
	GetPatroniMemberState(pod *v1.Pod) (string, error)
}

// Patroni API client
type Patroni struct {
	httpClient *http.Client
	logger     *logrus.Entry
}

// New create patroni
func New(logger *logrus.Entry) *Patroni {
	cl := http.Client{
		Timeout: timeout,
	}

	return &Patroni{
		logger:     logger,
		httpClient: &cl,
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

	p.logger.Debugf("making %s http request: %s", method, request.URL.String())

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

//SetStandbyClusterParameters sets StandbyCluster options via Patroni patch API call.
func (p *Patroni) SetStandbyClusterParameters(server *v1.Pod, parameters map[string]string) error {
	buf := &bytes.Buffer{}
	err := json.NewEncoder(buf).Encode(map[string]interface{}{"standby_cluster": parameters})
	if err != nil {
		return fmt.Errorf("could not encode json: %v", err)
	}
	apiURLString, err := apiURL(server)
	if err != nil {
		return err
	}
	return p.httpPostOrPatch(http.MethodPatch, apiURLString+configPath, buf)
}

//GetPatroniMemberState returns a state of member of a Patroni cluster
func (p *Patroni) GetPatroniMemberState(server *v1.Pod) (string, error) {

	apiURLString, err := apiURL(server)
	if err != nil {
		return "", err
	}
	response, err := p.httpClient.Get(apiURLString)
	if err != nil {
		return "", fmt.Errorf("could not perform Get request: %v", err)
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("could not read response: %v", err)
	}

	data := make(map[string]interface{})
	err = json.Unmarshal(body, &data)
	if err != nil {
		return "", err
	}

	state, ok := data["state"].(string)
	if !ok {
		return "", errors.New("Patroni Get call response contains wrong type for 'state' field")
	}

	return state, nil

}
