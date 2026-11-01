package controller

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/cluster"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"gopkg.in/yaml.v3"
)

func (c *Controller) makeClusterConfig() cluster.Config {
	infrastructureRoles := make(map[string]spec.PgUser)
	for k, v := range c.config.InfrastructureRoles {
		infrastructureRoles[k] = v
	}

	return cluster.Config{
		RestConfig:          c.config.RestConfig,
		OpConfig:            config.Copy(c.opConfig),
		PgTeamMap:           &c.pgTeamMap,
		InfrastructureRoles: infrastructureRoles,
		PodServiceAccount:   c.PodServiceAccount,
	}
}

func (c *Controller) clusterWorkerID(clusterName spec.NamespacedName) uint32 {
	// called from the informer handlers, the periodic resync and the API server
	c.clusterWorkersMu.Lock()
	defer c.clusterWorkersMu.Unlock()

	workerID, ok := c.clusterWorkers[clusterName]
	if ok {
		return workerID
	}

	c.clusterWorkers[clusterName] = c.curWorkerID

	if c.curWorkerID == c.opConfig.Workers-1 {
		c.curWorkerID = 0
	} else {
		c.curWorkerID++
	}

	return c.clusterWorkers[clusterName]
}

func (c *Controller) createOperatorCRD(desiredCrd *apiextv1.CustomResourceDefinition) error {
	crd, err := c.KubeClient.CustomResourceDefinitions().Get(context.TODO(), desiredCrd.Name, metav1.GetOptions{})
	if k8sutil.ResourceNotFound(err) {
		if _, err := c.KubeClient.CustomResourceDefinitions().Create(context.TODO(), desiredCrd, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("could not create customResourceDefinition %q: %v", desiredCrd.Name, err)
		}
	}
	if err != nil {
		c.logger.Errorf("could not get customResourceDefinition %q: %v", desiredCrd.Name, err)
	}
	if crd != nil {
		c.logger.Infof("customResourceDefinition %q is already registered and will only be updated", crd.Name)
		crd.Spec = desiredCrd.Spec
		_, err := c.KubeClient.CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("could not update customResourceDefinition %q: %v", crd.Name, err)
		}
	}
	c.logger.Infof("customResourceDefinition %q is registered", crd.Name)

	return wait.PollUntilContextTimeout(context.TODO(), c.config.CRDReadyWaitInterval, c.config.CRDReadyWaitTimeout, false, func(ctx context.Context) (bool, error) {
		c, err := c.KubeClient.CustomResourceDefinitions().Get(context.TODO(), desiredCrd.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		for _, cond := range c.Status.Conditions {
			switch cond.Type {
			case apiextv1.Established:
				if cond.Status == apiextv1.ConditionTrue {
					return true, err
				}
			case apiextv1.NamesAccepted:
				if cond.Status == apiextv1.ConditionFalse {
					return false, fmt.Errorf("name conflict: %v", cond.Reason)
				}
			}
		}

		return false, err
	})
}

func (c *Controller) createPostgresCRD() error {
	crd, err := acidv1.PostgresCRD(c.opConfig.CRDCategories)
	if err != nil {
		return fmt.Errorf("could not create Postgres CRD object: %v", err)
	}
	return c.createOperatorCRD(crd)
}

func (c *Controller) createConfigurationCRD() error {
	crd, err := acidv1.OperatorConfigurationCRD(c.opConfig.CRDCategories)
	if err != nil {
		return fmt.Errorf("could not create OperatorConfiguration CRD object: %v", err)
	}
	return c.createOperatorCRD(crd)
}

func readDecodedRole(s string) (*spec.PgUser, error) {
	var result spec.PgUser
	if err := yaml.Unmarshal([]byte(s), &result); err != nil {
		return nil, fmt.Errorf("could not decode yaml role: %v", err)
	}
	return &result, nil
}

var emptyName = (spec.NamespacedName{})

// Return information about what secrets we need to use to create
// infrastructure roles and in which format are they. This is done in
// compatible way, so that the previous logic is not changed, and handles both
// configuration in ConfigMap & CRD.
func (c *Controller) getInfrastructureRoleDefinitions() []*config.InfrastructureRole {
	var roleDef config.InfrastructureRole

	// take from CRD configuration
	rolesDefs := c.opConfig.InfrastructureRoles

	// check if we can extract something from the configmap config option
	if c.opConfig.InfrastructureRolesDefs != "" {
		// The configmap option could contain either a role description (in the
		// form key1: value1, key2: value2), which has to be used together with
		// an old secret name.

		var secretName spec.NamespacedName
		var err error
		propertySep := ","
		valueSep := ":"

		// The field contains the format in which secret is written, let's
		// convert it to a proper definition
		properties := strings.Split(c.opConfig.InfrastructureRolesDefs, propertySep)
		roleDef = config.InfrastructureRole{Template: false}

		for _, property := range properties {
			values := strings.Split(property, valueSep)
			if len(values) < 2 {
				continue
			}
			name := strings.TrimSpace(values[0])
			value := strings.TrimSpace(values[1])

			switch name {
			case "secretname":
				if err = secretName.DecodeWorker(value, "default"); err != nil {
					c.logger.Warningf("Could not marshal secret name %s: %v", value, err)
				} else {
					roleDef.SecretName = secretName
				}
			case "userkey":
				roleDef.UserKey = value
			case "passwordkey":
				roleDef.PasswordKey = value
			case "rolekey":
				roleDef.RoleKey = value
			case "defaultuservalue":
				roleDef.DefaultUserValue = value
			case "defaultrolevalue":
				roleDef.DefaultRoleValue = value
			default:
				c.logger.Warningf("Role description is not known: %s", properties)
			}
		}

		if roleDef.SecretName != emptyName &&
			(roleDef.UserKey != "" || roleDef.DefaultUserValue != "") &&
			roleDef.PasswordKey != "" {
			rolesDefs = append(rolesDefs, &roleDef)
		}
	}

	if c.opConfig.InfrastructureRolesSecretName != emptyName {
		// At this point we deal with the old format, let's replicate it
		// via existing definition structure and remember that it's just a
		// template, the real values are in user1,password1,inrole1 etc.
		rolesDefs = append(rolesDefs, &config.InfrastructureRole{
			SecretName:  c.opConfig.InfrastructureRolesSecretName,
			UserKey:     "user",
			PasswordKey: "password",
			RoleKey:     "inrole",
			Template:    true,
		})
	}

	return rolesDefs
}

func (c *Controller) getInfrastructureRoles(
	rolesSecrets []*config.InfrastructureRole) (
	map[string]spec.PgUser, error) {

	errors := make([]string, 0)
	noRolesProvided := true
	roles := []spec.PgUser{}
	uniqRoles := make(map[string]spec.PgUser)

	// To be compatible with the legacy implementation we need to return nil if
	// the provided secret name is empty. The equivalent situation in the
	// current implementation is an empty rolesSecrets slice or all its items
	// are empty.
	for _, role := range rolesSecrets {
		if role.SecretName != emptyName {
			noRolesProvided = false
		}
	}

	if noRolesProvided {
		return uniqRoles, nil
	}

	for _, secret := range rolesSecrets {
		infraRoles, err := c.getInfrastructureRole(secret)

		if err != nil || infraRoles == nil {
			c.logger.Debugf("cannot get infrastructure role: %+v", *secret)

			if err != nil {
				errors = append(errors, fmt.Sprintf("%v", err))
			}

			continue
		}

		roles = append(roles, infraRoles...)
	}

	for _, r := range roles {
		if _, exists := uniqRoles[r.Name]; exists {
			msg := "conflicting infrastructure roles: roles[%s] = (%q, %q)"
			c.logger.Debugf(msg, r.Name, uniqRoles[r.Name], r)
		}

		uniqRoles[r.Name] = r
	}

	if len(errors) > 0 {
		return uniqRoles, fmt.Errorf("%s", strings.Join(errors, `', '`))
	}

	return uniqRoles, nil
}

// Generate list of users representing one infrastructure role based on its
// description in various K8S objects. An infrastructure role could be
// described by a secret and optionally a config map. The former should contain
// the secret information, i.e. username, password, role. The latter could
// contain an extensive description of the role and even override an
// information obtained from the secret (except a password).
//
// This function returns a list of users to be compatible with the previous
// behaviour, since we don't know how many users are actually encoded in the
// secret if it's a "template" role. If the provided role is not a template
// one, the result would be a list with just one user in it.
//
// FIXME: This dependency on two different objects is rather unnecessary
// complicated, so let's get rid of it via deprecation process.
func (c *Controller) getInfrastructureRole(
	infraRole *config.InfrastructureRole) (
	[]spec.PgUser, error) {

	rolesSecret := infraRole.SecretName
	roles := []spec.PgUser{}

	if rolesSecret == emptyName {
		// we don't have infrastructure roles defined, bail out
		return nil, nil
	}

	infraRolesSecret, err := c.KubeClient.
		Secrets(rolesSecret.Namespace).
		Get(context.TODO(), rolesSecret.Name, metav1.GetOptions{})
	if err != nil {
		msg := "could not get infrastructure roles secret %s/%s: %v"
		return nil, fmt.Errorf(msg, rolesSecret.Namespace, rolesSecret.Name, err)
	}

	secretData := infraRolesSecret.Data

	if infraRole.Template {
	Users:
		for i := 1; i <= len(secretData); i++ {
			properties := []string{
				infraRole.UserKey,
				infraRole.PasswordKey,
				infraRole.RoleKey,
			}
			t := spec.PgUser{Origin: spec.RoleOriginInfrastructure}
			for _, p := range properties {
				key := fmt.Sprintf("%s%d", p, i)
				if val, present := secretData[key]; !present {
					if p == "user" {
						// exit when the user name with the next sequence id is
						// absent
						break Users
					}
				} else {
					s := string(val)
					switch p {
					case "user":
						t.Name = s
					case "password":
						t.Password = s
					case "inrole":
						t.MemberOf = append(t.MemberOf, s)
					default:
						c.logger.Warningf("unknown key %q", p)
					}
				}
				// XXX: This is a part of the original implementation, which is
				// rather obscure. Why do we delete this key? Wouldn't it be
				// used later in comparison for configmap?
				delete(secretData, key)
			}

			if t.Valid() {
				roles = append(roles, t)
			} else {
				msg := "infrastructure role %q is not complete and ignored"
				c.logger.Warningf(msg, t)
			}
		}
	} else {
		roleDescr := &spec.PgUser{Origin: spec.RoleOriginInfrastructure}

		if details, exists := secretData[infraRole.Details]; exists {
			if err := yaml.Unmarshal(details, &roleDescr); err != nil {
				return nil, fmt.Errorf("could not decode yaml role: %v", err)
			}
		} else {
			roleDescr.Name = util.Coalesce(string(secretData[infraRole.UserKey]), infraRole.DefaultUserValue)
			roleDescr.Password = string(secretData[infraRole.PasswordKey])
			roleDescr.MemberOf = append(roleDescr.MemberOf,
				util.Coalesce(string(secretData[infraRole.RoleKey]), infraRole.DefaultRoleValue))
		}

		if !roleDescr.Valid() {
			msg := "infrastructure role %q is not complete and ignored"
			c.logger.Warningf(msg, roleDescr)

			return nil, nil
		}

		if roleDescr.Name == "" {
			msg := "infrastructure role %q has no name defined and is ignored"
			c.logger.Warningf(msg, roleDescr.Name)
			return nil, nil
		}

		if roleDescr.Password == "" {
			msg := "infrastructure role %q has no password defined and is ignored"
			c.logger.Warningf(msg, roleDescr.Name)
			return nil, nil
		}

		roles = append(roles, *roleDescr)
	}

	// Now plot twist. We need to check if there is a configmap with the same
	// name and extract a role description if it exists.
	infraRolesMap, err := c.KubeClient.
		ConfigMaps(rolesSecret.Namespace).
		Get(context.TODO(), rolesSecret.Name, metav1.GetOptions{})
	if err == nil {
		// we have a configmap with username - json description, let's read and decode it
		for role, s := range infraRolesMap.Data {
			roleDescr, err := readDecodedRole(s)
			if err != nil {
				return nil, fmt.Errorf("could not decode role description: %v", err)
			}
			// check if we have a a password in a configmap
			c.logger.Debugf("found role description for role %q: %+v", role, roleDescr)
			if passwd, ok := secretData[role]; ok {
				roleDescr.Password = string(passwd)
				delete(secretData, role)
			} else {
				c.logger.Warningf("infrastructure role %q has no password defined and is ignored", role)
				continue
			}
			roleDescr.Name = role
			roleDescr.Origin = spec.RoleOriginInfrastructure
			roles = append(roles, *roleDescr)
		}
	}

	// TODO: check for role collisions
	return roles, nil
}

func (c *Controller) loadPostgresTeams() {
	pgTeams, err := c.KubeClient.PostgresTeamsGetter.PostgresTeams(c.opConfig.WatchedNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		c.logger.Errorf("could not list postgres team objects: %v", err)
	}

	c.pgTeamMap.Load(pgTeams)
	c.logger.Debugf("Internal Postgres Team Cache: %#v", c.pgTeamMap)
}

func (c *Controller) postgresTeamAdd(obj interface{}) {
	pgTeam, ok := obj.(*acidv1.PostgresTeam)
	if !ok {
		c.logger.Errorf("could not cast to PostgresTeam spec")
		return
	}
	c.logger.Debugf("PostgreTeam %q added. Reloading postgres team CRDs and overwriting cached map", pgTeam.Name)
	c.loadPostgresTeams()
}

func (c *Controller) postgresTeamUpdate(prev, obj interface{}) {
	pgTeam, ok := obj.(*acidv1.PostgresTeam)
	if !ok {
		c.logger.Errorf("could not cast to PostgresTeam spec")
		return
	}
	c.logger.Debugf("PostgreTeam %q updated. Reloading postgres team CRDs and overwriting cached map", pgTeam.Name)
	c.loadPostgresTeams()
}

func (c *Controller) podClusterName(pod *v1.Pod) spec.NamespacedName {
	if name, ok := pod.Labels[c.opConfig.ClusterNameLabel]; ok {
		return spec.NamespacedName{
			Namespace: pod.Namespace,
			Name:      name,
		}
	}

	return spec.NamespacedName{}
}
