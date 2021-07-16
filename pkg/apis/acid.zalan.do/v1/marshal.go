package v1

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type postgresqlCopy Postgresql
type postgresStatusCopy PostgresStatus

// MarshalJSON converts a maintenance window definition to JSON.
func (m *MaintenanceWindow) MarshalJSON() ([]byte, error) {
	if m.Everyday {
		return []byte(fmt.Sprintf("\"%s-%s\"",
			m.StartTime.Format("15:04"),
			m.EndTime.Format("15:04"))), nil
	}

	return []byte(fmt.Sprintf("\"%s:%s-%s\"",
		m.Weekday.String()[:3],
		m.StartTime.Format("15:04"),
		m.EndTime.Format("15:04"))), nil
}

// UnmarshalJSON converts a JSON to the maintenance window definition.
func (m *MaintenanceWindow) UnmarshalJSON(data []byte) error {
	var (
		got MaintenanceWindow
		err error
	)

	parts := strings.Split(string(data[1:len(data)-1]), "-")
	if len(parts) != 2 {
		return fmt.Errorf("incorrect maintenance window format")
	}

	fromParts := strings.Split(parts[0], ":")
	switch len(fromParts) {
	case 3:
		got.Everyday = false
		got.Weekday, err = parseWeekday(fromParts[0])
		if err != nil {
			return fmt.Errorf("could not parse weekday: %v", err)
		}

		got.StartTime, err = parseTime(fromParts[1] + ":" + fromParts[2])
	case 2:
		got.Everyday = true
		got.StartTime, err = parseTime(fromParts[0] + ":" + fromParts[1])
	default:
		return fmt.Errorf("incorrect maintenance window format")
	}
	if err != nil {
		return fmt.Errorf("could not parse start time: %v", err)
	}

	got.EndTime, err = parseTime(parts[1])
	if err != nil {
		return fmt.Errorf("could not parse end time: %v", err)
	}

	if got.EndTime.Before(&got.StartTime) {
		return fmt.Errorf("'From' time must be prior to the 'To' time")
	}

	*m = got

	return nil
}

// UnmarshalJSON converts a JSON to the status subresource definition.
func (ps *PostgresStatus) UnmarshalJSON(data []byte) error {
	var (
		tmp    postgresStatusCopy
		status string
	)

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		metaErr := json.Unmarshal(data, &status)
		if metaErr != nil {
			return fmt.Errorf("could not parse status: %v; err %v", string(data), metaErr)
		}
		tmp.PostgresClusterStatus = status
	}
	*ps = PostgresStatus(tmp)

	return nil
}

// UnmarshalJSON converts a JSON into the PostgreSQL object.
func (p *Postgresql) UnmarshalJSON(data []byte) error {
	var tmp postgresqlCopy

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		metaErr := json.Unmarshal(data, &tmp.ObjectMeta)
		if metaErr != nil {
			return err
		}

		tmp.Error = err.Error()
		tmp.Status.PostgresClusterStatus = ClusterStatusInvalid

		*p = Postgresql(tmp)

		return nil
	}
	tmp2 := Postgresql(tmp)

	if clusterName, err := extractClusterName(tmp2.ObjectMeta.Name, tmp2.Spec.TeamID); err != nil {
		tmp2.Error = err.Error()
		tmp2.Status = PostgresStatus{PostgresClusterStatus: ClusterStatusInvalid}
	} else if err := validateCloneClusterDescription(tmp2.Spec.Clone); err != nil {

		tmp2.Error = err.Error()
		tmp2.Status.PostgresClusterStatus = ClusterStatusInvalid
	} else {
		tmp2.Spec.ClusterName = clusterName
	}

	*p = tmp2

	return nil
}

// UnmarshalJSON convert to Duration from byte slice of json
func (d *Duration) UnmarshalJSON(b []byte) error {
	var (
		v   interface{}
		err error
	)
	if err = json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case string:
		t, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		*d = Duration(t)
		return nil
	case float64:
		t := time.Duration(val)
		*d = Duration(t)
		return nil
	default:
		return fmt.Errorf("could not recognize type %T as a valid type to unmarshal to Duration", val)
	}
}
