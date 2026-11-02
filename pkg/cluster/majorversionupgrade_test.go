package cluster

import (
	"reflect"
	"testing"
)

func TestParseLaggingLogicalReplicationSlots(t *testing.T) {
	tests := []struct {
		name      string
		output    string
		wantSlots []string
		wantErr   bool
	}{
		{
			name:      "no slots",
			wantSlots: []string{},
		},
		{
			name:      "slots at threshold are allowed",
			output:    "at_threshold|33554432\n",
			wantSlots: []string{},
		},
		{
			name:      "lagging slot is returned",
			output:    "lagging_slot|33554433\n",
			wantSlots: []string{"lagging_slot|33554433"},
		},
		{
			name:      "only lagging slots are returned",
			output:    "healthy_slot|100\nlagging_slot|33554433\n",
			wantSlots: []string{"lagging_slot|33554433"},
		},
		{
			name:    "invalid output is rejected",
			output:  "unexpected output",
			wantErr: true,
		},
		{
			name:    "invalid lag is rejected",
			output:  "slot|unknown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlots, err := parseLaggingLogicalReplicationSlots(tt.output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLaggingLogicalReplicationSlots() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(gotSlots, tt.wantSlots) {
				t.Errorf("parseLaggingLogicalReplicationSlots() = %v, want %v", gotSlots, tt.wantSlots)
			}
		})
	}
}
