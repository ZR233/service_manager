package service_manager

import (
	"testing"
)

func TestManager_NewConsumer(t *testing.T) {

	type args struct {
		options []ConsumerOption
	}
	tests := []struct {
		name        string
		servicePath string
		args        args
		wantErr     bool
	}{
		{"no service", "test123", args{}, true},
		{"service data error", "test_error", args{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager([]string{"192.168.0.3:2181"})
			_, err := m.NewConsumer(m.NewService(tt.servicePath), tt.args.options...)
			err = m.RunSync()

			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Log(err)

		})
	}
}
