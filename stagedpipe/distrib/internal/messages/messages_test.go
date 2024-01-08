package messages

import (
	"log"
	"reflect"
	"testing"
)

// TestVersionMarshaling tests the MarshalBinary and UnmarshalBinary methods of Version.
func TestVersionMarshaling(t *testing.T) {
	tests := []struct {
		name    string
		version Version
	}{
		{"Version 1.2.3", Version{Major: 1, Minor: 2, Patch: 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Testing MarshalBinary
			marshaled, err := tt.version.MarshalBinary(nil)
			if err != nil {
				t.Errorf("MarshalBinary() error = %v", err)
				return
			}

			// Testing UnmarshalBinary
			var v Version
			err = v.UnmarshalBinary(marshaled)
			if err != nil {
				t.Errorf("UnmarshalBinary() error = %v", err)
				return
			}

			// Comparing the result
			if v != tt.version {
				t.Errorf("Unmarshaled Version = %v, want %v", v, tt.version)
			}
		})
	}
}

// TestConnectMarshaling tests the MarshalBinary and UnmarshalBinary methods of Connect.
func TestConnectMarshaling(t *testing.T) {
	tests := []struct {
		name    string
		connect Connect
	}{
		{"Connect with Version 1.2.3", Connect{Type: CNTRequestGroup, Version: Version{Major: 1, Minor: 2, Patch: 3}}},
		// Add more test cases for different Connect structs.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Testing MarshalBinary
			marshaled, err := tt.connect.MarshalBinary(nil)
			if err != nil {
				t.Errorf("MarshalBinary() error = %v", err)
				return
			}

			log.Println("marshaled len: ", len(marshaled))

			// Testing UnmarshalBinary
			var c Connect
			err = c.UnmarshalBinary(marshaled)
			if err != nil {
				t.Errorf("UnmarshalBinary() error = %v", err)
				return
			}

			// Comparing the result
			if c != tt.connect {
				t.Errorf("Unmarshaled Connect = %v, want %v", c, tt.connect)
			}
		})
	}
}

// TestLenMethods tests the Len() method for each struct.
func TestLenMethods(t *testing.T) {
	// Test for Version
	version := Version{Major: 1, Minor: 2, Patch: 3}
	if version.Len() != 3 {
		t.Errorf("Version.Len() = %d, want 3", version.Len())
	}

	// Test for Connect
	connect := Connect{Type: CNTRequestGroup, Version: version}
	if connect.Len() != 4 { // 1 byte for Type and 3 bytes for Version
		t.Errorf("Connect.Len() = %d, want 4", connect.Len())
	}

	// Add tests for Len() methods of other structs.
}

func TestErrorMarshaling(t *testing.T) {
	tests := []struct {
		name  string
		error Error
	}{
		{"Error with message", Error{Msg: "test error", Code: ECInternal}},
		// Add more test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled, err := tt.error.MarshalBinary(nil)
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			var e Error
			err = e.UnmarshalBinary(marshaled)
			if err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}

			if !reflect.DeepEqual(e, tt.error) {
				t.Errorf("Unmarshaled Error = %+v, want %+v", e, tt.error)
			}
		})
	}
}

// Additional tests should be written for Request, Data, Control, and Error in a similar fashion.

func TestRequestMarshaling(t *testing.T) {
	tests := []struct {
		name    string
		request Request
	}{
		{"Request with data", Request{Data: Data{Raw: []byte("test data"), ConnID: 1, RequestGroupID: 2}, Error: Error{Msg: "test error", Code: ECInternal}}},
		// Add more test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled, err := tt.request.MarshalBinary(nil)
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			var r Request
			err = r.UnmarshalBinary(marshaled)
			if err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}

			if !reflect.DeepEqual(r, tt.request) {
				t.Errorf("Unmarshaled Request = %+v, want %+v", r, tt.request)
			}
		})
	}
}

func TestDataMarshaling(t *testing.T) {
	tests := []struct {
		name string
		data Data
	}{
		{"Data with raw bytes", Data{Raw: []byte("test data"), ConnID: 1, RequestGroupID: 2}},
		// Add more test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled, err := tt.data.MarshalBinary(nil)
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			var d Data
			err = d.UnmarshalBinary(marshaled)
			if err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}

			if !reflect.DeepEqual(d, tt.data) {
				t.Errorf("Unmarshaled Data = %+v, want %+v", d, tt.data)
			}
		})
	}
}

func TestControlMarshaling(t *testing.T) {
	tests := []struct {
		name    string
		control Control
	}{
		{"Control with config data", Control{Type: CTConfig, Config: []byte("config data"), Error: Error{Msg: "test error", Code: ECInternal}, Version: Version{Major: 1, Minor: 2, Patch: 3}}},
		// Add more test cases.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled, err := tt.control.MarshalBinary(nil)
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			var c Control
			err = c.UnmarshalBinary(marshaled)
			if err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}

			if !reflect.DeepEqual(c, tt.control) {
				t.Errorf("Unmarshaled Control = %+v, want %+v", c, tt.control)
			}
		})
	}
}
