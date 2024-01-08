// Package messages provides the messages that are sent between the overseer, coordinator,
// and workers.
// WARNING: THIS PACKAGE IS FRAIL!!!!
// Editing this package takes great care, because we are manually encoding and decoding the messages.
// Adding/Removing/Changing a field has reprecussions on the message format.
// If you change a field or encode anything new, you MUST:
// 1. Update the MarshalBinary and UnmarshalBinary methods for the type.
// 2. Update the Len method for the type.
// 3. Update the tests for the type.
package messages

import (
	"encoding/binary"
	"fmt"
	"log"
)

// BinaryMarshaler is the interface that must be implemented by a type that can be sent over the wire.
type BinaryMarshaler interface {
	// MarshalBinary encodes the type into b and returns the result. If b is too small, it will be reallocated.
	// If b is too large, it will be truncated.
	MarshalBinary(b []byte) ([]byte, error)
	// UnmarshalBinary decodes the type from b into the receiver.
	UnmarshalBinary(b []byte) error
}

// MessageType indicates the type of message that is sent.
type MessageType uint8

const (
	MTUnknown MessageType = 0
	MTConnect MessageType = 1
	MTControl MessageType = 2
)

// ConnType indicates the type of connection that is being made.
type ConnType uint8

const (
	CNTUnknown      ConnType = 0
	CNTRequestGroup ConnType = 1
)

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Version{}

// Version details the version information.
// Binary format: [major(uint8)][minor(uint8)][patch(uint8)]
type Version struct {
	// Major is the major version.
	Major uint8
	// Minor is the minor version.
	Minor uint8
	// Patch is the patch version.
	Patch uint8
}

// String implements fmt.Stringer.
func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// IsZero returns true if the version is the zero value.
func (v *Version) IsZero() bool {
	if v == nil {
		return true
	}
	return v.Major == 0 && v.Minor == 0 && v.Patch == 0
}

// Equal returns true if the two versions are equal.
func (v *Version) Equal(v2 Version) bool {
	return v.Major == v2.Major && v.Minor == v2.Minor && v.Patch == v2.Patch
}

// Len returns the length of the type in binary form.
func (v *Version) Len() int {
	return 3
}

// MarshalBinary encodes the type into b. If b is too small, it will be reallocated.
func (v *Version) MarshalBinary(b []byte) ([]byte, error) {
	b = getReqSlice(b, v.Len())
	b[0] = v.Major
	b[1] = v.Minor
	b[2] = v.Patch
	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver.
func (v *Version) UnmarshalBinary(b []byte) error {
	if len(b) < v.Len() {
		return fmt.Errorf("invalid length for version: %d", len(b))
	}
	v.Major = b[0]
	v.Minor = b[1]
	v.Patch = b[2]
	return nil
}

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Connect{}

// Connect is the message that is sent when a connection is made.
// Binary format: [type(uint8)][version(Version)]
type Connect struct {
	// Type is the type of connection that is being made.
	Type ConnType
	// Version is the version of the connection.
	Version Version
}

// Len returns the length of the type in binary form.
func (c *Connect) Len() int {
	return 1 + c.Version.Len()
}

// MarshalBinary encodes the type into b. If b is too small, it will be reallocated.
func (c *Connect) MarshalBinary(b []byte) ([]byte, error) {
	var err error
	b = getReqSlice(b, c.Len())
	log.Println("b: ", len(b))

	b[0] = uint8(c.Type)
	_, err = c.Version.MarshalBinary(b[1:])
	if err != nil {
		return nil, err
	}
	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver. If b is too small, it will be reallocated.
func (c *Connect) UnmarshalBinary(b []byte) error {
	if len(b) != c.Len() {
		return fmt.Errorf("invalid length for connect: %d", len(b))
	}
	c.Type = ConnType(b[0])
	return c.Version.UnmarshalBinary(b[1:])
}

// ControlType indicates the type of control message.
type ControlType uint8

const (
	CTUnknown ControlType = 0
	CTError   ControlType = 1
	CTConfig  ControlType = 2
	CTCancel  ControlType = 3
	CTFin     ControlType = 4
)

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Control{}

// Control message.
// Binary format: [type(uint8)][Version][config start(uint32)][Error][config]
type Control struct {
	Config  []byte
	Error   Error
	Version Version
	Type    ControlType
}

// Len returns the length of the type in binary form.
func (c *Control) Len() int {
	return len(c.Config) + c.Error.Len() + c.Version.Len() + 1 /*type*/ + 4 /*config start*/
}

func (c *Control) MarshalBinary(b []byte) ([]byte, error) {
	const lenSize = 4
	var err error
	b = getReqSlice(b, c.Len())

	b[0] = uint8(c.Type)

	_, err = c.Version.MarshalBinary(b[1:])
	if err != nil {
		return nil, err
	}

	configStart := 1 + c.Version.Len() + c.Error.Len() + lenSize
	binary.BigEndian.PutUint32(b[1+c.Version.Len():], uint32(configStart))

	// Write the error.
	_, err = c.Error.MarshalBinary(b[1+c.Version.Len()+lenSize : configStart])
	if err != nil {
		return nil, err
	}

	// Write the config.
	copy(b[configStart:], c.Config)
	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver. If b is too small, it will be reallocated.
func (c *Control) UnmarshalBinary(b []byte) error {
	const lenSize = 4

	if len(b) < 1+c.Version.Len() {
		return fmt.Errorf("invalid length for control: %d", len(b))
	}

	c.Type = ControlType(b[0])

	if err := c.Version.UnmarshalBinary(b[1 : 1+c.Version.Len()]); err != nil {
		return err
	}

	configStartPos := 1 + c.Version.Len()
	if len(b) < configStartPos {
		return fmt.Errorf("invalid length for control config: %d", len(b))
	}
	configStart := binary.BigEndian.Uint32(b[configStartPos:])

	if err := c.Error.UnmarshalBinary(b[1+c.Version.Len()+lenSize : configStart]); err != nil {
		return err
	}

	c.Config = b[configStart:]
	return nil
}

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Request{}

// Request message.
// Binary format: [data start(uint32)][Error][Data]
type Request struct {
	Error Error
	Data  Data
}

// Len returns the length of the type in binary form.
func (r *Request) Len() int {
	return r.Data.Len() + r.Error.Len() + 4 /*data start*/
}

// MarshalBinary encodes the type into b. If b is too small, it will be reallocated.
func (r *Request) MarshalBinary(b []byte) ([]byte, error) {
	const lenSize = 4
	b = getReqSlice(b, r.Len())

	// Encode where the data starts, as Error and Data are variable length.
	dataStart := r.Error.Len() + lenSize
	log.Println("dataStart: ", dataStart)
	binary.BigEndian.PutUint32(b[0:4], uint32(dataStart))

	_, err := r.Error.MarshalBinary(b[lenSize:])
	if err != nil {
		return nil, err
	}

	_, err = r.Data.MarshalBinary(b[r.Error.Len()+lenSize:])
	if err != nil {
		return nil, err
	}

	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver. If b is too small, it will be reallocated.
func (r *Request) UnmarshalBinary(b []byte) error {
	if len(b) < r.Error.Len()+4 /*data start*/ {
		return fmt.Errorf("invalid length for request: %d", len(b))
	}
	dataStart := binary.BigEndian.Uint32(b[0:4])
	log.Println("dataStart: ", dataStart)
	if err := r.Error.UnmarshalBinary(b[4:dataStart]); err != nil {
		return err
	}
	if len(b) < int(dataStart) {
		return nil
	}
	return r.Data.UnmarshalBinary(b[dataStart:])
}

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Data{}

// Data for the message.
// Binary format: [conn id(uint32)][request group id(uint32)][data]
type Data struct {
	Raw            []byte
	ConnID         uint32
	RequestGroupID uint32
}

// Len returns the length of the type in binary form.
func (d *Data) Len() int {
	return 8 + len(d.Raw)
}

// MarshalBinary encodes the type into b. If b is too small, it will be reallocated.
func (d *Data) MarshalBinary(b []byte) ([]byte, error) {
	b = getReqSlice(b, d.Len())
	binary.BigEndian.PutUint32(b[0:4], d.ConnID)
	binary.BigEndian.PutUint32(b[4:8], d.RequestGroupID)
	copy(b[8:], d.Raw)
	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver. If b is too small, it will be reallocated.
func (d *Data) UnmarshalBinary(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("invalid length for data: %d", len(b))
	}
	d.ConnID = uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	d.RequestGroupID = uint32(b[4])<<24 | uint32(b[5])<<16 | uint32(b[6])<<8 | uint32(b[7])
	d.Raw = b[8:]
	return nil
}

// ErrorCode indicates the type of error.
type ErrorCode uint8

const (
	ECUnknown       ErrorCode = 0
	ECNotAuthorized ErrorCode = 1
	ECInternal      ErrorCode = 2
	ECCancelled     ErrorCode = 3
	ECPipelineError ErrorCode = 4
)

// Makes sure that the types implement the BinaryMarshaler interface.
var _ BinaryMarshaler = &Error{}

// Error message.
// Binary format: [code(uint8)][msg(string)]
type Error struct {
	Msg  string
	Code ErrorCode
}

// Error implements error.Error().
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == ECUnknown {
		return ""
	}
	return fmt.Sprintf("%v: %s", e.Code, e.Msg)
}

// IsZero returns true if the error is the zero value.
func (e *Error) IsZero() bool {
	if e == nil {
		return true
	}
	return e.Code == ECUnknown
}

// Len returns the length of the type in binary form.
func (e *Error) Len() int {
	if e == nil {
		return 1
	}
	return len(e.Msg) + 1
}

// MarshalBinary encodes the type into b. If b is too small, it will be reallocated.
func (e *Error) MarshalBinary(b []byte) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("cannot marshal nil error, but also, don't use a *Error, use Error")
	}
	b = getReqSlice(b, e.Len())
	b[0] = uint8(e.Code)
	copy(b[1:], e.Msg)
	return b, nil
}

// UnmarshalBinary decodes the type from b into the receiver. If b is too small, it will be reallocated.
func (e *Error) UnmarshalBinary(b []byte) error {
	if len(b) < 1 {
		return fmt.Errorf("invalid length for error: %d", len(b))
	}
	e.Code = ErrorCode(b[0])
	e.Msg = string(b[1:])
	return nil
}

// getReqSlice returns a slice of length size. If b is too small, it will be reallocated.
// It will also shrink the slice len down to size if it is too large (which does not affect capacity)
func getReqSlice(b []byte, size int) []byte {
	switch {
	case cap(b) == size:
		return b[:size]
	case cap(b) > size:
		return b[:size]
	case cap(b) < size:
		return make([]byte, size)
	}
	panic("unreachable")
}
