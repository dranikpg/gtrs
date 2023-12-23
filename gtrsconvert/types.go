package gtrsconvert

import (
	"encoding"
	"time"
)

type AsciiTime time.Time

var _ encoding.BinaryMarshaler = (AsciiTime)(time.Time{})
var _ encoding.BinaryUnmarshaler = (*AsciiTime)(nil)

func (m *AsciiTime) UnmarshalBinary(text []byte) error {
	// create m if nil
	if m == nil {
		*m = AsciiTime{}
	}
	// cast the type so it unmarshals correctly
	var am *time.Time = (*time.Time)(m)
	err := am.UnmarshalText(text)
	if err != nil {
		return err
	}
	// assign the value back
	*m = (AsciiTime)(*am)
	return nil
}

func (m AsciiTime) MarshalBinary() (text []byte, err error) {
	// converts type so it marshals correctly
	return time.Time(m).MarshalText()
}

func (m *AsciiTime) Time() time.Time {
	if m == nil {
		return time.Time{}
	}
	return time.Time(*m)
}
