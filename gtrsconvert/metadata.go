package gtrsconvert

import "encoding/json"

type Metadata map[string]any

func (m *Metadata) UnmarshalText(text []byte) error {
	// create m if nil
	if m == nil {
		*m = Metadata{}
	}
	// cast the type so it unmarshals correctly
	var am map[string]any = map[string]any(*m)
	err := json.Unmarshal(text, &am)
	if err != nil {
		return err
	}
	// assign the value back
	*m = (Metadata)(am)
	return nil
}

func (m Metadata) MarshalText() (text []byte, err error) {
	// converts type so it marshals correctly
	return json.Marshal(map[string]any(m))
}
