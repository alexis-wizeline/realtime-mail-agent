package ingestevents

import (
	"encoding/json"
	"errors"
	"io"
)

// TODO move this to aseparate package for only models
type IngestEvent struct {
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	MessageID string `json:"message_id"`
	Type      string `json:"type"`
}

func (i *IngestEvent) Valid() error {
	if len(i.EventID) == 0 {
		return errors.New("event id cannot be empty")
	}
	if len(i.UserID) == 0 {
		return errors.New("user id cannot be empty")
	}
	if len(i.MessageID) == 0 {
		return errors.New("message id cannot be empty")
	}
	if len(i.Type) == 0 {
		return errors.New("type cannot by empty")
	}
	return nil
}

func (i *IngestEvent) Serialize() ([]byte, error) {
	return json.Marshal(i)
}

func IngestEVentReqFromBodyRequest(b io.ReadCloser) (*IngestEvent, error) {
	defer b.Close()

	decoder := json.NewDecoder(b)
	i := &IngestEvent{}
	err := decoder.Decode(i)
	if err != nil {
		return nil, err
	}

	err = i.Valid()
	if err != nil {
		return nil, err
	}

	return i, nil
}
