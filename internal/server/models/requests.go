package models

import (
	"encoding/json"
	"io"
)

type IngestEvent struct {
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	MessageID string `json:"message_id"`
	Type      string `json:"type"`
}

func IngestEVentReqFromBodyRequest(b io.ReadCloser) (*IngestEvent, error) {
	defer b.Close()

	buf, err := io.ReadAll(b)
	if err != nil {
		return nil, err
	}
	i := &IngestEvent{}
	err = json.Unmarshal(buf, i)
	if err != nil {
		return nil, err
	}

	return i, nil
}
