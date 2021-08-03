package pubsub

import (
	"encoding/json"
	"time"
)

// Datetime format YYYY-MM-DD hh:mm:ss
const TimeFormat = "2006-01-02 15:04:05"

// Empty interface for Kafaka Messages on stream
type MessageInterface interface {
}

// Converting Raw Message from Produce() to message which can be consumed by
// other wiadapters. Raw data will be dumped into payload, timestamp will be added
// and message_type will be set to empty string. This is solely to make messages
// compatible with the ones created by other wiadapters
type CompatibleBaseMessage struct {
	Payload     interface{} `json:"payload"`
	MessageType string      `json:"message_type"`
	Timestamp   string      `json:"timestamp"`
}

// Converts raw message to compatible message and then transforms it to JSON bytes
// @params:
// 		message: Message which is to be sent
// @returns:
//		[]byte: slice of byte for given message
func ToCompatibleMessage(message MessageInterface) []byte {
	compatibleMessage := CompatibleBaseMessage{
		Payload:     message,
		MessageType: "",
		Timestamp:   time.Now().Format(TimeFormat),
	}
	return ToJSONByteSlice(compatibleMessage)

}

// Converts message to JSON-ified slice of bytes
// @params:
//		message: Message which is to be sent
// @returns:
// 		[]byte: slice of byte for given message
func ToJSONByteSlice(message MessageInterface) []byte {
	marshalled, err := json.Marshal(message)
	if err != nil {
		panic(err.Error())
	}
	return marshalled
}

// Converts message to JSON-ified string
// @params:
//		message: Message which is to be sent
// @returns:
// 		[]byte: string for given message
func ToJSONString(message MessageInterface) string {
	arr := ToJSONByteSlice(message)
	return string(arr)
}

// Transforms data to Compatible Message
// @params:
// 		data: slice of bytes which is to be unloaded
// 		message: pointer to a message struct in which data will be unloaded
// @returns:
// 		error: if any error occurred in data transformation
func FromCompatibleMessage(data []byte, compatibleMessage *CompatibleBaseMessage) error {
	err := json.Unmarshal(data, compatibleMessage)
	return err
}

func GetPayload(data []byte, message MessageInterface) error {
	msg := CompatibleBaseMessage{}
	err := FromCompatibleMessage(data, &msg)
	if err != nil {
		return err
	}
	payload := msg.Payload
	payloadBody, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return FromJSONByteSlice(payloadBody, message)
}

// Transforms JSON-ified slice of byte back to message.
// @params:
// 		data: slice of bytes which is to be unloaded
// 		message: pointer to a message struct in which data will be unloaded
// @returns:
// 		error: if any error occurred in data transformation
func FromJSONByteSlice(data []byte, message MessageInterface) error {
	err := json.Unmarshal(data, message)
	return err
}

// Transforms JSON-ified string back to message.
// Similar to FromJSONByteSlice
func FromJSONString(data string, message MessageInterface) error {
	bytes := []byte(data)
	return FromJSONByteSlice(bytes, message)
}
