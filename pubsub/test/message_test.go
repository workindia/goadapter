package pubsub

import (
	"testing"

	"github.com/workindia/wigoadapters/pubsub"
)

type Person struct {
	Age     int      `json:"age"`
	Name    string   `json:"name"`
	Parents []string `json:"parents"`
}

func TestMessageCompatible(t *testing.T) {
	tim := Person{
		Age:     12,
		Name:    "Tim",
		Parents: []string{"Alice", "Bob"},
	}
	msg := pubsub.ToCompatibleMessage(tim)
	mit := Person{}
	err := pubsub.GetPayload(msg, &mit)
	if err != nil {
		t.Log("Failed to get payload")
	}
	checkTwoPersons(mit, tim, t)
}

func TestMessageBytes(t *testing.T) {
	tim := Person{
		Age:     12,
		Name:    "Tim",
		Parents: []string{"Alice", "Bob"},
	}
	bytes := pubsub.ToJSONByteSlice(tim)
	if len(bytes) != 49 {
		t.Log("Conversion to JSON Byte Slice Failed")
		t.Fail()
	}

	mit := Person{}
	err := pubsub.FromJSONByteSlice(bytes, &mit)
	if err != nil {
		t.Log("Error in converting JSON back to struct", err.Error())
		t.Fail()
	}
	checkTwoPersons(mit, tim, t)
}

func TestMessageString(t *testing.T) {
	tim := Person{
		Age:     12,
		Name:    "Tim",
		Parents: []string{"Alice", "Bob"},
	}
	str := pubsub.ToJSONString(tim)
	if str != "{\"age\":12,\"name\":\"Tim\",\"parents\":[\"Alice\",\"Bob\"]}" {
		t.Log("Conversion to JSON string failed.")
		t.Fail()
	}
	mit := Person{}
	err := pubsub.FromJSONString(str, &mit)
	if err != nil {
		t.Log("Error in converting JSON back to struct", err.Error())
		t.Fail()
	}
	checkTwoPersons(mit, tim, t)
}

func checkTwoPersons(a, b Person, t *testing.T) {
	bothSame := a.Age == b.Age && a.Name == b.Name && len(a.Parents) == len(b.Parents)
	if !bothSame {
		t.Logf("Conversion failed. Original object %v and converted object %v", a, b)
		t.Fail()
	}
}
