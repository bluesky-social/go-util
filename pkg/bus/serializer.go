// Package bus implements common utilities for working with the Bus
package bus

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"google.golang.org/protobuf/proto"
)

// NewSerializer creates a single-type Serializer for type M.
func NewSerializer[M proto.Message]() serde.Serializer {
	return newSingleTypeProtobufSerializer[M]()
}

// NewDeserializer creates a single-type Deserializer for type M.
func NewDeserializer[M proto.Message]() serde.Deserializer {
	return newSingleTypeProtobufDeserializer[M]()
}

func newSingleTypeProtobufSerializer[M proto.Message]() serde.Serializer {
	return singleTypeProtobufSerializer[M]{}
}

func newSingleTypeProtobufDeserializer[M proto.Message]() serde.Deserializer {
	return singleTypeProtobufDeserializer[M]{}
}

type singleTypeProtobufSerializer[M proto.Message] struct{}

func (singleTypeProtobufSerializer[M]) ConfigureSerializer(
	schemaregistry.Client,
	serde.Type,
	*serde.SerializerConfig,
) error {
	return nil
}

func (singleTypeProtobufSerializer[M]) Serialize(_ string, value any) ([]byte, error) {
	message, ok := value.(M)
	if !ok {
		return nil, fmt.Errorf("unknown message type: %T", value)
	}
	return proto.Marshal(message)
}

func (singleTypeProtobufSerializer[M]) Close() error {
	return nil
}

type singleTypeProtobufDeserializer[M proto.Message] struct{}

func (singleTypeProtobufDeserializer[M]) ConfigureDeserializer(
	schemaregistry.Client,
	serde.Type,
	*serde.DeserializerConfig,
) error {
	return nil
}

func (singleTypeProtobufDeserializer[M]) Deserialize(_ string, payload []byte) (any, error) {
	var message M
	var ok bool
	message, ok = message.ProtoReflect().Type().New().Interface().(M)
	if !ok {
		return nil, fmt.Errorf("did not get message type %T from ProtoReflect", message)
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, err
	}
	return message, nil
}

func (singleTypeProtobufDeserializer[M]) DeserializeInto(_ string, payload []byte, value any) error {
	message, ok := value.(M)
	if !ok {
		return fmt.Errorf("unknown message type: %T", value)
	}
	return proto.Unmarshal(payload, message)
}

func (singleTypeProtobufDeserializer[M]) Close() error {
	return nil
}
