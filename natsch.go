package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type (
	ConsumerErr string
	MessageErr  string
	Conn        struct {
		*nats.Conn
		jetstream.JetStream
		streams      map[string]jetstream.Stream
		streamsRwMut sync.RWMutex
	}
	Tagger struct {
		conn  *Conn
		kv    jetstream.KeyValue
		id    string
		queue string
	}
	Msg struct {
		*nats.Msg
		Deadline int64
		Id       string
	}
)

const (
	CONSUMER_INVALID = ConsumerErr("consumer is not valid")

	MESSAGE_DEADLINE_NOT_FOUND = MessageErr("deadline not found")

	HEADER_DEADLINE = "deadline"

	METADATA_ID = "id"

	SUFFIX_TAGS = "TAGS"
)

func (consumerErr ConsumerErr) Error() string {
	return string(consumerErr)
}

func (messageErr MessageErr) Error() string {
	return string(messageErr)
}

func NewTagger(conn *Conn, queue string, consumerId string) (*Tagger, error) {
	cfg := jetstream.KeyValueConfig{
		Bucket: fmt.Sprintf("%s%s", strings.ToUpper(queue), SUFFIX_TAGS),
	}
	kv, err := conn.JetStream.CreateOrUpdateKeyValue(context.TODO(), cfg)
	if err != nil {
		return nil, err
	}
	tagger := Tagger{
		conn:  conn,
		kv:    kv,
		id:    consumerId,
		queue: queue,
	}
	return &tagger, nil
}

func (tagger *Tagger) Tag(seqNumber uint64) error {
	_, err := tagger.kv.Put(context.TODO(), fmt.Sprintf("%d", seqNumber), []byte(tagger.id))
	return err
}

func (tagger *Tagger) UnTag(seqNumber uint64) error {
	return tagger.kv.Delete(context.TODO(), fmt.Sprintf("%d", seqNumber))
}

func (tagger *Tagger) Sync(stream jetstream.Stream) error {
	keys, err := tagger.kv.Keys(context.TODO())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return err
	}
	consumers := make(map[string]bool)
	consumerLister := stream.ListConsumers(context.TODO())
	for consumer := range consumerLister.Info() {
		id, ok := consumer.Config.Metadata[METADATA_ID]
		if !ok {
			return CONSUMER_INVALID
		}
		consumers[id] = true
	}
	for _, key := range keys {
		seqNumber, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return err
		}
		value, err := tagger.kv.Get(context.TODO(), key)
		if err != nil {
			return err
		}
		_, ok := consumers[string(value.Value())]
		if ok {
			continue
		}
		msg, err := stream.GetMsg(context.TODO(), seqNumber)
		if err != nil {
			continue
		}
		err = stream.DeleteMsg(context.TODO(), seqNumber)
		if err != nil {
			continue
		}
		newMsg, err := WrapRawStreamingMessage(msg)
		if err != nil {
			return err
		}
		err = tagger.conn.PublishMsgSch(newMsg)
		if err != nil {
			return err
		}
		err = tagger.UnTag(seqNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *Conn) QueueSubscribeSch(subject string, queue string, cb func(*Msg)) (jetstream.ConsumeContext, error) {
	stream, err := GetOrCreateStream(conn, subject)
	if err != nil {
		return nil, err
	}
	id := uuid.New().String()
	tagger, err := NewTagger(conn, queue, id)
	if err != nil {
		return nil, err
	}
	err = tagger.Sync(stream)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   1,
		Name:          queue,
		Metadata: map[string]string{
			METADATA_ID: id,
		},
	}
	consumer, err := stream.CreateConsumer(context.TODO(), cfg)
	if err != nil {
		if !errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, err
		}
		consumer, err = stream.Consumer(context.TODO(), queue)
		if err != nil {
			return nil, err
		}
	}
	return consumer.Consume(func(msg jetstream.Msg) {
		err := msg.Ack()
		if err != nil {
			log.Println(err)
		}
		metadata, err := msg.Metadata()
		if err != nil {
			log.Println(err)
			return
		}
		err = tagger.Tag(metadata.Sequence.Stream)
		if err != nil {
			log.Println(err)
		}
		newMsg, err := WrapJetStreamMessage(msg)
		if err != nil {
			log.Println(err)
			return
		}
		duration := time.Until(time.UnixMicro(newMsg.Deadline))
		go func() {
			defer guard()
			<-time.After(duration)
			cb(newMsg)
			err := tagger.UnTag(metadata.Sequence.Stream)
			if err != nil {
				log.Println(err)
			}
			err = stream.DeleteMsg(context.TODO(), metadata.Sequence.Stream)
			if err != nil {
				log.Println(err)
			}
		}()
	})
}

func (conn *Conn) PublishSch(subject string, deadline time.Time, data []byte) error {
	msg := NewMsg()
	msg.Subject = subject
	msg.Data = data
	msg.Deadline = deadline.UnixMicro()
	return conn.PublishMsgSch(msg)
}

func (conn *Conn) PublishMsgSch(msg *Msg) error {
	_, err := GetOrCreateStream(conn, msg.Subject)
	if err != nil {
		return err
	}
	if msg.Msg.Header == nil {
		msg.Msg.Header = nats.Header{}
	}
	msg.Msg.Header.Set(HEADER_DEADLINE, fmt.Sprintf("%d", msg.Deadline))
	_, err = conn.JetStream.PublishMsg(context.TODO(), msg.Msg)
	return err
}

func GetOrCreateStream(conn *Conn, subject string) (jetstream.Stream, error) {
	var err error
	conn.streamsRwMut.RLock()
	stream, ok := conn.streams[subject]
	conn.streamsRwMut.RUnlock()
	if !ok {
		cfg := jetstream.StreamConfig{
			Name:     strings.ToUpper(subject),
			Subjects: []string{subject},
			FirstSeq: 1,
		}
		stream, err = conn.JetStream.CreateStream(context.TODO(), cfg)
		if err != nil {
			if err != jetstream.ErrStreamNameAlreadyInUse {
				return nil, err
			}
			stream, err = conn.Stream(context.TODO(), subject)
			if err != nil {
				return nil, err
			}
		}
		conn.streamsRwMut.Lock()
		conn.streams[subject] = stream
		conn.streamsRwMut.Unlock()
	}
	return stream, nil
}

func NewMsg() *Msg {
	return WrapMessage(&nats.Msg{})
}

func WrapMessage(natsMsg *nats.Msg) *Msg {
	msg := Msg{}
	msg.Msg = natsMsg
	msg.Id = uuid.New().String()
	return &msg
}

func WrapRawStreamingMessage(rawStreamingMsg *jetstream.RawStreamMsg) (*Msg, error) {
	msg := NewMsg()
	msg.Subject = rawStreamingMsg.Subject
	msg.Header = rawStreamingMsg.Header
	msg.Data = rawStreamingMsg.Data
	deadline := rawStreamingMsg.Header.Get(HEADER_DEADLINE)
	if deadline == "" {
		return nil, MESSAGE_DEADLINE_NOT_FOUND
	}
	deadlineInt64, err := strconv.ParseInt(deadline, 10, 64)
	if err != nil {
		return nil, err
	}
	msg.Deadline = deadlineInt64
	return msg, nil
}

func WrapJetStreamMessage(natsMsg jetstream.Msg) (*Msg, error) {
	msg := NewMsg()
	msg.Subject = natsMsg.Subject()
	msg.Header = natsMsg.Headers()
	msg.Reply = natsMsg.Reply()
	msg.Data = natsMsg.Data()
	deadline := natsMsg.Headers().Get(HEADER_DEADLINE)
	if deadline == "" {
		return nil, MESSAGE_DEADLINE_NOT_FOUND
	}
	deadlineInt64, err := strconv.ParseInt(deadline, 10, 64)
	if err != nil {
		return nil, err
	}
	msg.Deadline = deadlineInt64
	return msg, nil
}

func New(conn *nats.Conn) (*Conn, error) {
	_conn := Conn{}
	_conn.Conn = conn
	_conn.streams = make(map[string]jetstream.Stream)
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}
	_conn.JetStream = js
	return &_conn, nil
}

func guard() {
	if r := recover(); r != nil {
		log.Println(r)
	}
}
