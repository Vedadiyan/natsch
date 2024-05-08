package natsch

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
	Conn struct {
		*nats.Conn
		jetstream.JetStream
		streams      map[string]jetstream.Stream
		streamsRwMut sync.RWMutex
	}
	Msg struct {
		*nats.Msg
		Deadline int64
		Id       string
	}
)

func (conn *Conn) QueueSubscribeSch(subject string, queue string, cb func(*Msg)) (jetstream.Consumer, error) {
	stream, err := GetOrCreateStream(conn, subject)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   1,
		Name:          queue,
		AckWait:       2,
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
	consumer.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		metadata, err := msg.Metadata()
		if err != nil {
			log.Println(err)
			return
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
			stream.DeleteMsg(context.TODO(), metadata.Sequence.Stream)
		}()
	})
	return consumer, nil
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
	msg.Msg.Header.Set("deadline", fmt.Sprintf("%d", msg.Deadline))
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

func WrapJetStreamMessage(natsMsg jetstream.Msg) (*Msg, error) {
	msg := NewMsg()
	msg.Subject = natsMsg.Subject()
	msg.Header = natsMsg.Headers()
	msg.Reply = natsMsg.Reply()
	msg.Data = natsMsg.Data()
	deadline := natsMsg.Headers().Get("deadline")
	if deadline == "" {
		return nil, fmt.Errorf("deadline not found")
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
