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
	LockErr     string
	ConsumerErr string
	Conn        struct {
		*nats.Conn
		jetstream.JetStream
		streams      map[string]jetstream.Stream
		streamsRwMut sync.RWMutex
	}
	Tagger struct {
		conn   *Conn
		kv     jetstream.KeyValue
		id     string
		queue  string
		locker *Locker
	}
	Locker struct {
		kv jetstream.KeyValue
	}
	Msg struct {
		*nats.Msg
		Deadline int64
		Id       string
	}
)

const (
	LOCK_IN_USE     = LockErr("lock is already in use")
	LOCK_UNATTENDED = LockErr("lock is unattended")

	CONSUMER_INVALID = ConsumerErr("consumer is not valid")
)

func (lockErr LockErr) Error() string {
	return string(lockErr)
}

func (consumerErr ConsumerErr) Error() string {
	return string(consumerErr)
}

func NewLocker(conn *Conn, queue string) (*Locker, error) {
	cfg := jetstream.KeyValueConfig{
		Bucket: fmt.Sprintf("%sLOCKS", strings.ToUpper(queue)),
	}
	kv, err := conn.CreateOrUpdateKeyValue(context.TODO(), cfg)
	if err != nil {
		return nil, err
	}
	locker := Locker{}
	locker.kv = kv
	return &locker, nil
}

func (locker *Locker) Lock(seqNumber uint64, consumerId string, consumers map[string]bool) error {
	_, err := locker.kv.Create(context.TODO(), fmt.Sprintf("%d", seqNumber), []byte(consumerId))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			_, ok := consumers[consumerId]
			if ok {
				return LOCK_IN_USE
			}
			return LOCK_UNATTENDED
		}
		return err
	}
	return nil
}

func (locker *Locker) Unlock(seqNumber uint64) error {
	return locker.kv.Delete(context.TODO(), fmt.Sprintf("%d", seqNumber))
}

func NewTagger(conn *Conn, queue string, consumerId string) (*Tagger, error) {
	cfg := jetstream.KeyValueConfig{
		Bucket: fmt.Sprintf("%sTAGS", strings.ToUpper(queue)),
	}
	kv, err := conn.JetStream.CreateOrUpdateKeyValue(context.TODO(), cfg)
	if err != nil {
		return nil, err
	}
	locker, err := NewLocker(conn, queue)
	if err != nil {
		return nil, err
	}
	tagger := Tagger{
		conn:   conn,
		kv:     kv,
		id:     consumerId,
		queue:  queue,
		locker: locker,
	}
	return &tagger, nil
}

func (tagger *Tagger) Tag(msg jetstream.Msg) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}
	_, err = tagger.kv.Put(context.TODO(), fmt.Sprintf("%d", metadata.Sequence.Stream), []byte(tagger.id))
	return err
}

func (tagger *Tagger) UnTag(msg jetstream.Msg) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}
	return tagger.kv.Delete(context.TODO(), fmt.Sprintf("%d", metadata.Sequence.Stream))
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
		id, ok := consumer.Config.Metadata["id"]
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

	REPEAT:
		{
			err = tagger.locker.Lock(seqNumber, tagger.id, consumers)
			if err != nil {
				if errors.Is(err, LOCK_UNATTENDED) {
					tagger.locker.Unlock(seqNumber)
					goto REPEAT
				}
				continue
			}
		}

		_, ok := consumers[string(value.Value())]
		if ok {
			continue
		}
		err = stream.DeleteMsg(context.TODO(), seqNumber)
		if err != nil {
			return err
		}
		msg, err := stream.GetMsg(context.TODO(), seqNumber)
		if err != nil {
			return nil
		}
		newMsg, err := WrapRawStreamingMessage(msg)
		if err != nil {
			return err
		}
		err = tagger.conn.PublishMsgSch(newMsg)
		if err != nil {
			return err
		}
		err = tagger.locker.Unlock(seqNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *Conn) QueueSubscribeSch(subject string, queue string, cb func(*Msg)) (jetstream.Consumer, error) {
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
			"id": id,
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
	consumer.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		tagger.Tag(msg)
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
			tagger.UnTag(msg)
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

func WrapRawStreamingMessage(rawStreamingMsg *jetstream.RawStreamMsg) (*Msg, error) {
	msg := NewMsg()
	msg.Subject = rawStreamingMsg.Subject
	msg.Header = rawStreamingMsg.Header
	msg.Data = rawStreamingMsg.Data
	deadline := rawStreamingMsg.Header.Get("deadline")
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

func main() {
	conn, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		panic(err)
	}
	schConn, err := New(conn)
	if err != nil {
		panic(err)
	}
	_, err = schConn.QueueSubscribeSch("test", "test", func(m *Msg) {
		fmt.Println(string(m.Data), 1)
	})
	if err != nil {
		panic(err)
	}
	_, err = schConn.QueueSubscribeSch("test", "test", func(m *Msg) {
		fmt.Println(string(m.Data), 2)
	})
	if err != nil {
		panic(err)
	}
	err = schConn.PublishSch("test", time.Now().Add(time.Second*1), []byte("OKK"))
	if err != nil {
		panic(err)
	}
	fmt.Scanln()
}
