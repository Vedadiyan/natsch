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
	ConsumerContext struct {
		jetstream.ConsumeContext
		stopped  bool
		draining bool
	}
)

const (
	ERR_CONSUMER_INVALID = ConsumerErr("consumer is not valid")

	ERR_MESSAGE_DEADLINE_NOT_FOUND = MessageErr("deadline not found")

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

func NewConsumerContext(consumerContext jetstream.ConsumeContext) *ConsumerContext {
	newConsumerContext := ConsumerContext{}
	newConsumerContext.ConsumeContext = consumerContext
	return &newConsumerContext
}

func (consumerContext *ConsumerContext) Stop() {
	consumerContext.stopped = true
	consumerContext.ConsumeContext.Stop()
}

func (consumerContext *ConsumerContext) Drain() {
	consumerContext.draining = true
	consumerContext.ConsumeContext.Drain()
}

func (consumerContext *ConsumerContext) Stopped() bool {
	return consumerContext.stopped
}

func (consumerContext *ConsumerContext) Draining() bool {
	return consumerContext.draining
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
	for _, key := range keys {
		seqNumber, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return err
		}
		value, err := tagger.kv.Get(context.TODO(), key)
		if err != nil {
			continue
		}
		_, err = tagger.conn.Request(string(value.Value()), nil, time.Second*10)
		if err == nil {
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

func (conn *Conn) QueueSubscribeSch(subject string, queue string, cb func(*Msg)) (*ConsumerContext, error) {
	stream, err := GetOrCreateStream(conn, subject)
	if err != nil {
		return nil, err
	}
	id := uuid.New().String()
	tagger, err := NewTagger(conn, queue, id)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		Name:    queue,
		AckWait: time.Second * 10,
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
	conn.Subscribe(id, func(msg *nats.Msg) {
		msg.Respond(nil)
	})
	consumerContext, err := consumer.Consume(func(msg jetstream.Msg) {
		err := msg.Ack()
		if err != nil {
			log.Println("ack:", err)
		}
		metadata, err := msg.Metadata()
		if err != nil {
			log.Println("metadata:", err)
			return
		}
		err = tagger.Tag(metadata.Sequence.Stream)
		if err != nil {
			log.Println("tag:", err)
		}
		newMsg, err := WrapJetStreamMessage(msg)
		if err != nil {
			log.Println("message wrap:", err)
			return
		}
		duration := time.Until(time.UnixMicro(newMsg.Deadline))
		go func() {
			defer guard()
			<-time.After(duration)
			cb(newMsg)
			err = stream.DeleteMsg(context.TODO(), metadata.Sequence.Stream)
			if err != nil {
				log.Println("delete key:", err)
			}
			err := tagger.UnTag(metadata.Sequence.Stream)
			if err != nil {
				log.Println("untag:", err)
			}
		}()
	})
	if err != nil {
		return nil, err
	}
	localConsumerContext := NewConsumerContext(consumerContext)
	go func() {
		for !localConsumerContext.Stopped() && !localConsumerContext.Draining() {
			err = tagger.Sync(stream)
			if err != nil {
				log.Println("sync:", err)
			}
			<-time.After(cfg.AckWait)
		}
	}()
	return localConsumerContext, nil
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
			Name:     strings.ToUpper(strings.ReplaceAll(subject, ".", "")),
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
	deadline := "0"
	deadline = rawStreamingMsg.Header.Get(HEADER_DEADLINE)
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
	deadline := "0"
	deadline = natsMsg.Headers().Get(HEADER_DEADLINE)
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
		log.Println("guarded:", r)
	}
}
