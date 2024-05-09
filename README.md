# Introduction
This library is built on top of NATS JetStream and provides a way to publish delayed messages with self-recovery features. If a consumer crashes while processing a message, the message will be redelivered to another consumer, ensuring that no messages are lost.

Prerequisites

1. Go programming language installed
2. NATS server running

# Getting Started
Import the library in your Go code:

    import "github.com/vedadiyan/natsch"

Connect to the NATS server and create a new Conn instance:

    nc, err := nats.Connect("nats://localhost:4222")
    if err != nil {
        // Handle error
    }

    conn, err := natsch.New(nc)
    if err != nil {
        // Handle error
    }

# Publishing Delayed Messages
To publish a delayed message, use the PublishSch method:

    subject := "my-subject"
    deadline := time.Now().Add(time.Minute) // Delay of 1 minute
    data := []byte("Hello, World!")

    err := conn.PublishSch(subject, deadline, data)
    if err != nil {
        // Handle error
    }

The PublishSch method takes three arguments:

1. subject: The NATS subject to publish the message to.
2. deadline: The time at which the message should be delivered.
3. data: The message payload.

# Consuming Delayed Messages
To consume delayed messages, use the QueueSubscribeSch method:

    subject := "my-subject"
    queue := "my-queue"

    cb := func(msg *natsch.Msg) {
        // Process the message
        fmt.Printf("Received message: %s\n", msg.Data)
    }

    consumerCtx, err := conn.QueueSubscribeSch(subject, queue, cb)
    if err != nil {
        // Handle error
    }

    // Keep the consumer running
    select {}

The QueueSubscribeSch method takes three arguments:

1. subject: The NATS subject to consume messages from.
2. queue: The queue group name.
3. cb: A callback function that will be called when a message is received.

The method returns a ConsumerContext instance, which can be used to stop or drain the consumer.

# Self-Recovery
If a consumer crashes while processing a message, the message will be redelivered to another consumer in the same queue group. The library automatically handles message tagging and syncing to ensure that no messages are lost.
