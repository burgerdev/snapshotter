package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/containerd/v2/plugins/snapshots/native"
)

var (
	addr       = flag.String("addr", "/tmp/snapshotter.sock", "Unix domain socket to listen on")
	root       = flag.String("root", "/tmp/snapshots", "storage directory")
	intercepts = flag.String("intercepts", "/tmp/snapshotter-intercepts.json", "intercept log file")
)

type Message struct {
	kind   string
	method string
	msg    any
}

func (m *Message) MarshalJSON() ([]byte, error) {
	protoMessage, ok := m.msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("message had unexpected type %T", m.msg)
	}
	msg, err := protojson.Marshal(protoMessage)
	if err != nil {
		return nil, fmt.Errorf("marshalling proto: %w", err)
	}

	return []byte(fmt.Sprintf(`{"method": %q, "type": %q, "message": %s}`, m.method, m.kind, string(msg))), nil
}

type LoggingInterceptor struct {
	f       *os.File
	encoder *json.Encoder
}

func New() (*LoggingInterceptor, error) {
	f, err := os.OpenFile(*intercepts, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening file %q: %w", *intercepts, err)
	}
	enc := json.NewEncoder(f)

	return &LoggingInterceptor{f: f, encoder: enc}, nil
}

func (li *LoggingInterceptor) Intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	log.Println("intercepting")
	reqMsg := &Message{kind: "request", method: info.FullMethod, msg: req}
	must(li.encoder.Encode(reqMsg))
	resp, err = handler(ctx, req)
	respMsg := &Message{kind: "response", method: info.FullMethod, msg: resp}
	must(li.encoder.Encode(respMsg))
	return resp, err
}

func (li *LoggingInterceptor) Close() {
	li.f.Close()
}

func main() {
	flag.Parse()

	interceptor, err := New()
	must(err)

	rpc := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptor.Intercept))

	snappy, err := native.NewSnapshotter(*root)
	must(err)

	service := snapshotservice.FromSnapshotter(snappy)
	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	l, err := net.Listen("unix", *addr)
	must(err)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go rpc.Serve(l)

	<-sigs
	rpc.Stop()
	os.Remove(*addr)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
