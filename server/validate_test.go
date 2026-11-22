package server

import (
	"errors"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newValidatingServer(t *testing.T) *Server {
	t.Helper()
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	return &Server{validator: v, log: quietLogger()}
}

// ruleMessage builds a dynamic message with `string name = 1
// [(buf.validate.field).string.min_len = 3]`.
func ruleMessage(t *testing.T, name string) proto.Message {
	t.Helper()
	fo := &descriptorpb.FieldOptions{}
	proto.SetExtension(fo, validate.E_Field, validate.FieldRules_builder{
		String: validate.StringRules_builder{MinLen: proto.Uint64(3)}.Build(),
	}.Build())
	fd := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("kitchen_test.proto"),
		Package:    proto.String("kitchentest"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"buf/validate/validate.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Req"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("name"), Number: proto.Int32(1), JsonName: proto.String("name"),
				Type:    descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:   descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Options: fo,
			}},
		}},
	}
	f, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	m := dynamicpb.NewMessage(f.Messages().Get(0))
	m.Set(f.Messages().Get(0).Fields().Get(0), protoreflectString(name))
	return m
}

func TestProtovalidate(t *testing.T) {
	s := newValidatingServer(t)
	if err := s.validate(ruleMessage(t, "alice")); err != nil {
		t.Fatalf("valid message: %v", err)
	}
	err := s.validate(ruleMessage(t, "al"))
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("code = %v (%v)", st.Code(), err)
	}
	br := badRequest(t, st)
	if len(br.GetFieldViolations()) != 1 || br.GetFieldViolations()[0].GetField() != "name" {
		t.Fatalf("violations = %v", br)
	}
}

// pgvReq mimics protoc-gen-validate generated code.
type pgvReq struct {
	*healthpb.HealthCheckRequest
	err error
}

func (p pgvReq) ValidateAll() error { return p.err }

type pgvField struct{ field, reason string }

func (e pgvField) Error() string  { return e.field + ": " + e.reason }
func (e pgvField) Field() string  { return e.field }
func (e pgvField) Reason() string { return e.reason }

type pgvMulti []error

func (m pgvMulti) Error() string      { return errors.Join(m...).Error() }
func (m pgvMulti) AllErrors() []error { return m }

func TestPGVFallback(t *testing.T) {
	s := newValidatingServer(t)
	if err := s.validate(pgvReq{HealthCheckRequest: &healthpb.HealthCheckRequest{}}); err != nil {
		t.Fatal(err)
	}
	err := s.validate(pgvReq{
		HealthCheckRequest: &healthpb.HealthCheckRequest{},
		err:                pgvMulti{pgvField{"service", "too short"}, pgvField{"name", "required"}},
	})
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument || len(badRequest(t, st).GetFieldViolations()) != 2 {
		t.Fatalf("got %v", err)
	}
	if err := s.validate("not a proto"); err != nil {
		t.Fatal(err)
	}
}

func badRequest(t *testing.T, st *status.Status) *errdetails.BadRequest {
	t.Helper()
	for _, d := range st.Details() {
		if br, ok := d.(*errdetails.BadRequest); ok {
			return br
		}
	}
	t.Fatalf("no BadRequest detail in %v", st)
	return nil
}

func protoreflectString(s string) protoreflect.Value { return protoreflect.ValueOfString(s) }
