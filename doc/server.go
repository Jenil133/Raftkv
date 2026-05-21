package doc

import (
	"context"
	"errors"

	"github.com/Jenil133/raftkv/proto/docpb"
)

// Server exposes a Store over gRPC. It implements docpb.DocsServer.
type Server struct {
	docpb.UnimplementedDocsServer
	store *Store
}

func NewServer(store *Store) *Server { return &Server{store: store} }

func code(err error) (docpb.Code, string) {
	switch {
	case err == nil:
		return docpb.Code_OK, ""
	case errors.Is(err, ErrNotFound):
		return docpb.Code_NOT_FOUND, err.Error()
	case errors.Is(err, ErrVersionConflict):
		return docpb.Code_VERSION_CONFLICT, err.Error()
	case errors.Is(err, ErrInvalid):
		return docpb.Code_INVALID, err.Error()
	default:
		return docpb.Code_UNAVAILABLE, err.Error()
	}
}

func toPB(d Document) *docpb.Document {
	return &docpb.Document{Collection: d.Collection, Id: d.ID, Version: d.Version, Json: d.Data}
}

func respond(d Document, err error) *docpb.Response {
	c, msg := code(err)
	r := &docpb.Response{Code: c, Error: msg}
	if err == nil && d.Version > 0 {
		r.Doc = toPB(d)
	}
	return r
}

func ifVersion(has bool, v uint64) *uint64 {
	if !has {
		return nil
	}
	return &v
}

func (s *Server) Put(ctx context.Context, r *docpb.PutRequest) (*docpb.Response, error) {
	d, err := s.store.Put(ctx, r.Collection, r.Id, r.Json, ifVersion(r.HasIfVersion, r.IfVersion))
	return respond(d, err), nil
}

func (s *Server) Get(ctx context.Context, r *docpb.GetRequest) (*docpb.Response, error) {
	d, err := s.store.Get(ctx, r.Collection, r.Id)
	return respond(d, err), nil
}

func (s *Server) Delete(ctx context.Context, r *docpb.DeleteRequest) (*docpb.Response, error) {
	err := s.store.Delete(ctx, r.Collection, r.Id, ifVersion(r.HasIfVersion, r.IfVersion))
	return respond(Document{}, err), nil
}

func (s *Server) Patch(ctx context.Context, r *docpb.PatchRequest) (*docpb.Response, error) {
	d, err := s.store.Patch(ctx, r.Collection, r.Id, r.Patch, ifVersion(r.HasIfVersion, r.IfVersion))
	return respond(d, err), nil
}

func (s *Server) Scan(ctx context.Context, r *docpb.ScanRequest) (*docpb.ScanResponse, error) {
	docs, err := s.store.Scan(ctx, r.Collection, r.AfterId, int(r.Limit))
	c, msg := code(err)
	resp := &docpb.ScanResponse{Code: c, Error: msg}
	for _, d := range docs {
		resp.Docs = append(resp.Docs, toPB(d))
	}
	return resp, nil
}
