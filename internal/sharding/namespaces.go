package sharding

import (
	"context"
	"maps"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "github.com/byPixelTV/kvstore/gen/go/kvstore/v1"
)

func (r *Router) ListNamespaces(ctx context.Context) ([]string, error) {
	if err := r.checkRouteAvailable(); err != nil {
		return nil, err
	}
	state := r.current()
	names := make(map[string]struct{})
	for i, g := range r.groups {
		leader := g.leader.Load()
		if leader < 0 || leader >= int32(len(g.conns)) || len(g.conns[leader]) == 0 {
			return nil, status.Errorf(codes.Unavailable, "shard %d Raft leader unknown", i)
		}
		result, err := api.NewConsoleClient(g.conns[leader][0]).ListNamespaces(ctx, &api.ListNamespacesRequest{})
		if err != nil {
			return nil, status.Errorf(status.Code(err), "shard %d namespace discovery: %s", i, status.Convert(err).Message())
		}
		for _, ns := range result.GetNamespaces() {
			if invalidKey(ns, "_") == nil {
				names[ns] = struct{}{}
			}
		}
	}
	if r.current().Epoch != state.Epoch {
		return nil, status.Error(codes.Aborted, "routing changed during namespace discovery; retry")
	}
	return slices.Sorted(maps.Keys(names)), nil
}
