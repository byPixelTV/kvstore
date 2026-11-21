package clienttls

import (
	"context"

	"google.golang.org/grpc"
)

func (o Options) DialOptions(apiKey string) ([]grpc.DialOption, error) {
	transport, err := o.Credentials()
	if err != nil {
		return nil, err
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	if apiKey != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearerKey{token: apiKey, tls: o.Enabled()}))
	}
	return options, nil
}

type bearerKey struct {
	token string
	tls   bool
}

func (k bearerKey) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + k.token}, nil
}

func (k bearerKey) RequireTransportSecurity() bool { return k.tls }
