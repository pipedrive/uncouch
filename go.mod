module github.com/pipedrive/uncouch

go 1.19

require (
	github.com/golang/snappy v0.0.4
	github.com/spf13/cobra v1.7.0
	go.uber.org/zap v1.24.0
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)

replace (
	golang.org/x/crypto => golang.org/x/crypto v0.4.0
	golang.org/x/net => golang.org/x/net v0.7.0
	golang.org/x/sys => golang.org/x/sys v0.5.0
	golang.org/x/text => golang.org/x/text v0.9.0
	gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.1
)
