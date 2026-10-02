module github.com/udaykishore-resu/idem/idemgrpc

go 1.22.0

require (
	github.com/udaykishore-resu/idem v0.1.0
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.4
)

require (
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
)

// This module is developed and tested against the working tree. The replace
// directive is ignored by modules that depend on this one; they resolve
// github.com/udaykishore-resu/idem to the version required above.
replace github.com/udaykishore-resu/idem => ../
