module github.com/meteocima/wrfda-runner

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/meteocima/namelist-prepare v1.0.0
	github.com/meteocima/virtual-server v1.4.0
)

replace github.com/meteocima/virtual-server => ../virtual-server
