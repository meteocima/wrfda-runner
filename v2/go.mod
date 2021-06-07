module github.com/meteocima/wrfda-runner/v2

go 1.16

replace github.com/meteocima/virtual-server => ../../virtual-server

replace github.com/meteocima/namelist-prepare => ../../namelist-prepare

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/meteocima/namelist-prepare v1.1.0
	github.com/meteocima/virtual-server v1.8.0
	github.com/parro-it/fileargs v0.0.0-20210327105848-8399f23fc4ca
	github.com/stretchr/testify v1.7.0
)
