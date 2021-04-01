module github.com/meteocima/wrfda-runner

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/meteocima/namelist-prepare v1.0.0
	github.com/meteocima/virtual-server v1.4.0
	github.com/parro-it/fileargs v0.0.0-20210326145448-216ff04f5f70
	github.com/stretchr/testify v1.7.0
)

replace github.com/meteocima/virtual-server => ../virtual-server
