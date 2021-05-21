module github.com/meteocima/wrfda-runner/v2

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/meteocima/namelist-prepare v1.1.0
	github.com/meteocima/virtual-server v1.7.0
	github.com/parro-it/fileargs v0.0.0-20210326145448-216ff04f5f70
	github.com/pkg/sftp v1.13.0 // indirect
	github.com/stretchr/testify v1.7.0
	golang.org/x/crypto v0.0.0-20210322153248-0c34fe9e7dc2 // indirect
	golang.org/x/sys v0.0.0-20210331175145-43e1dd70ce54 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/meteocima/virtual-server => ../virtual-server
replace github.com/meteocima/namelist-prepare => ../namelist-prepare
