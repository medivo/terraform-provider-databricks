.PHONY: dep
UNAME_S := $(shell uname -s)

build: | vendor
	@mkdir -p build
	@go build -o build/terraform-provider-databricks .

install: | build
ifeq ($(UNAME_S),Linux)
	@mkdir -p ~/.terraform.d/plugins/linux_amd64 && \
	cp build/terraform-provider-databricks ~/.terraform.d/plugins/linux_amd64/
endif
ifeq ($(UNAME_S),Darwin)
	@mkdir -p ~/.terraform.d/plugins/darwin_amd64 && \
	cp build/terraform-provider-databricks ~/.terraform.d/plugins/darwin_amd64/
endif

Gopkg.toml: dep
	@dep init &>/dev/null || true

Gopkg.lock: | Gopkg.toml

dep:
	@which dep &>/dev/null || curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

vendor: | Gopkg.lock
	@dep ensure -v

test: | vendor
	@go test -v -cover -race ./...

clean:
	@rm -rf build vendor

distclean: clean
ifeq ($(UNAME_S),Linux)
	@-rm ~/.terraform.d/plugins/linux_amd64/terraform-provider-databricks
endif
ifeq ($(UNAME_S),Darwin)
	@-rm ~/.terraform.d/plugins/darwin_amd64/terraform-provider-databricks
endif
