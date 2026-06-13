REBAR ?= rebar3
WAL_FUZZ_BUDGET ?= 60

## Jepsen / docker knobs. The base image must have a non-musl libc
## because the produced release links against the host ERTS at build
## time, and the runtime nodes also run debian:bookworm.
JEPSEN_BUILD_IMAGE   ?= erlang:27
JEPSEN_RELEASE_NAME  ?= bondy_mst_jepsen_release
JEPSEN_RELEASE_VSN   ?= 0.4.0
## The release lives under the sibling project at `jepsen/bondy_mst_jepsen/`.
## Inside Docker, `REBAR_BASE_DIR` overrides this — see `rel-jepsen`.
JEPSEN_RELEASE_TGZ   ?= jepsen/bondy_mst_jepsen/_build/default/rel/$(JEPSEN_RELEASE_NAME)/$(JEPSEN_RELEASE_NAME)-$(JEPSEN_RELEASE_VSN).tar.gz

.PHONY: node1 node2 node3 node fuzz-wal fuzz-wal-1h fuzz-wal-24h \
        rel-jepsen rel-jepsen-local jepsen-up jepsen-down jepsen-provision

all: compile

clean: clean-data
	rm -rf _build
	$(REBAR) clean

test: eunit cover ct proper

ct: clean-data
	${REBAR} as test ct

eunit:
	${REBAR} as test eunit

proper:
	${REBAR} as test proper

cover:
	${REBAR} cover

fuzz-wal:
	${REBAR} as test compile
	./scripts/wal_fuzz.escript $(WAL_FUZZ_BUDGET)

fuzz-wal-1h:
	${REBAR} as test compile
	./scripts/wal_fuzz.escript 3600

fuzz-wal-24h:
	${REBAR} as test compile
	./scripts/wal_fuzz.escript 86400

clean-data:
	rm -rf /tmp/bondy_mst/

clean-logs:
	rm -rf _build/test/logs/

docs:
	${REBAR} ex_doc skip_deps=true

## ----------------------------------------------------------------------------
## Jepsen
## ----------------------------------------------------------------------------

## Build a Linux release of bondy_mst_jepsen inside a one-shot Docker
## container (mirrors ra-kv-store/Makefile:rel-jepsen). Produces a
## tarball the Jepsen control container installs onto n1/n2/n3 via
## the `db/DB` setup hook.
##
## The release lives in its own rebar3 project at
## `jepsen/bondy_mst_jepsen/`, which depends on this lib via a
## `_checkouts/bondy_mst` symlink to the repo root. We force
## `REBAR_BASE_DIR=/tmp/jepsen_build` so the in-container OTP version
## doesn't trip over host-compiled .beam files inside the sibling
## project's `_build/`.
rel-jepsen:
	docker run --rm \
	  -v "$(PWD)":/usr/src/bondy_mst \
	  -e REBAR_BASE_DIR=/tmp/jepsen_build \
	  -w /usr/src/bondy_mst/jepsen/bondy_mst_jepsen \
	  $(JEPSEN_BUILD_IMAGE) \
	  bash -c '$(REBAR) tar -n $(JEPSEN_RELEASE_NAME) && cp /tmp/jepsen_build/default/rel/$(JEPSEN_RELEASE_NAME)/$(JEPSEN_RELEASE_NAME)-$(JEPSEN_RELEASE_VSN).tar.gz /usr/src/bondy_mst/jepsen/jepsen.bondymst/'

## Same as rel-jepsen but builds locally (skip Docker). Only useful on
## a Linux dev box; macOS-built releases will not run inside the
## Debian Jepsen nodes.
rel-jepsen-local:
	cd jepsen/bondy_mst_jepsen && $(REBAR) release tar
	cp $(JEPSEN_RELEASE_TGZ) jepsen/jepsen.bondymst/

jepsen-up:
	cd jepsen/docker && \
	  test -f shared/jepsen-bot || ssh-keygen -t rsa -m pem \
	    -f shared/jepsen-bot -C jepsen-bot -N '' && \
	  docker compose up --detach && \
	  ./provision.sh

jepsen-down:
	cd jepsen/docker && docker compose down

jepsen-provision:
	cd jepsen/docker && ./provision.sh