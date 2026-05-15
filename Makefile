REBAR ?= rebar3
WAL_FUZZ_BUDGET ?= 60

.PHONY: node1 node2 node3 node fuzz-wal fuzz-wal-1h fuzz-wal-24h

all: compile

clean: clean-data
	rm -rf _build
	$(REBAR) clean

test: eunit cover ct

ct: clean-data
	${REBAR} as test ct

eunit:
	${REBAR} as test eunit

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