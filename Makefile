REBAR ?= rebar3

.PHONY: node1 node2 node3 node

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

clean-data:
	rm -rf /tmp/bondy_mst/

clean-logs:
	rm -rf _build/test/logs/

docs:
	${REBAR} ex_doc skip_deps=true