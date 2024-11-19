REBAR ?= rebar3

.PHONY: node1 node2 node3 node

all: compile

clean:
	$(REBAR) clean

test: eunit cover
	${REBAR} as test ct

eunit:
	${REBAR} as test eunit

cover:
	${REBAR} cover

