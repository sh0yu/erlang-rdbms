# Fallback build for environments without rebar3.
ERLC ?= erlc
EBIN := ebin
SRC  := $(wildcard src/*.erl)
TEST := $(wildcard test/*.erl)
ERLC_FLAGS := +debug_info -I include

.PHONY: all clean test shell

all: $(EBIN)
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN) $(SRC)

$(EBIN):
	mkdir -p $(EBIN)

test: all
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN) $(TEST)
	@erl -noshell -pa $(EBIN) -eval \
	  'case eunit:test([{dir,"$(EBIN)"}], [verbose]) of ok -> halt(0); _ -> halt(1) end.'

shell: all
	erl -pa $(EBIN) -eval 'application:ensure_all_started(transaction_db).'

clean:
	rm -rf $(EBIN) _build
