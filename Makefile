TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

run:
	time $(DUNE) exec main

# for auto-completion of Makefile target
clean::
