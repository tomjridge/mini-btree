TMP_DOC_DIR:=/tmp/mini-btree
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

run_test:
	dune build test/test.exe	
	rm -f ./test.exe ./test.btree
	cp _build/default/test/test.exe .
	./test.exe create
	./test.exe insert
	./test.exe list

# for auto-completion of Makefile target
clean::
