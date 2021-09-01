TMP_DOC_DIR:=/tmp/mini-btree
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

run_test:
	dune build test/test.exe	
	rm -f ./test.exe ./test.btree
	cp _build/default/test/test.exe .
	time ./test.exe create
	time ./test.exe insert
#	time ./test.exe insert_many
	time ./test.exe list
	time ./test.exe delete
	time ./test.exe list

# for auto-completion of Makefile target
clean::
