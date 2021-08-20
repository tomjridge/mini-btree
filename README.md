# A minimal implementation of a B-tree

To run some examples: `make run_test`

Sample test output [here](https://gist.github.com/tomjridge/e083a0e781189df4c4428c2da2cdc108)

The interface for a B-tree with ints for keys and ints for values looks as:


```ocaml
module Btree_on_file : sig
  type t
  val create      : fn:string -> t m
  val open_       : fn:string -> t m
  val find        : t -> int -> int option m
  val insert      : t -> int -> int -> unit m
  val insert_many : t -> (int * int) list -> (int * int) list m
  val delete      : t -> int -> unit m
  val close       : t -> unit m
end 
```

(see `example.ml`)
