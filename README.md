# A minimal implementation of a B-tree

To run some examples: `make run_test`

Sample test output
[here](https://gist.github.com/tomjridge/e083a0e781189df4c4428c2da2cdc108)
and (with cache, 1M individual inserts in 1.5s)
[here](https://gist.github.com/tomjridge/9a244e14a3f84f9d2236b1510f76cc18)

The interface for a B-tree looks like:


```ocaml
(** Result of invoking make functor: the B-tree interface *)
module type T = sig
  type k
  type v
  type t
  val create      : fn:string -> t
  val open_       : fn:string -> t
  val find        : t -> k -> v option
  val insert      : t -> k -> v -> unit
  val insert_many : t -> (k * v) list -> (k * v) list
  val delete      : t -> k -> unit
  val close       : t -> unit
end
```

(see `src/make_intf.ml`)


Docs may be found [here](http://tomjridge.github.io/ocamldocs/mini-btree/index.html)
