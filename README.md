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
  val create      : fn:string -> t m
  val open_       : fn:string -> t m
  val find        : t -> k -> v option m
  val insert      : t -> k -> v -> unit m
  val insert_many : t -> (k * v) list -> (k * v) list m
  val delete      : t -> k -> unit m
  val close       : t -> unit m
end
```

(see `make_intf.ml`; the monad is just `'a Lwt.t`, renamed to `'a m`)


Docs may be found [here](tomjridge.github.io/ocamldocs/mini-btree/index.html)
