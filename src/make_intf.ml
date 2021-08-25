open Btree_impl_intf

(** What we need for marshalling *)
module type FOR_BIN_PROT = sig
  type k[@@deriving bin_io]
  type v[@@deriving bin_io]
  type r[@@deriving bin_io]
end


(** What we need to build a B-tree on a file *)
module type S = sig
  include FOR_BIN_PROT with type r = int
  val k_cmp: k k_cmp
  val constants : constants
  val blk_sz : int
end


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

(** Type of the Make functor *)
module type MAKE = functor (S:S) -> T with type k=S.k and type v=S.v 

