(** Main implementation types *)

type constants = {
  max_leaf_keys   : int;
  max_branch_keys : int;
}

(** Calculate constants given blk size and k and v size. NOTE assumes type r = int *)
let make_constants ~blk_sz ~k_sz ~v_sz = 
  let bytes_per_int = 10 in (* over estimate *)
  let r_sz = bytes_per_int in
  (* NOTE subtract bytes_per_int for the tag *)
  let constants = { 
    max_leaf_keys   = (blk_sz - bytes_per_int)/(2*(k_sz+r_sz)); 
    max_branch_keys = (blk_sz - bytes_per_int)/(2*(k_sz+v_sz)) 
  }
  in
  constants

type 'k k_cmp = {
  k_cmp: 'k -> 'k -> int
}

(** NOTE Leaves are mutable *)
type ('k,'v,'leaf) leaf_ops = {
  lookup           : 'k -> 'leaf -> 'v option;
  insert           : 'k -> 'v -> 'leaf -> unit;
  remove           : 'k -> 'leaf -> unit;
  leaf_nkeys       : 'leaf -> int;
  split_leaf       : int -> 'leaf -> 'leaf*'k*'leaf;  (* first leaf has n keys *)
  to_kvs           : 'leaf -> ('k*'v) list;
  of_kvs           : ('k*'v) list -> 'leaf;
}


module Top_or_bottom = struct
  type 'k or_top = 'k option

  type 'k or_bottom = 'k option

end
open Top_or_bottom

let k_lt ~k_cmp ~k1 ~k2 = 
  match k2 with 
  | None -> true
  | Some k2 -> k_cmp.k_cmp k1 k2 < 0

let k_leq ~k_cmp ~k1 ~k2 =
  match k1 with
  | None -> true
  | Some k1 -> k_cmp.k_cmp k1 k2 < 0

type ('k,'r) segment = 'k or_bottom * 'r * ('k*'r) list * 'k or_top

(** NOTE Nodes are mutable *)
type ('k,'r,'branch) branch_ops = {
  find             : 'k -> 'branch -> ('k or_bottom*'r*'k or_top);
  branch_nkeys     : 'branch -> int;
  split_branch     : int -> 'branch -> 'branch * 'k * 'branch; (* first branch has n keys; n>=1 *)
  make_small_root  : 'r*'k*'r -> 'branch;
  to_krs           : 'branch -> ('k list * 'r list);
  of_krs           : ('k list * 'r list) -> 'branch;
  replace          : ('k,'r) segment -> ('k,'r) segment -> 'branch -> unit;
}

type ('branch,'leaf,'node) node_ops = {
  cases: 'a. 'node -> leaf:('leaf -> 'a) -> branch:('branch -> 'a) -> 'a;
  of_leaf: 'leaf -> 'node;
  of_branch: 'branch -> 'node;
}


type ('r,'node) store_ops = {
  read : 'r -> 'node;
  write : 'r -> 'node -> unit;
  flush : unit -> unit;
}

type 'r free = { free: 'r list }[@@inline]

type 'r new_root = { new_root: 'r }[@@inline]

type ('k,'v,'r) insert_many_return_type = 
  | Rebuilt of 'r free * 'r new_root option * ('k * 'v) list
  | Remaining of ('k * 'v) list
  | Unchanged_no_kvs

type ('k,'v,'r) btree_ops = {
  find: r:'r -> k:'k -> 'v option;

  insert: k:'k -> v:'v -> r:'r -> ('r free * 'r new_root option);

  insert_many:
    kvs:('k * 'v) list ->
    r:'r ->
    ('k,'v,'r) insert_many_return_type;

  delete: k:'k -> r:'r -> unit
}


(** What we need to construct the B-tree *)
module type S_kvr = sig
  type k
  type v
  type r
  val constants : constants
  val k_cmp     : k k_cmp
end

(** What we get after constructing the B-tree *)
module type T = sig
  type k
  type v
  type r
  (* Leaf, branch implementations *)
  type leaf
  type branch
  type node
  val leaf : (k, v, leaf) leaf_ops
  val branch : (k, r, branch) branch_ops
  val node : (branch, leaf, node) node_ops

  (* Make function *)
  val make : 
    store : (r, node) store_ops -> 
    alloc : (unit -> r) -> 
    (k, v, r) btree_ops
end

(** Type of the make functor *)
module type MAKE = functor (S:S_kvr) -> T with type k=S.k and type v=S.v and type r=S.r


let blk_sz_4096 = 4096

(** Example, k=int, v=int, r=int *)
module Int_int = struct
  let blk_sz = blk_sz_4096
  type k = int
  type v = int
  type r = int
  let k_cmp = {k_cmp=Int.compare}

  (* NOTE 10 is max size of marshalled int *)
  let constants = make_constants ~blk_sz ~k_sz:10 ~v_sz:10
end

module type EXAMPLE_INT_INT = T with type k=int and type v=int and type r=int
