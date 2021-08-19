(** B-tree implementation *)

open Util
open Btree_intf


(** Most abstract implementation *)
module Make_1(S:sig
    type leaf
    type branch
    type node
    type k
    type v
    type r
    val constants : constants
    val k_cmp     : k k_cmp
    val leaf      : (k,v,leaf)leaf_ops
    val branch    : (k,r,branch)branch_ops
    val node      : (branch,leaf,node)node_ops
    val store     : (r,node)store_ops
    val alloc     : unit -> r m
  end) = struct
  open S

  let find_leaf ~r ~k:k0 = 
    let rec find_r ~sofar ~r =
      store.read r >>= fun n -> 
      find_n ~sofar ~r ~n
    and find_n ~sofar ~r ~n =
      node.cases n
        ~leaf:(fun l -> return (sofar,r,l))
        ~branch:(fun b -> 
            let (k1,r1,k2) = branch.find k0 b in
            find_r ~sofar:( (r,b,k1,r1,k2)::sofar ) ~r)
    in
    find_r ~sofar:[] ~r

  let _ : r:r -> k:k -> ((r * branch * k option * r * k option) list * r * leaf) m = find_leaf

  type stack = (r * branch * k option * r * k option) list

  (* NOTE the triple (_,r,leaf) consists of the leaf and the original
     pointer r to the leaf; the first component is a stack: for each
     branch we traverse, we have the original pointer, the branch, and
     the followed pointer with two k bounds *)

  let bounds (stk:stack) = 
    (stk,None,None) |> iter_k (fun ~k x -> match x with
        | ([],lo,hi) -> (lo,hi)
        | ( (_,_,k1,_,k2)::stk,lo,hi) -> 
          (* if we already have a bound, we stick with it, since it is
             tightest *)
          match lo,hi with
          | Some lo, Some hi -> (Some lo,Some hi)
          | None,Some hi     -> k (stk,k1,Some hi)
          | Some lo,None     -> k (stk,Some lo, k2)
          | None,None        -> k (stk,k1,k2) )

  let find ~r ~k = 
    find_leaf ~r ~k >>= fun (_,_,l) -> 
    return (leaf.lookup k l)

  let _ : r:r -> k:k -> (v option) m = find

  let {max_leaf_keys;max_branch_keys} = constants

  let leaf_is_large l = 
    leaf.leaf_nkeys l > max_leaf_keys
    
  let split_large_leaf l = 
    leaf.split_leaf max_leaf_keys l

  let branch_is_large b = 
    branch.branch_nkeys b > max_branch_keys

  let split_large_branch b =
    branch.split_branch max_branch_keys b

  let insert ~rebuild ~k ~v ~r =
    find_leaf ~r ~k >>= fun (sofar,r,l) -> 
    leaf.insert k v l;
    match leaf_is_large l with
    | false -> 
      store.write r (node.of_leaf l) >>= fun () -> 
      return (`Ok [])
    | true -> 
      split_large_leaf l |> fun (l1,k,l2) -> 
      alloc () >>= fun r1 -> 
      alloc () >>= fun r2 ->
      store.write r1 (node.of_leaf l1) >>= fun () -> 
      store.write r2 (node.of_leaf l2) >>= fun () -> 
      rebuild ~free:[r] ~sofar ~r1 ~k ~r2 


  (* FIXME shouldn't we always return with a new root? *)
  let rec rebuild :    
    free:r list ->
    sofar:(r * branch * k option * r * k option) list ->
    r1:r ->
    k:k ->
    r2:r ->
    _
    = 
    fun ~free ~sofar ~r1 ~k ~r2 -> 
    match sofar with 
    | [] -> 
      (* need a new root *)
      alloc () >>= fun r -> 
      branch.make_small_root (r1,k,r2) |> fun b -> 
      store.write r (node.of_branch b) >>= fun () -> 
      return (`New_root(free,r))
    | (r3,b,k1,r4,k2)::sofar -> 
      branch.replace (k1,r4,[],k2) (k1,r1,[(k,r2)],k2) b;
      match branch_is_large b with
      | false -> 
        (* FIXME we want to have a new node every point to the root *)
        store.write r3 (node.of_branch b) >>= fun () -> 
        return (`Ok free)
      | true -> 
        (* need to split again *)
        split_large_branch b |> fun (b1,k,b2) -> 
        alloc () >>= fun r1 -> 
        alloc () >>= fun r2 ->
        store.write r1 (node.of_branch b1) >>= fun () -> 
        store.write r2 (node.of_branch b2) >>= fun () -> 
        rebuild ~free:(r3::free) ~sofar ~r1 ~k ~r2

  let _ : free:r list ->
    sofar:(r * branch * k option * r * k option) list ->
    r1:r -> k:k -> r2:r -> ([> `New_root of r list * r | `Ok of r list ]) m 
    = rebuild

  let insert = insert ~rebuild

  let _ : k:k -> v:v -> r:r -> ([ `New_root of r list * r | `Ok of r list]) m = insert

  let k_lt,k_leq = (k_lt ~k_cmp, k_leq ~k_cmp)

  (* k1 <= k2 < k3 *)
  let ordered lo k hi =
    k_leq ~k1:lo ~k2:k && 
    k_lt ~k1:k ~k2:hi

  (** Insert multiple kv; once we find a leaf we insert as many as
     possible, rather than repeatedly traversing from the
     root. Assumes kvs ordered by k (or at least, this is needed for
     the performance advantage) *)
  let insert_many ~kvs ~r = 
    match kvs with
    | [] -> return `Unchanged
    | (k,v)::kvs -> 
      find_leaf ~r ~k >>= fun (sofar,r,l) -> 
      let (lo,hi) = bounds sofar in
      (* We insert as many as we can, subject to bounds, upto twice
         the max leaf size; when we can do no more, we either rebuild
         or return directly *)
      let remaining = 
        (k,v)::kvs |> iter_k (fun ~k:kont kvs ->
            match kvs with 
            | [] -> []
            | (k,v)::kvs -> 
              match ordered lo k hi && leaf.leaf_nkeys l < 2*max_leaf_keys with
              | true -> 
                leaf.insert k v l;
                kont kvs
              | false -> kvs)
      in
      match leaf_is_large l with
      | false -> 
        store.write r (node.of_leaf l) >>= fun () -> 
        return (`Remaining remaining)
      | true -> 
        split_large_leaf l |> fun (l1,k,l2) -> 
        alloc () >>= fun r1 -> 
        alloc () >>= fun r2 ->
        store.write r1 (node.of_leaf l1) >>= fun () -> 
        store.write r2 (node.of_leaf l2) >>= fun () -> 
        rebuild ~free:[r] ~sofar ~r1 ~k ~r2 >>= fun x -> 
        return (`Rebuilt (x,`Remaining remaining))


  let _ : 
    kvs:(k * v) list ->
    r:r ->
    (k,v,r) insert_many_return_type m= insert_many

      
      

  (** For delete, we just delete directly, with no attempt to
      rebalance; so some leaves may well be empty. The bad case for
      this hack is when we insert a huge number of kvs, which we then
      delete. In this case, the tree may be somewhat tall, so lookup
      takes longer than it might if we actually implemented delete
      properly. *)
  let delete ~k ~r =
    find_leaf ~k ~r >>= fun (_,r,l) -> 
    leaf.remove k l;
    store.write r (node.of_leaf l)

  (** The btree operations *)
  let btree_ops = { find; insert; insert_many; delete }

end



(** Make implementations of leaf, branch and node *)

module type S_kvr = sig
  type k
  type v
  type r
  val constants : constants
  val k_cmp     : k k_cmp
end


module Make_leaf_branch(S:S_kvr) 
  : sig
    module S : S_kvr
    open S
    type leaf
    type branch
    type node
    val leaf      : (k,v,leaf)leaf_ops
    val branch    : (k,r,branch)branch_ops
    val node      : (branch,leaf,node)node_ops
  end with module S = S
= 
struct
  module S = S
  open S

  (** Leaf implementation *)

  module K = struct type t = k let compare = k_cmp.k_cmp end

  module Map_k = Map.Make(K)

  type leaf = v Map_k.t ref (* leaves are mutable *)

  let of_kvs kvs = ref @@ Map_k.of_seq (List.to_seq kvs)

  let to_kvs l = !l |> Map_k.bindings

  let leaf : _ leaf_ops = {
    lookup=(fun k l -> Map_k.find_opt k !l);
    insert=(fun k v l -> l:=Map_k.add k v !l);
    remove=(fun k l -> l:=Map_k.remove k !l);
    leaf_nkeys=(fun l -> Map_k.cardinal !l);
    split_leaf=(fun i l -> 
        to_kvs l |> fun kvs -> 
        Base.List.split_n kvs i |> fun (xs,ys) -> 
        match ys with 
        | [] -> failwith "split_leaf: ys is []"
        | (k,_)::_ -> (of_kvs xs),k,(of_kvs ys));
    to_kvs;
    of_kvs;
  }


  (** Branch implementation *)

  module K' = struct 
    type t = k option 
    let compare = Base.Option.compare k_cmp.k_cmp 
  end

  module Map_k' = Map.Make(K')

  type branch = r Map_k'.t ref (* also mutable *)

  let branch : _ branch_ops = failwith "FIXME"

  (** Nodes *)

  type node = Branch of branch | Leaf of leaf

  let node : _ node_ops = 
    let cases n ~leaf ~branch = 
      match n with
      | Leaf l -> leaf l
      | Branch b -> branch b
    in
    {
      cases;
      of_leaf=(fun l -> Leaf l);
      of_branch=(fun b -> Branch b)
    }

end

(* module X = Make_leaf_branch *)

(** Combine Make_1 with Make_leaf_branch *)
module Make_2(S:S_kvr) 
  :
    sig
      open S
      type leaf
      type branch
      type node
      val leaf : (k, v, leaf) leaf_ops
      val branch : (k, r, branch) branch_ops
      val node : (branch, leaf, node) node_ops
      module Make :
        functor
          (T : sig
             val store : (r, node) store_ops
             val alloc : unit -> r m
           end)
          -> sig val btree_ops : (k, v, r) btree_ops end
    end
= 
struct
  open S
  module Leaf_branch = Make_leaf_branch(S)
  include Leaf_branch

  (** T parameters typically provided at runtime *)
  module Make
      (T:sig 
         val store     : (r,node)store_ops
         val alloc     : unit -> r m
       end) 
    : 
    sig
      val btree_ops: (k,v,r)btree_ops
    end
  = 
  struct

    module S2 = struct
      include S
      type nonrec leaf = leaf
      type nonrec branch = branch
      type nonrec node = node
      let leaf = leaf
      let branch = branch
      let node = node
      include T
    end

    include Make_1(S2)

    let btree_ops : (k,v,r) btree_ops = btree_ops

  end
end
