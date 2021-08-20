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
    trace "f1";
    let rec find_r ~sofar ~r =
      trace "f2";
      store.read r >>= fun n -> 
      trace "f3";
      find_n ~sofar ~r ~n
    and find_n ~sofar ~r ~n =
      trace "f4";
      node.cases n
        ~leaf:(fun l -> return (sofar,r,l))
        ~branch:(fun b -> 
            let (k1,r1,k2) = branch.find k0 b in
            find_r ~sofar:( (r,b,k1,r1,k2)::sofar ) ~r:r1)
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
    trace "in1";
    find_leaf ~r ~k >>= fun (sofar,r,l) -> 
    trace "in1.5";
    leaf.insert k v l;
    trace "in2";
    match leaf_is_large l with
    | false -> 
      trace "in3";
      store.write r (node.of_leaf l) >>= fun () -> 
      return (`Ok [])
    | true -> 
      trace "in4";
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

  let insert ~k ~v ~r = 
    trace "i1";
    insert ~rebuild ~k ~v ~r >>= fun r -> 
    trace "i2"; return r
   

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
    insert=(fun k v l -> 
        trace "li 1";
        l:=Map_k.add k v !l;
        trace "li 2"
      );
    remove=(fun k l -> l:=Map_k.remove k !l);
    leaf_nkeys=(fun l -> Map_k.cardinal !l);
    split_leaf=(fun i l -> 
        trace "sl1";
        to_kvs l |> fun kvs -> 
        Base.List.split_n kvs i |> fun (xs,ys) -> 
        trace "sl2";
        match ys with 
        | [] -> failwith "split_leaf: ys is []"
        | (k,_)::_ -> (of_kvs xs),k,(of_kvs ys));
    to_kvs;
    of_kvs;
  }


  (** Branch implementation *)

  (** A branch is implemented as a map from k option to r; None is
     always in the map; None < Some k; we use Base.Map because it has
     a slightly more extensive API which suits our purpose here *)

  


  module K' = struct 
    type t = k option 
    let compare = Base.Option.compare k_cmp.k_cmp 
    (* ASSUMES Base.Option.compare places None < Some k *)

    let sexp_of_t: t -> Base.Sexp.t = fun _ -> Base.Sexp.Atom __LOC__
    (** ASSUMES this function is never called in our usecases; FIXME
       it is called; how? *)
  end

  module C = struct
    type t = K'.t
    include Base.Comparator.Make(K')
  end

  let comparator : _ Base.Map.comparator = (module C)

  module Map = Base.Map

  type branch = (k option,r,C.comparator_witness) Map.t ref (* also mutable *)

  (* this primed version works with k options *)
  let to_krs' b : k option list * r list = 
    b |> Map.to_alist |> function
    | [] -> failwith "to_krs: branch must contain a mapping for None"
    | (k,r)::krs -> 
      assert(k=None);
      List.split krs |> fun (ks,rs) -> 
      (ks,r::rs)

  let to_krs b : k list * r list = 
    to_krs' b |> fun (ks,rs) -> 
    List.map dest_Some ks,rs

  let _ = to_krs

  let of_krs' krs = 
    let (ks,rs) = krs in
    assert(List.length ks +1 = List.length rs);
    List.combine (None::ks) rs |> fun krs -> 
    krs |> Map.of_alist_exn comparator

  let of_krs (ks,rs) = of_krs' (List.map (fun x -> Some x) ks,rs)

  let find k b : k option * r * k option = 
    (* find the greatest key <= k *)
    Map.closest_key b `Less_or_equal_to (Some k) |> function
    | None -> 
      assert(Map.mem b None);
      failwith "find: impossible"
    | Some (k1,r) -> 
      Map.closest_key b `Greater_than (Some k) |> function
      | None -> (k1,r,None)
      | Some(k2,_) -> (k1,r,k2)

  (* NOTE A bit inefficient *)
  let split_branch i b : _ Map.t * k * _ Map.t = 
    b |> Map.to_alist |> fun krs -> 
    Base.List.split_n krs i |> fun (xs,ys) ->
    let (ks1,rs1) = match xs with
      | (None,r)::rest -> 
        let ks,rs = List.split rest in
        (ks,r::rs)
      | _ -> failwith "split_branch: xs"
    in
    match ys with
    | (Some k,r)::krs -> 
      List.split krs |> fun (ks2,rs2) -> 
      of_krs' (ks1,rs1),k,(of_krs' (ks2,r::rs2))
    | _ -> failwith "split_branch:ys"

  let replace (s1:(k,r) segment) (s2:(k,r)segment) b = 
    let (k,_r,krs,_) = s1 in
    let (k',r',krs',_) = s2 in
    assert(K'.compare k k' = 0);
    (* also, k maps to r etc *)
    (* remove old *)
    (krs,b) |> iter_k (fun ~k:kont (krs,b) -> 
        match krs with
        | [] -> b
        | (k,_r)::krs -> 
          kont (krs,Map.remove b (Some k)))
    |> fun b -> 
    (* add new *)
    let new_ = (k',r')::(List.map (fun (k,r) -> (Some k,r)) krs') in
    (new_,b) |> iter_k (fun ~k:kont (krs,b) -> 
        match krs with
        | [] -> b
        | (k,r)::krs -> 
          kont (krs,Map.set b ~key:k ~data:r))
    |> fun b -> 
    b
  
  let branch : _ branch_ops = {
    find=(fun k b -> find k !b);
    branch_nkeys=(fun b -> Map.length !b);
    split_branch=(fun i b -> split_branch i !b |> fun (a,b,c) -> (ref a,b,ref c));
    make_small_root=(fun (r1,k,r2) -> of_krs ([k],[r1;r2]) |> ref);
    to_krs=(fun b -> to_krs !b);
    of_krs=(fun krs -> of_krs krs |> ref);
    replace=(fun s1 s2 b -> 
        replace s1 s2 !b |> fun b' -> b:=b');    
  }

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



(** Combine Make_1 with Make_leaf_branch *)

module Make_2(S:S_kvr) 
  :
    sig
      open S

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
        alloc : (unit -> r m) -> 
        (k, v, r) btree_ops
    end
= 
(* ignore following - the above sig is what matters *)
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

  let make ~store ~alloc =
    let module A = struct let store=store let alloc=alloc end in
    let module B = Make(A) in
    B.btree_ops

end


(** Example with k,v=int,int ... with blk_sz coded as 4096, and
   assumed 10 bytes per int to marshall *)
module Example_int_int = struct

  let blk_sz = 4096

  module S (* : S_kvr *) = struct
    open Bin_prot.Std
    type k = int
    type v = int
    type r = int[@@deriving bin_io]
    let k_cmp = {k_cmp=Int.compare}

    let bytes_per_int = 10 (* over estimate *)

    (* NOTE subtract bytes_per_int for the tag *)
    let constants = { 
      max_leaf_keys   = (blk_sz - bytes_per_int)/(2*bytes_per_int); 
      max_branch_keys = (blk_sz - bytes_per_int)/(2*bytes_per_int) }
  end
  
  include S

  include Make_2(S)

  let make : store:(r, node) store_ops -> alloc:(unit -> r m) -> (k, v, r) btree_ops = make  
end
