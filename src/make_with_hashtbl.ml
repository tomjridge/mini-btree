(** In-memory implementation; no marshalling *)

open Btree_impl_intf

(* NOTE we don't use k_cmp, since we use a hashtbl for the store *)
module Make_1(S:S_kvr with type r=int) = struct
  (* open S *)

  include Btree_impl.Make(S)
  
  type t = {
    store: (r,node)Hashtbl.t;
    store_ops: (r,node)store_ops;
    min_free: int ref;
    alloc: unit -> r;
    btree_ops: (k,v,r)btree_ops;
    mutable root: r
  }

  module With_store(S:sig val store: (r,node)Hashtbl.t end) = struct
    open S
    let store_ops : (r,node) store_ops = {
      read=(fun r -> Hashtbl.find store r);
      write=(fun r n -> Hashtbl.replace store r n);
      flush=(fun () -> ());
    }
  end

  let create () = 
    let store=Hashtbl.create 1024 in
    let open With_store(struct let store=store end) in
    store_ops.write 1 (node.of_leaf (leaf.of_kvs []));
    let min_free=ref 2 in
    let alloc = fun () -> let x = !min_free in incr min_free; x in
    let btree_ops = make ~store:store_ops ~alloc in
    { store;store_ops;min_free;alloc;btree_ops;root=1 }

  let find t k = t.btree_ops.find ~r:t.root ~k

  let insert t k v = 
    t.btree_ops.insert ~k ~v ~r:t.root |> function (_free,new_root) -> 
    match new_root with 
    | None -> ()
    | Some {new_root} -> t.root<-new_root

  let delete t k = t.btree_ops.delete ~k ~r:t.root

  let batch t ops = 
    ops |> List.iter (function
        | (k,`Insert v) -> insert t k v
        | (k, `Delete) -> delete t k)

  let export t : _ export_t = 
    t.btree_ops.export t.root
    
end

module type T = sig
  type k
  type v
  type r = int
  type t
  val create : unit -> t
  val find : t -> k -> v option
  val insert : t -> k -> v -> unit
  val delete : t -> k -> unit
  val batch : t -> (k * [ `Delete | `Insert of v ]) list -> unit
end

module Make_2(S:S_kvr with type r=int) : T = Make_1(S)

module Make = Make_2
