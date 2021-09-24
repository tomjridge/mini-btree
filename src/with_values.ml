(** An int->string B-tree, with a separate values file. Then a
   string->string store, with hashing of keys. *)

open Util

module Btree = Examples.Example_ii

(** Operations are insert: (k,`Insert v), or delete: (k,`Delete) *)
type 'k op = 'k * [ `Insert of string | `Delete ]

module type S = sig
  type t
  type k
  val create : btree_fn:string -> values_fn:string -> t
  val find : t -> k -> string option
  val batch : t -> k op list -> unit
  val close : t -> unit
end

module Make_1 = struct

  type t = {
    values: Values.t;
    btree : Btree.t
  }

  let create ~btree_fn ~values_fn =
    let values = Values.create ~fn:values_fn in
    let btree = Btree.create ~fn:btree_fn in
    { values; btree }

  let find t k = 
    Btree.find t.btree k |> function
    | None -> None
    | Some v' -> Some(Values.read_value t.values ~off:v')

  (* FIXME add batch operation to B-tree *)
  let batch t (ops:'k op list) = 
    (* get inserts and deletes *)
    ([],[],ops) |> iter_k (fun ~k:kont (is,ds,ops) -> 
        match ops with 
        | [] -> (is,ds)
        | op::ops -> 
          match op with
          | (k, `Insert v) -> kont ((k,v)::is,ds,ops)
          | (k, `Delete) -> kont (is,k::ds,ops))
    |> fun (is,ds) -> 
    (* sort for key locality *)
    let is = is |> List.sort (fun (k1,_) (k2,_) -> Int.compare k1 k2) in
    (* convert string values to int *)
    let is = is |> List.map (fun (k,v_s) -> (k,Values.append_value t.values v_s)) in
    let ds = List.sort Int.compare ds in
    (* inserts *)
    is |> iter_k (fun ~k:kont kvs -> 
        match kvs with
        | [] -> ()
        | _ -> 
          Btree.insert_many t.btree kvs |> fun kvs -> 
          kont kvs);
    (* deletes *)
    ds |> List.iter (Btree.delete t.btree);
    ()

  let close t = 
    Values.close t.values;
    Btree.close t.btree;
    ()
     
end

module Make_2 : S with type k:=int = Make_1


(** Version with key hashing *)

module Make_3 = struct
  
  let hash s = XXHash.XXH64.hash s |> Int64.to_int

  type t = Make_1.t

  open Make_1

  let create = create

  let find t k = find t (hash k)

  let batch t ops = 
    batch t (List.rev_map (fun (k,v) -> (hash k,v)) ops |> List.rev) (* FIXME want to be sure that no duplicate keys *)

  let close = close
  
end

module Make_4 : S with type k:=string = Make_3

module With_key_hashing = Make_4
