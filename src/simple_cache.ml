(** A simple cache, using a hashtbl and a dirty flag.

Users are expected to insert elements and take care when elements are expunged.
 *)

open Util

(** What we get from creating a cache *)
module type T = sig
  type k
  type v
  type v'    = { mutable dirty : bool; v : v; }
  type cache
  val create    : max_sz:int -> cache
  val add'      : cache -> k -> v' -> unit
  val add       : cache -> k -> v -> unit
  val find_opt' : cache -> k -> v' option
  val find_opt  : cache -> k -> v option
  val remove    : cache -> k -> unit
  val size      : cache -> int
  val to_seq    : cache -> (k * v') Seq.t
  val trim      : cache -> float -> (k * v) list
end

module Make(S:sig
    type k
    type v
  end) 
: T with type k = S.k and type v = S.v
= struct
  open S

  type k = S.k
  type v = S.v

  type v' = {mutable dirty:bool; v:v}

  type tbl = (k,v') Hashtbl.t

  type cache = {
    max_sz: int;
    tbl: tbl
  }

  let create ~max_sz = 
    Hashtbl.create max_sz |> fun tbl -> 
    { max_sz; tbl }
    
  let add' c k v' = Hashtbl.replace c.tbl k v'

  (* assume new entries are dirty *)
  let add c k v = Hashtbl.replace c.tbl k {dirty=true;v}

  let find_opt' c k = Hashtbl.find_opt c.tbl k

  let find_opt c k = find_opt' c k |> Option.map (fun v' -> v'.v)

  (* might remove a dirty entry *)
  let remove c k = Hashtbl.remove c.tbl k

  let size c = Hashtbl.length c.tbl

  let to_seq c = Hashtbl.to_seq c.tbl

  (* force capacity to pc * max_sz, and return dirty entries *)
  let trim c (pc:float) = 
    let target_sz = pc *. (float_of_int c.max_sz) |> int_of_float in
    assert(target_sz >= 0 && target_sz <= c.max_sz);
    let sz = size c in
    match sz <= target_sz with 
    | true -> []
    | false -> begin
        let n = sz - target_sz in
        (* need to remove n entries *)
        Hashtbl.to_seq c.tbl |> fun seq -> 
        let to_remove = 
          (n,seq,[]) |> iter_k (fun ~k:kont (n,seq,(kvs:_ list)) -> 
              match n > 0 with
              | false -> kvs
              | true -> 
                (* get next elt and add to kvs *)
                seq () |> function 
                | Seq.Nil -> kvs (* impossible? *)
                | Cons( (k,v'),seq ) -> kont (n-1, seq, (k,v')::kvs))
        in
        (* remove, and accumulate dirty entries *)
        let dirty = 
          (to_remove,[]) |> iter_k (fun ~k:kont (to_remove,dirty) -> 
              match to_remove with
              | [] -> dirty
              | (k,v')::to_remove -> 
                Hashtbl.remove c.tbl k;
                match v'.dirty with
                | true -> kont (to_remove,(k,v'.v)::dirty)
                | false -> kont (to_remove,dirty))
        in
        dirty
      end
end
