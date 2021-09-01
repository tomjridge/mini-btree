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
  val flush     : cache -> (k * v) list
end

module Make(S:sig
    type k
    type v
  end) 
: T with type k = S.k and type v = S.v
= struct
  (* open S *)

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

  (* mark entries as clean, and return the dirties *)
  let flush c =
    to_seq c |> fun seq -> 
    ([],seq) |> iter_k (fun ~k:kont (xs,seq) -> 
        seq () |> function
        | Seq.Nil -> xs
        | Cons ( (k,v'),seq) -> 
          match v'.dirty with
          | false -> kont (xs,seq)
          | true -> 
            v'.dirty <- false;
            kont ( (k,v'.v)::xs, seq))
end


(** Cache for find, insert, delete operations *)
module Make_map_cache(S:sig type k type v end) = struct
  
  include Make(struct type k=S.k type v = [`Present of S.v | `Absent ] end) 
  (* absent from a delete, or just not there; this explicitly records
     when a key is not present in the lower map *)
  
  let find_opt c k = find_opt c k 

  let insert c k v = add c k (`Present v)

  let insert_many c kvs = kvs |> List.iter (fun (k,v) -> insert c k v)

  let delete c k = add c k `Absent

  (* if we go to the lower map and find a key is not present, we
     record that here *)
  let note_absent c k = add' c k {dirty=false;v=`Absent}
       
end
