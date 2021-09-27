(** Exhaustive testing

Set up a B-tree with very small parameters; test against a reference
   map implementation.

*)

open Mini_btree
open Util
open Btree_impl_intf

module S = struct
  type k=int 
  type v=int
  type r=int
  (* let constants = {max_leaf_keys=2;max_branch_keys=2} *)
  let constants = {max_leaf_keys=1;max_branch_keys=2}
  let k_cmp = { k_cmp=Int.compare }
end

module Btree = Mini_btree.Make_with_hashtbl.Make(S)

module Oracle = Map.Make(struct type t = int let compare = Int.compare end)

let lo,hi = 1,20

let keys = Base.List.range lo hi



open Sexplib.Std
type pure_t = 
  [ `Branch of int list * pure_t list | `Leaf of (int*int) list ]
  [@@deriving sexp]


(* FIXME move to util *)
let rec export_to_pure (e:_ export_t) = 
  match e with
  | `On_disk(_,`Leaf kvs) -> `Leaf kvs
  | `On_disk(_,`Branch(ks,rs)) -> 
    `Branch(ks,List.map export_to_pure rs)

let _ = export_to_pure

let rec leaves (p:pure_t) = 
  match p with
  | `Leaf kvs -> [kvs]
  | `Branch (_ks,es) -> 
    List.map leaves es |> List.concat


(*
type ('k,'v) pure_t = 
  [ `Branch of 'k list * ('k,'v) pure_t list | `Leaf of ('k*'v) list ]
*)


let rec ordered compare xs = 
  match xs with
  | [] | [_] -> true
  | x::y::xs -> 
    compare x y < 0 && ordered compare (y::xs)

(** Wellformedness check *)
let wf_pure (p:pure_t) = 
  let rec wf lo p hi = 
    match p with
    | `Branch (ks,ps) -> 
      let bs = [
        List.length ks > 0;
        (* within bounds *)
        (if List.length ks > 0 then lo <= List.hd ks else false);
        (if List.length ks > 0 then Base.List.last_exn ks < hi else false);
        (* ks are ordered *)
        ordered Int.compare ks;
        (* ks and ps lengths agree *)
        List.length ks+1 = List.length ps;
        
        (* wf holds for subtrees *)
        let is = Base.List.range 0 (List.length ks) in        
        is |> List.for_all (fun i -> 
            let lo = if i=0 then lo else List.nth ks (i-1) in
            let hi = if i=List.length ks -1 then hi else List.nth ks i in
            wf lo (List.nth ps i) hi) ]
      in
      List.for_all (fun x -> x) bs
    | `Leaf kvs -> (
        match kvs = [] with
        | true -> true
        | false -> 
          let ks = List.map fst kvs in
          let bs = [
            (* within bounds *)
            lo <= List.hd ks;
            Base.List.last_exn ks < hi;
            (* ks are ordered *)
            ordered Int.compare ks]
          in
          List.for_all (fun x -> x) bs)
  in
  wf Int.min_int p Int.max_int


let oracle p = 
  leaves p |> fun lfs -> 
  (Oracle.empty,lfs) |> iter_k (fun ~k:kont (m,lfs) -> 
      match lfs with 
      | [] -> m
      | lf::lfs -> 
        (* FIXME may also want to check that each leaf has no
           duplicates, is separated by keys above etc; perhaps use a
           wf function on the export *)
        assert(List.map fst lf |> List.for_all (fun k -> not (Oracle.mem k m)));
        kont (Oracle.add_seq (List.to_seq lf) m, lfs))
    
(* For a given state, we want to apply all the possible operations:
   (k,`Insert v) and (k,`Delete) *)

let assert_wf_pure saved_pure p = 
  assert(wf_pure p || (
      begin         
        Sexplib.Sexp.to_string_hum 
          [%message "check_pure failure"
              ~pure:(p : pure_t)
              ~prev:(saved_pure : pure_t)
          ] |> print_endline
      end;      
      false));
  ()

(* n is the fuel parameter; o agrees with t *)
let rec depth (t:Btree.t) (o:_ Oracle.t) n =
  match n>0 with
  | false -> ()
  | true -> 
    (* save state of t *)
    let saved = Btree.export t in
    let saved_pure = saved |> export_to_pure in
    let per_op = function
      | (k,`Insert v) -> 
        let t = Btree.import saved in
        Btree.insert t k v;
        let o' = Oracle.add k v o in
        let tp = Btree.export t |> export_to_pure in
        assert_wf_pure saved_pure tp;
        let o'' = oracle tp in
        assert(Oracle.compare Int.compare o' o'' = 0);
        depth t o' (n-1)
      | (k,`Delete) -> 
        let t = Btree.import saved in
        let () = Btree.delete t k in
        let o' = Oracle.remove k o in
        let tp' = Btree.export t |> export_to_pure in
        assert_wf_pure saved_pure tp';
        let o'' = oracle tp' in
        assert(Oracle.compare Int.compare o' o'' = 0);
        depth t o' (n-1)
    in
    let ops = 
      (keys |> List.map (fun k -> (k,`Insert k))) @
      (keys |> List.map (fun k -> (k,`Delete)))
    in
    ops |> List.iter per_op

let go () = 
  let t = Btree.create () in
  let o = Oracle.empty in
  depth t o 5
    
let main = go ()    
  


