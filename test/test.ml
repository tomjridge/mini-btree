
(* let _ = print_endline "hello world!" *)
open Lwt
open Mini_btree
open Util

module Btree = Example.Btree_on_file

let fn = "test.btree"

let run = Lwt_main.run

let lim = 1_000_000

let insert_many_size = 1000

let _ =
  match Sys.argv |> Array.to_list |> List.tl with
  | ["create"] -> begin
    Printf.printf "Creating...\n";
    let f = Btree.create ~fn >>= fun t -> Btree.close t in
    run f end
  | ["insert"] -> 
    Printf.printf "Inserting %d entries...\n" lim;
    let f = 
      Btree.open_ ~fn >>= fun t -> 
      1 |> iter_k (fun ~k:kont i -> 
          match i > lim with
          | true -> return ()
          | false -> 
            trace (Printf.sprintf "inserting %d" i);
            Btree.insert t i (2*i) >>= fun () -> 
            kont (i+1)) >>= fun () -> 
      Btree.close t        
    in
    run f

  | ["insert_many"] -> 
    Printf.printf "Inserting_many %d entries...\n" lim;
    let f = 
      Btree.open_ ~fn >>= fun t -> 
      1 |> iter_k (fun ~k:kont i -> 
          match i > lim with
          | true -> return ()
          | false -> 
            trace (Printf.sprintf "inserting %d" i);
            let j = min (i+insert_many_size) lim in
            let ks = Base.List.range i j in
            let kvs = List.map (fun x -> (x,2*x)) ks in
            kvs |> iter_k (fun ~k:kont2 kvs -> 
                match kvs with
                | [] -> return ()
                | _ -> 
                  Btree.insert_many t kvs >>= fun kvs -> 
                  kont2 kvs) >>= fun () -> 
            kont j) >>= fun () -> 
      Btree.close t        
    in
    run f

  | ["delete"] -> 
    Printf.printf "Deleting some entries...\n";
    let f = 
      Btree.open_ ~fn >>= fun t -> 
      100 |> iter_k (fun ~k:kont i -> 
          match i >=200 with
          | true -> return ()
          | false -> 
            trace (Printf.sprintf "deleting %d" i);
            Btree.delete t i >>= fun () -> 
            kont (i+1)) >>= fun () -> 
      Btree.close t        
    in
    run f
  | ["list"] -> 
    Printf.printf "Listing first few entries (max 1000)...\n";
    (* list first few values *)
    let f = 
      Btree.open_ ~fn >>= fun t -> 
      (1,[]) |> iter_k (fun ~k:kont (i,xs) -> 
          match i > min lim 1000 with
          | true -> return xs
          | false -> 
            Btree.find t i >>= function
            | None -> kont (i+1,xs)
            | Some v -> kont (i+1,(i,v)::xs)) >>= fun xs -> 
      let open Sexplib.Std in
      begin         
        Sexplib.Sexp.to_string_hum 
          [%message "result of list"
              ~entries:(List.rev xs : (int*int)list) 
          ] |> print_endline
      end;      
      Btree.close t
    in
    run f
  | _ -> failwith "unrecognized command line arg"
    
