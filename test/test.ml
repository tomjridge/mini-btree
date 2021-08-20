
(* let _ = print_endline "hello world!" *)
open Lwt
open Mini_btree
open Util

module Btree = Example.Btree_on_file

let fn = "test.btree"

let run = Lwt_main.run

let lim = 10_000

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
    
