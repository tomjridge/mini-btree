
(* let _ = print_endline "hello world!" *)
(* open Lwt *)
open Mini_btree
open Util

module Btree = Examples.Example_int_int_mmap

let fn = "test.btree"

(* let run = Lwt_main.run *)

(* let lim = 1_000_000 *)

let insert_many_size = 1000

let _ =
  match Sys.argv |> Array.to_list |> List.tl with
  | ["create"] -> begin
    Printf.printf "Creating...\n";
    Btree.create ~fn |> fun t -> Btree.close t
  end
  | ["insert";lim] -> 
    let lim = int_of_string lim in
    Printf.printf "Inserting %d entries...\n%!" lim;
    let f = 
      Btree.open_ ~fn |> fun t -> 
      1 |> iter_k (fun ~k:kont i -> 
          match i > lim with
          | true -> ()
          | false -> 
            (* trace (Printf.sprintf "inserting %d" i); *)
            Btree.insert t i (2*i) |> fun () -> 
            kont (i+1)) |> fun () -> 
      Printf.printf "before close...\n%!";
      Btree.close t |> fun () -> 
      Printf.printf "after close\n%!";
      ()
    in
    f

  | ["insert_many";lim] -> 
    let lim = int_of_string lim in
    Printf.printf "Inserting_many %d entries...\n" lim;
    let f = 
      Btree.open_ ~fn |> fun t -> 
      1 |> iter_k (fun ~k:kont i -> 
          match i > lim with
          | true -> ()
          | false -> 
            trace (Printf.sprintf "inserting %d" i);
            (* NOTE we want to include lim, so go upto lim+1 *)
            let j = min (i+insert_many_size) (lim+1) in            
            assert(j>i);
            let ks = Base.List.range i j in            
            let kvs = List.map (fun x -> (x,2*x)) ks in
            kvs |> iter_k (fun ~k:kont2 kvs -> 
                match kvs with
                | [] -> ()
                | _ -> 
                  Btree.insert_many t kvs |> fun kvs -> 
                  kont2 kvs) |> fun () -> 
            kont j) |> fun () -> 
      Btree.close t        
    in
    f

  | ["random_w";lim] -> 
    let lim = int_of_string lim in
    Printf.printf "Random writes, %d entries...\n%!" lim;
    let f = 
      Btree.open_ ~fn |> fun t -> 
      1 |> iter_k (fun ~k:kont i -> 
          match i > lim with
          | true -> ()
          | false -> 
            (* trace (Printf.sprintf "inserting %d" i); *)
            let k = Random.int 1_000_000_000 in
            Btree.insert t k (2*k) |> fun () -> 
            kont (i+1)) |> fun () -> 
      Printf.printf "before close...\n%!";
      Btree.close t |> fun () -> 
      Printf.printf "after close\n%!";
      ()
    in
    f


  | ["random_wb";n;sz] -> 
    (* random writes, in batches of size *)
    let batch_sz = int_of_string sz in
    let n = int_of_string n in
    Printf.printf "Random writes, %d batches, %d in each batch...\n%!" n batch_sz;
    Btree.open_ ~fn |> fun t -> 
    let t1 = Util.time () in
    1 |> iter_k (fun ~k:kont n' -> 
        match n' > n with
        | true -> ()
        | false -> 
          (* trace (Printf.sprintf "inserting %d" i); *)
          let xs = List.init batch_sz (fun _ -> Random.int 1_000_000_000) in
          let xs = List.sort Int.compare xs in
          let xs = List.map (fun x -> (x,2*x)) xs in
          xs |> iter_k (fun ~k:kont1 xs ->
              Btree.insert_many t xs |> fun xs -> 
              match xs with
              | [] -> ()
              | _ -> kont1 xs);
          kont (n'+1));
    let t2 = Util.time () in
    Printf.printf "Completed in %f\n%!" (t2 -. t1);
    Printf.printf "before close...\n%!";
    Btree.close t |> fun () -> 
    let t3 = Util.time () in
    Printf.printf "Completed in %f\n%!" (t3 -. t2);
    Printf.printf "after close\n%!";
    ()
    

  | ["delete"] -> 
    Printf.printf "Deleting some entries...\n";
    let f = 
      Btree.open_ ~fn |> fun t -> 
      100 |> iter_k (fun ~k:kont i -> 
          match i >=200 with
          | true -> ()
          | false -> 
            trace (Printf.sprintf "deleting %d" i);
            Btree.delete t i |> fun () -> 
            kont (i+1)) |> fun () -> 
      Btree.close t        
    in
    f
  | ["list";lim] -> 
    let lim = int_of_string lim in
    Printf.printf "Listing first few entries (max 1000)...\n";
    (* list first few values *)
    let f = 
      Btree.open_ ~fn |> fun t -> 
      (1,[]) |> iter_k (fun ~k:kont (i,xs) -> 
          match i > min lim 1000 with
          | true -> xs
          | false -> 
            Btree.find t i |> function
            | None -> kont (i+1,xs)
            | Some v -> kont (i+1,(i,v)::xs)) |> fun xs -> 
      let open Sexplib.Std in
      begin         
        Sexplib.Sexp.to_string_hum 
          [%message "result of list"
              ~entries:(List.rev xs : (int*int)list) 
          ] |> print_endline
      end;      
      Btree.close t
    in
    f
  | _ -> failwith "unrecognized command line arg"
    
