(** Essentially the Y combinator; useful for anonymous recursive
   functions. The k argument is the recursive callExample:

{[
  iter_k (fun ~k n -> 
      if n = 0 then 1 else n * k (n-1))

]}


 *)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x

let dest_Some = function
  | None -> failwith "dest_Some"
  | Some x -> x

(* trace execution *)
let trace _s = 
  (* print_endline s *)
  ()

let array1_of_genarray = Bigarray.array1_of_genarray

let genarray_of_array1 = Bigarray.genarray_of_array1
