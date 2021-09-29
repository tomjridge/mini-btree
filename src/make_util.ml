(** Utility functions *)

open Btree_impl_intf

module Add_cache(S:sig
    type r
    type node
    val uncached_store_ops: (r,node)store_ops
    val max_sz : int
  end) 
: sig 
  val read_hook : (r:S.r -> n:S.node -> unit) ref
  val write_hook : (r:S.r -> n:S.node -> unit) ref
  val cached_store_ops: (S.r,S.node)store_ops 
end
= struct
  
  open S
  
  module C = Simple_cache.Make(struct
      type k = r
      type v = node
    end)

  (* let {read;write;flush} = uncached_store_ops *)

  let lower = uncached_store_ops

  let read_hook = ref (fun ~r:r ~n:n -> ignore(r,n);())

  let write_hook = ref (fun ~r:r ~n:n -> ignore(r,n);())

  let cached_store_ops = 
    (* let max_sz = 1000 in *)
    let pc = 0.8 in (* trim to 80% of max_sz *)
    let c = C.create ~max_sz in
    let flush () = 
      C.flush c |> fun dirty -> 
      List.iter (fun (r,n) -> lower.write r n) dirty;
      lower.flush () 
    in            
    let read r = 
      C.find_opt c r |> function
      | None -> 
        lower.read r |> fun n -> 
        C.(add' c r {dirty=false;v=n});
        n
      | Some n -> 
        n
    in
    let write r n = 
      C.add c r n;
      match C.size c > max_sz with
      | false -> ()
      | true ->          
        C.trim c pc |> fun (dirties,do_remove) -> 
        (* FIXME is following async OK if a subsequent trim overlaps?
           FIXME should the following be async? *)        
        List.iter (fun (r,n) -> lower.write r n) dirties;
        do_remove();
        ()
    in            
    {read;write;flush}

end

let add_cache (type r node) ~(uncached_store_ops:(r,node)store_ops) ~max_sz = 
  let module A = Add_cache(struct 
      type nonrec r=r 
      type nonrec node=node 
      let uncached_store_ops=uncached_store_ops 
      let max_sz = max_sz
    end) 
  in
  A.cached_store_ops

let _ = add_cache
