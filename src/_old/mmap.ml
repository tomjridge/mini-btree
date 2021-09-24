include Mmap_private.Make_1(struct 
    let int_size_is_geq_63 = (Sys.int_size >= 63)
    let mmap_increment_size = 4_294967296 (* 4GB *)
  end)
