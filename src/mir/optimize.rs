use mir::MirQuery;

pub fn optimize(q: MirQuery) -> MirQuery {
    //remove_extraneous_projections(&mut q);
    q
}

// currently unused
#[allow(dead_code)]
fn remove_extraneous_projections(_q: &mut MirQuery) {
    unimplemented!()
}
