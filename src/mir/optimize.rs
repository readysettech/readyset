use mir::MirQuery;

pub fn optimize(mut q: MirQuery) -> MirQuery {
    //remove_extraneous_projections(&mut q);
    q
}

fn remove_extraneous_projections(q: &mut MirQuery) {
    unimplemented!()
}
