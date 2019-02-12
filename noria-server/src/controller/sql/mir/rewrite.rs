use crate::controller::sql::mir::SqlToMirConverter;
use mir::node::{MirNode, MirNodeType};
use mir::MirNodeRef;

pub(super) fn make_rewrite_nodes(
    mir_converter: &SqlToMirConverter,
    name: &str,
    prev_node: MirNodeRef,
    table: &str,
    node_count: usize,
) -> Result<Vec<MirNodeRef>, String> {
    let mut nodes = Vec::new();
    let rewrite_policies = match mir_converter
        .universe
        .rewrite_policies
        .get(&String::from(table))
    {
        Some(p) => p.clone(),
        // no policies associated with this base node
        None => return Ok(nodes),
    };

    let mut node_count = node_count;

    debug!(
        mir_converter.log,
        "Found {} rewrite policies for table {}",
        rewrite_policies.len(),
        table
    );

    let mut parent = prev_node;

    for p in rewrite_policies {
        let fields = parent.borrow().columns().to_vec();
        let should_rewrite = mir_converter.get_view(&p.rewrite_view)?;

        let rw = MirNode::new(
            &format!("{}_n{}", name, node_count),
            mir_converter.schema_version,
            fields,
            MirNodeType::Rewrite {
                value: p.value,
                column: p.column,
                key: p.key,
            },
            vec![parent.clone(), should_rewrite.clone()],
            vec![],
        );
        nodes.extend(vec![should_rewrite, rw.clone()]);
        parent = rw;
        node_count += 1;
    }

    Ok(nodes)
}
