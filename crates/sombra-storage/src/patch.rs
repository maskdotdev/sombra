use sombra_types::PropId;

#[derive(Clone, Debug)]
pub enum PropPatchOp<'a> {
    Set(PropId, super::types::PropValue<'a>),
    Delete(PropId),
}

#[derive(Clone, Debug)]
pub struct PropPatch<'a> {
    pub ops: Vec<PropPatchOp<'a>>,
}

impl<'a> PropPatch<'a> {
    pub fn new(ops: Vec<PropPatchOp<'a>>) -> Self {
        Self { ops }
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
