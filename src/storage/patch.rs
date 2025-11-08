use crate::types::PropId;

/// Operations for patching properties on graph elements.
#[derive(Clone, Debug)]
pub enum PropPatchOp<'a> {
    /// Set a property to a specific value.
    Set(PropId, super::types::PropValue<'a>),
    /// Delete a property.
    Delete(PropId),
}

/// A batch of property patch operations.
#[derive(Clone, Debug)]
pub struct PropPatch<'a> {
    /// The list of patch operations to apply.
    pub ops: Vec<PropPatchOp<'a>>,
}

impl<'a> PropPatch<'a> {
    /// Creates a new property patch from a vector of operations.
    pub fn new(ops: Vec<PropPatchOp<'a>>) -> Self {
        Self { ops }
    }

    /// Returns true if this patch contains no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
