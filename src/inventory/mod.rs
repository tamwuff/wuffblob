#[derive(Debug)]
pub struct FileInfo {
    pub size: usize,
    pub md5: [u8; 16],
}

#[derive(Debug)]
pub struct Inventory {
    pub files: std::collections::BTreeMap<crate::wuffpath::WuffPath, FileInfo>,
    pub implicit_directories: std::collections::BTreeSet<crate::wuffpath::WuffPath>,
    pub additional_empty_directories: std::collections::BTreeSet<crate::wuffpath::WuffPath>,
}
