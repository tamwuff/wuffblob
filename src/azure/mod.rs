#[derive(Debug)]
pub struct AzureClient {
    pub container_client: azure_storage_blobs::prelude::ContainerClient,
    pub data_lake_client: azure_storage_datalake::clients::FileSystemClient,
}

impl AzureClient {
    pub fn new(storage_account: &str, access_key: &str, container: &str) -> AzureClient {
        AzureClient {
            container_client: azure_storage_blobs::prelude::ClientBuilder::new(
                storage_account,
                azure_storage::StorageCredentials::access_key(
                    storage_account,
                    access_key.to_string(),
                ),
            )
            .container_client(container),
            data_lake_client: azure_storage_datalake::clients::DataLakeClientBuilder::new(
                storage_account,
                azure_storage::StorageCredentials::access_key(
                    storage_account,
                    access_key.to_string(),
                ),
            )
            .build()
            .file_system_client(container),
        }
    }

    pub async fn inventory(
        &self,
        ctx: &crate::ctx::Ctx<'_>,
    ) -> Result<crate::inventory::Inventory, crate::error::WuffBlobError> {
        let mut files: std::collections::BTreeMap<crate::wuffpath::WuffPath, crate::inventory::FileInfo> = std::collections::BTreeMap::<crate::wuffpath::WuffPath, crate::inventory::FileInfo>::new();
        let mut directories: std::collections::BTreeSet<crate::wuffpath::WuffPath> =
            std::collections::BTreeSet::<crate::wuffpath::WuffPath>::new();
        let mut implicit_directories: std::collections::BTreeSet<crate::wuffpath::WuffPath> =
            std::collections::BTreeSet::<crate::wuffpath::WuffPath>::new();
        let mut contents = self.container_client.list_blobs().into_stream();
        while let Some(possible_chunk) = futures::stream::StreamExt::next(&mut contents).await {
            let chunk: &azure_storage_blobs::container::operations::list_blobs::ListBlobsResponse =
                &possible_chunk?;
            for blob in chunk.blobs.blobs() {
                let path: crate::wuffpath::WuffPath = crate::wuffpath::WuffPath::from_osstr(
                    AsRef::<std::ffi::OsStr>::as_ref(&blob.name),
                );
                if !path.is_canonical() {
                    return Err(format!("path {} is not canonical", &blob.name).into());
                }
                path.all_parents(|p: &crate::wuffpath::WuffPath| {
                    if !implicit_directories.contains(p) {
                        implicit_directories.insert(p.clone());
                    }
                });
                let mut is_dir = false;
                if let Some(resource_type) = &blob.properties.resource_type {
                    if resource_type == "directory" {
                        is_dir = true;
                    }
                }
                if is_dir {
                    directories.insert(path);
                } else if let Some(md5) = &blob.properties.content_md5 {
                    files.insert(
                        path,
                        crate::inventory::FileInfo {
                            size: blob.properties.content_length as usize,
                            md5: *md5.as_slice(),
                        },
                    );
                } else {
                    return Err(format!("{} does not have MD5", &blob.name).into());
                }
            }
            println!("OK: {:#?}", &chunk);
        }
        let additional_empty_directories: std::collections::BTreeSet<crate::wuffpath::WuffPath> = &directories - &implicit_directories;
        Ok(crate::inventory::Inventory {
            files: files,
            implicit_directories: implicit_directories,
            additional_empty_directories: additional_empty_directories,
        })
    }
}
