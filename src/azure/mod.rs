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
}
