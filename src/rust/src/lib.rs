use extendr_api::prelude::*;
use opendal::{BlockingOperator, Operator, Metadata};
use opendal::services::Fs;
use anyhow::Result;

fn sanitize_dir_path(path_str: &str) -> String {
    let mut p = path_str.trim().to_string();
    if !p.is_empty() && !p.ends_with('/') {
        p.push('/');
    }
    p
}

/// Represents metadata for an entry in OpenDAL.
#[derive(Debug, Clone)]
#[extendr]
pub struct OpenDALMetadata {
    meta: Metadata
}

impl From<Metadata> for OpenDALMetadata {
    fn from(meta: Metadata) -> OpenDALMetadata {
        OpenDALMetadata { meta }
    }
}

#[extendr]
impl OpenDALMetadata {
    pub fn is_file(&self) -> bool {
        self.meta.is_file()
    }

    pub fn is_dir(&self) -> bool {
        self.meta.is_dir()
    }

    pub fn cache_control(&self) -> Option<&str> {
        self.meta.cache_control()
    }

    pub fn content_length(&self) -> u64 {
        self.meta.content_length()
    }

    pub fn content_md5(&self) -> Option<&str> {
        self.meta.content_md5()
    }

    pub fn content_type(&self) -> Option<&str> {
        self.meta.content_type()
    }

    // pub fn content_range(&self) -> Option<opendal::raw::BytesContentRange> {
    //     self.meta.content_range()
    // }

    // pub fn last_modified(&self) -> Option<> {
    //     self.meta.last_modified()
    // }

    pub fn etag(&self) -> Option<&str> {
        self.meta.etag()
    }

    pub fn content_disposition(&self) -> Option<&str> {
        self.meta.content_disposition()
    }

    pub fn version(&self) -> Option<&str> {
        self.meta.version()
    }
}

#[extendr]
struct OpenDALOperator {
    op: BlockingOperator,
}

struct OpenDALOperatorInfo {}

#[extendr]
impl OpenDALOperator {
    fn info() {
        unimplemented!()
    }

    fn new_fs(root_path: String) -> Result<Self> {
        let builder = Fs::default()
          .root(&root_path);

        let operator = Operator::new(builder)?.finish().blocking();

        Ok(Self { op: operator })
    }

    // fn new_s3(
    //     bucket: String,
    //     region: Option<String>,
    //     endpoint: Option<String>,
    //     access_key_id: Option<String>,
    //     secret_access_key: Option<String>,
    //     session_token: Option<String>,
    //     enable_virtual_host_style: Option<bool>,
    //     root: Option<String>,
    // ) -> Result<Self> {
    //     let mut builder = S3::default();
    //     builder.bucket(&bucket);

    //     if let Some(r) = region {
    //         builder.region(&r);
    //     }
    //     if let Some(e) = endpoint {
    //         builder.endpoint(&e);
    //     }
    //     if let Some(ak) = access_key_id {
    //         builder.access_key_id(&ak);
    //     }
    //     if let Some(sk) = secret_access_key {
    //         builder.secret_access_key(&sk);
    //     }
    //     if let Some(st) = session_token {
    //         builder.security_token(&st);
    //     }
    //     if let Some(vhost) = enable_virtual_host_style {
    //         if vhost {
    //             builder.enable_virtual_host_style();
    //         }
    //     }
    //     if let Some(p_root) = root {
    //         builder.root(&p_root);
    //     }

    //     let operator_builder = Operator::new(builder)?;
    //     let operator = operator_builder.finish();
    //     Ok(Self { op: operator.blocking() })
    // }

    // fn new_gcs(
    //     bucket: String,
    //     credential_path: Option<String>,
    //     credential_json_content: Option<String>,
    //     endpoint: Option<String>,
    //     default_storage_class: Option<String>,
    //     predefined_acl: Option<String>,
    //     root: Option<String>,
    // ) -> Result<Self> {
    //     let mut builder = Gcs::default();
    //     builder.bucket(&bucket);

    //     if let Some(cp) = credential_path {
    //         builder.credential_path(&cp);
    //     }
    //     if let Some(cc_json) = credential_json_content {
    //         builder.credential(&cc_json);
    //     }
    //     if let Some(e) = endpoint {
    //         builder.endpoint(&e);
    //     }
    //     if let Some(dsc) = default_storage_class {
    //         builder.default_storage_class(&dsc);
    //     }
    //     if let Some(acl) = predefined_acl {
    //         builder.predefined_acl(&acl);
    //     }
    //     if let Some(p_root) = root {
    //         builder.root(&p_root);
    //     }

    //     let operator_builder = Operator::new(builder)?;
    //     let operator = operator_builder.finish();
    //     Ok(Self { op: operator.blocking() })
    // }

    // General Paths
    fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.op.exists(path)?)
    }

    /// Retrieves metadata for a path.
    fn stat(&self, path: &str) -> Result<OpenDALMetadata> {
        let meta = self.op.stat(path)?;
        Ok(OpenDALMetadata::from(meta))
    }

    // Directories
    fn create_dir(&self, path: &str) -> Result<()> {
        Ok(self.op.create_dir(path)?)
    }

    fn list(&self, path: &str) -> Result<Vec<String>> {

        let entries = self.op.list(path)?;
        Ok(entries
            .into_iter()
            .map(|entry| entry.name().to_string())
            .collect())
    }

    // Files
    fn read_raw(&self, path: &str) -> Result<Robj> {
        let content = self.op.read(path)?;
        Ok(Raw::from_bytes(&content.to_vec()).into())
    }

    fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let _ = self.op.write(path, data)?;
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<()> {
        Ok(self.op.delete(path)?)
    }

    fn copy(&self, source_path: &str, destination_path: &str) -> Result<()> {
        Ok(self.op.copy(source_path, destination_path)?)
    }

    fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        Ok(self.op.rename(old_path, new_path)?)
    }

    fn remove_all(&self, path: &str) -> Result<()> {
        Ok(self.op.remove_all(path)?)
    }
}

// Macro to generate R exports
extendr_module! {
    mod opendalr;
    impl OpenDALOperator;
    impl OpenDALMetadata;
}
