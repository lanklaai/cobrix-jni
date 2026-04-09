use std::sync::Arc;

use anyhow::Context;
use jni::objects::{GlobalRef, JClass, JObject, JObjectArray, JString, JValue};
use jni::{InitArgsBuilder, JNIVersion, JavaVM};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum CobrixJniError {
    #[error("JNI error: {0}")]
    Jni(#[from] jni::errors::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Cobrix bridge error: {0}")]
    Bridge(String),
}

pub type Result<T> = std::result::Result<T, CobrixJniError>;

#[derive(Debug, Clone, Deserialize)]
pub struct CobrixSchema {
    pub layout: String,
    pub fields: Vec<String>,
    pub record_length: usize,
}

#[derive(Clone)]
pub struct CobrixJvm {
    vm: Arc<JavaVM>,
    bridge: GlobalRef,
}

pub struct CobrixBatchReader {
    jvm: CobrixJvm,
    handle: i64,
}

impl CobrixJvm {
    /// Create a JVM and initialize the Scala bridge object.
    pub fn new(classpath: &str) -> anyhow::Result<Self> {
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            .option(&format!("-Djava.class.path={classpath}"))
            .build()
            .context("failed to build JVM args")?;

        let vm = Arc::new(JavaVM::new(jvm_args).context("failed to create JVM")?);
        let mut env = vm
            .attach_current_thread()
            .context("failed to attach JNI thread")?;

        let cls = env
            .find_class("com/cobrixjni/CobrixBridge")
            .context("cannot find com.cobrixjni.CobrixBridge")?;
        let obj = env
            .new_object(cls, "()V", &[])
            .context("cannot create CobrixBridge")?;
        let bridge = env
            .new_global_ref(obj)
            .context("cannot globalize CobrixBridge")?;

        Ok(Self { vm, bridge })
    }

    pub fn schema_from_copybook(&self, copybook_path: &str) -> Result<CobrixSchema> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(CobrixJniError::from)?;
        let path = env
            .new_string(copybook_path)
            .map_err(CobrixJniError::from)?;
        let path_obj = JObject::from(path);

        let out = env
            .call_method(
                self.bridge.as_obj(),
                "schemaJson",
                "(Ljava/lang/String;)Ljava/lang/String;",
                &[JValue::Object(&path_obj)],
            )
            .and_then(|v| v.l())
            .map_err(CobrixJniError::from)?;

        let json = env
            .get_string(&JString::from(out))
            .map_err(CobrixJniError::from)?
            .to_string_lossy()
            .into_owned();

        serde_json::from_str(&json).map_err(CobrixJniError::from)
    }

    pub fn open_batch_reader(
        &self,
        copybook_path: &str,
        data_path: &str,
        batch_size: i32,
    ) -> Result<CobrixBatchReader> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(CobrixJniError::from)?;
        let copybook = env
            .new_string(copybook_path)
            .map_err(CobrixJniError::from)?;
        let data = env.new_string(data_path).map_err(CobrixJniError::from)?;
        let copybook_obj = JObject::from(copybook);
        let data_obj = JObject::from(data);

        let handle = env
            .call_method(
                self.bridge.as_obj(),
                "openReader",
                "(Ljava/lang/String;Ljava/lang/String;I)J",
                &[
                    JValue::Object(&copybook_obj),
                    JValue::Object(&data_obj),
                    JValue::Int(batch_size),
                ],
            )
            .and_then(|v| v.j())
            .map_err(CobrixJniError::from)?;

        Ok(CobrixBatchReader {
            jvm: self.clone(),
            handle,
        })
    }
}

impl CobrixBatchReader {
    /// Returns None when the file is fully consumed.
    pub fn next_batch(&mut self) -> Result<Option<Vec<String>>> {
        let mut env = self
            .jvm
            .vm
            .attach_current_thread()
            .map_err(CobrixJniError::from)?;

        let arr_obj = env
            .call_method(
                self.jvm.bridge.as_obj(),
                "nextBatchJson",
                "(J)[Ljava/lang/String;",
                &[JValue::Long(self.handle)],
            )
            .and_then(|v| v.l())
            .map_err(CobrixJniError::from)?;

        if arr_obj.is_null() {
            return Ok(None);
        }

        let arr = JObjectArray::from(arr_obj);
        let len = env.get_array_length(&arr).map_err(CobrixJniError::from)?;
        let mut rows = Vec::with_capacity(len as usize);
        for i in 0..len {
            let s = env
                .get_object_array_element(&arr, i)
                .map_err(CobrixJniError::from)?;
            let js = JString::from(s);
            rows.push(
                env.get_string(&js)
                    .map_err(CobrixJniError::from)?
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        Ok(Some(rows))
    }

    pub fn close(&mut self) -> Result<()> {
        let mut env = self
            .jvm
            .vm
            .attach_current_thread()
            .map_err(CobrixJniError::from)?;
        env.call_method(
            self.jvm.bridge.as_obj(),
            "closeReader",
            "(J)V",
            &[JValue::Long(self.handle)],
        )
        .map_err(CobrixJniError::from)?;
        Ok(())
    }
}

impl Drop for CobrixBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[allow(dead_code)]
fn _ensure_bridge_signature(_cls: JClass) {}
