use std::sync::Arc;

use anyhow::Context;
use jni::objects::{Global, JClass, JObject, JObjectArray, JString, JValue};
use jni::{InitArgsBuilder, JNIVersion, JavaVM, jni_sig, jni_str};
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
    bridge: Arc<Global<JObject<'static>>>,
}

pub struct CobrixBatchReader {
    jvm: CobrixJvm,
    handle: i64,
}

impl CobrixJvm {
    /// Create a JVM and initialize the Scala bridge object.
    pub fn new(classpath: &str) -> anyhow::Result<Self> {
        let classpath_opt = format!("-Djava.class.path={classpath}");
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V1_8)
            .option(&classpath_opt)
            .build()
            .context("failed to build JVM args")?;

        let vm = Arc::new(JavaVM::new(jvm_args).context("failed to create JVM")?);
        let bridge = vm
            .attach_current_thread(|env| {
                let cls = env
                    .find_class(jni_str!("com/cobrixjni/CobrixBridge"))
                    .context("cannot find com.cobrixjni.CobrixBridge")?;
                let obj = env
                    .new_object(cls, jni_sig!("()V"), &[])
                    .context("cannot create CobrixBridge")?;
                env.new_global_ref(obj)
                    .context("cannot globalize CobrixBridge")
            })
            .context("failed to attach JNI thread")?;

        Ok(Self {
            vm,
            bridge: Arc::new(bridge),
        })
    }

    pub fn schema_from_copybook(&self, copybook_path: &str) -> Result<CobrixSchema> {
        self.vm.attach_current_thread(|env| {
            let path = env.new_string(copybook_path).map_err(CobrixJniError::from)?;
            let path_obj = JObject::from(path);

            let out = env
                .call_method(
                    self.bridge.as_obj(),
                    jni_str!("schemaJson"),
                    jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
                    &[JValue::Object(&path_obj)],
                )
                .and_then(|v| v.l())
                .map_err(CobrixJniError::from)?;

            let out = env.cast_local::<JString>(out).map_err(CobrixJniError::from)?;
            let json_chars = out.mutf8_chars(env).map_err(CobrixJniError::from)?;
            let json = json_chars.to_str().into_owned();

            serde_json::from_str(&json).map_err(CobrixJniError::from)
        })
    }

    pub fn open_batch_reader(
        &self,
        copybook_path: &str,
        data_path: &str,
        batch_size: i32,
    ) -> Result<CobrixBatchReader> {
        let handle = self.vm.attach_current_thread(|env| {
            let copybook = env.new_string(copybook_path).map_err(CobrixJniError::from)?;
            let data = env.new_string(data_path).map_err(CobrixJniError::from)?;
            let copybook_obj = JObject::from(copybook);
            let data_obj = JObject::from(data);

            env.call_method(
                self.bridge.as_obj(),
                jni_str!("openReader"),
                jni_sig!("(Ljava/lang/String;Ljava/lang/String;I)J"),
                &[
                    JValue::Object(&copybook_obj),
                    JValue::Object(&data_obj),
                    JValue::Int(batch_size),
                ],
            )
            .and_then(|v| v.j())
            .map_err(CobrixJniError::from)
        })?;

        Ok(CobrixBatchReader {
            jvm: self.clone(),
            handle,
        })
    }
}

impl CobrixBatchReader {
    /// Returns None when the file is fully consumed.
    pub fn next_batch(&mut self) -> Result<Option<Vec<String>>> {
        self.jvm.vm.attach_current_thread(|env| {
            let arr_obj = env
                .call_method(
                    self.jvm.bridge.as_obj(),
                    jni_str!("nextBatchJson"),
                    jni_sig!("(J)[Ljava/lang/String;"),
                    &[JValue::Long(self.handle)],
                )
                .and_then(|v| v.l())
                .map_err(CobrixJniError::from)?;

            if arr_obj.is_null() {
                return Ok(None);
            }

            let arr = env
                .cast_local::<JObjectArray<'_, JString>>(arr_obj)
                .map_err(CobrixJniError::from)?;
            let len = arr.len(env).map_err(CobrixJniError::from)?;
            let mut rows = Vec::with_capacity(len);
            for i in 0..len {
                let js = arr
                    .get_element(env, i)
                    .map_err(CobrixJniError::from)?;
                rows.push(
                    js.mutf8_chars(env)
                        .map_err(CobrixJniError::from)?
                        .to_str()
                        .into_owned(),
                );
            }
            Ok(Some(rows))
        })
    }

    pub fn close(&mut self) -> Result<()> {
        self.jvm.vm.attach_current_thread(|env| {
            env.call_method(
                self.jvm.bridge.as_obj(),
                jni_str!("closeReader"),
                jni_sig!("(J)V"),
                &[JValue::Long(self.handle)],
            )
            .map_err(CobrixJniError::from)?;
            Ok(())
        })
    }
}

impl Drop for CobrixBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[allow(dead_code)]
fn _ensure_bridge_signature(_cls: JClass) {}
