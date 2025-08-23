use jni::{errors::Result as JniResult, objects::{JClass, JMethodID}, signature::ReturnType, JNIEnv};

pub struct JniStore<'a> {
    pub class: JClass<'a>,
    pub method_get: JMethodID,
    pub method_get_ret: ReturnType
}

impl<'a> JniStore<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/JniStore";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<JniStore<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(JniStore {
            method_get: env.get_method_id(Self::JVM_CLASS, "get", "(Ljava/lang/String;)Ljava/lang/Object")?,
            method_get_ret: ReturnType::Object,
            class,
        })
    }
}
