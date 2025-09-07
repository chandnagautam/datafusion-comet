use jni::{errors::Result as JniResult, objects::{JClass, JStaticMethodID}, signature::ReturnType, JNIEnv};

#[allow(dead_code)]
pub struct JniStore<'a> {
    pub class: JClass<'a>,
    pub method_get: JStaticMethodID,
    pub method_get_ret: ReturnType
}

impl<'a> JniStore<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/comet/JniStore";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<JniStore<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(JniStore {
            method_get: env.get_static_method_id(Self::JVM_CLASS, "get", "(Ljava/lang/String;)Ljava/lang/Object")?,
            method_get_ret: ReturnType::Object,
            class,
        })
    }
}
