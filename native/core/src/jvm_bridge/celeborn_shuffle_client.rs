use jni::{
    errors::Result as JniResult,
    objects::{JClass, JMethodID},
    signature::{Primitive, ReturnType},
    JNIEnv,
};

#[allow(dead_code)]
pub struct CelebornShuffleClient<'a> {
    pub class: JClass<'a>,
    pub method_push_data: JMethodID,
    pub method_push_data_ret: ReturnType,
    pub method_merge_data: JMethodID,
    pub method_merge_data_ret: ReturnType,
}

impl<'a> CelebornShuffleClient<'a> {
    pub const JVM_CLASS: &'static str = "org/apache/celeborn/client/ShuffleClient";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CelebornShuffleClient<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CelebornShuffleClient {
            method_push_data: env.get_method_id(Self::JVM_CLASS, "pushData", "(IIII[BIIII;)I")?,
            method_push_data_ret: ReturnType::Primitive(Primitive::Int),
            method_merge_data: env.get_method_id(Self::JVM_CLASS, "mergeData", "(IIII[BIIII)I")?,
            method_merge_data_ret: ReturnType::Primitive(Primitive::Int),
            class,
        })
    }
}
