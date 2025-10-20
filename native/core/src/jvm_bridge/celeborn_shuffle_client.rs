// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    pub const JVM_CLASS: &'static str = "org/apache/celeborn/CelebornShuffleClientWrapper";

    pub fn new(env: &mut JNIEnv<'a>) -> JniResult<CelebornShuffleClient<'a>> {
        let class = env.find_class(Self::JVM_CLASS)?;

        Ok(CelebornShuffleClient {
            method_push_data: env.get_method_id(Self::JVM_CLASS, "pushData", "(IIII[BIIII)I")?,
            method_push_data_ret: ReturnType::Primitive(Primitive::Int),
            method_merge_data: env.get_method_id(Self::JVM_CLASS, "mergeData", "(IIII[BIIII)I")?,
            method_merge_data_ret: ReturnType::Primitive(Primitive::Int),
            class,
        })
    }
}
