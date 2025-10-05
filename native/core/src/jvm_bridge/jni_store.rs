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
            method_get: env.get_static_method_id(Self::JVM_CLASS, "get", "(Ljava/lang/String;)Ljava/lang/Object;")?,
            method_get_ret: ReturnType::Object,
            class,
        })
    }
}
