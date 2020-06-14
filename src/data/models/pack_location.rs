//
// Generated by using `cargo expand` on a simple example that defines the types
// to be serialized, with serde derive pragma.
//
// 1. cargo new serdex
// 2. Add serde to Cargo.toml
// 3. Copy the type definitions to main.rs
// 4. Add #[derive(Serialize, Deserialize)] to each type
// 5. cargo expand
// 6. Copy the results here, stripping away the compiler directives
//
use crate::domain::entities::PackLocation;

impl serde::Serialize for PackLocation {
    fn serialize<__S>(&self, __serializer: __S) -> serde::export::Result<__S::Ok, __S::Error>
    where
        __S: serde::Serializer,
    {
        let mut _serde_state = match serde::Serializer::serialize_struct(
            __serializer,
            "PackLocation",
            false as usize + 1 + 1 + 1,
        ) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        match serde::ser::SerializeStruct::serialize_field(&mut _serde_state, "s", &self.store) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        match serde::ser::SerializeStruct::serialize_field(&mut _serde_state, "b", &self.bucket) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        match serde::ser::SerializeStruct::serialize_field(&mut _serde_state, "o", &self.object) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        serde::ser::SerializeStruct::end(_serde_state)
    }
}

impl<'de> serde::Deserialize<'de> for PackLocation {
    fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
    where
        __D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
            __field2,
            __ignore,
        }
        struct __FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "field identifier")
            }
            fn visit_u64<__E>(self, __value: u64) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    0u64 => serde::export::Ok(__Field::__field0),
                    1u64 => serde::export::Ok(__Field::__field1),
                    2u64 => serde::export::Ok(__Field::__field2),
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"field index 0 <= i < 3",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "s" => serde::export::Ok(__Field::__field0),
                    "b" => serde::export::Ok(__Field::__field1),
                    "o" => serde::export::Ok(__Field::__field2),
                    _ => serde::export::Ok(__Field::__ignore),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"s" => serde::export::Ok(__Field::__field0),
                    b"b" => serde::export::Ok(__Field::__field1),
                    b"o" => serde::export::Ok(__Field::__field2),
                    _ => serde::export::Ok(__Field::__ignore),
                }
            }
        }
        impl<'de> serde::Deserialize<'de> for __Field {
            #[inline]
            fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
            where
                __D: serde::Deserializer<'de>,
            {
                serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
            }
        }
        struct __Visitor<'de> {
            marker: serde::export::PhantomData<PackLocation>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = PackLocation;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "struct PackLocation")
            }
            #[inline]
            fn visit_seq<__A>(
                self,
                mut __seq: __A,
            ) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::SeqAccess<'de>,
            {
                let __field0 = match match serde::de::SeqAccess::next_element::<String>(&mut __seq)
                {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    serde::export::Some(__value) => __value,
                    serde::export::None => {
                        return serde::export::Err(serde::de::Error::invalid_length(
                            0usize,
                            &"struct PackLocation with 3 elements",
                        ));
                    }
                };
                let __field1 = match match serde::de::SeqAccess::next_element::<String>(&mut __seq)
                {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    serde::export::Some(__value) => __value,
                    serde::export::None => {
                        return serde::export::Err(serde::de::Error::invalid_length(
                            1usize,
                            &"struct PackLocation with 3 elements",
                        ));
                    }
                };
                let __field2 = match match serde::de::SeqAccess::next_element::<String>(&mut __seq)
                {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    serde::export::Some(__value) => __value,
                    serde::export::None => {
                        return serde::export::Err(serde::de::Error::invalid_length(
                            2usize,
                            &"struct PackLocation with 3 elements",
                        ));
                    }
                };
                serde::export::Ok(PackLocation {
                    store: __field0,
                    bucket: __field1,
                    object: __field2,
                })
            }
            #[inline]
            fn visit_map<__A>(
                self,
                mut __map: __A,
            ) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::MapAccess<'de>,
            {
                let mut __field0: serde::export::Option<String> = serde::export::None;
                let mut __field1: serde::export::Option<String> = serde::export::None;
                let mut __field2: serde::export::Option<String> = serde::export::None;
                while let serde::export::Some(__key) =
                    match serde::de::MapAccess::next_key::<__Field>(&mut __map) {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    }
                {
                    match __key {
                        __Field::__field0 => {
                            if serde::export::Option::is_some(&__field0) {
                                return serde::export::Err(
                                    <__A::Error as serde::de::Error>::duplicate_field("s"),
                                );
                            }
                            __field0 = serde::export::Some(
                                match serde::de::MapAccess::next_value::<String>(&mut __map) {
                                    serde::export::Ok(__val) => __val,
                                    serde::export::Err(__err) => {
                                        return serde::export::Err(__err);
                                    }
                                },
                            );
                        }
                        __Field::__field1 => {
                            if serde::export::Option::is_some(&__field1) {
                                return serde::export::Err(
                                    <__A::Error as serde::de::Error>::duplicate_field("b"),
                                );
                            }
                            __field1 = serde::export::Some(
                                match serde::de::MapAccess::next_value::<String>(&mut __map) {
                                    serde::export::Ok(__val) => __val,
                                    serde::export::Err(__err) => {
                                        return serde::export::Err(__err);
                                    }
                                },
                            );
                        }
                        __Field::__field2 => {
                            if serde::export::Option::is_some(&__field2) {
                                return serde::export::Err(
                                    <__A::Error as serde::de::Error>::duplicate_field("o"),
                                );
                            }
                            __field2 = serde::export::Some(
                                match serde::de::MapAccess::next_value::<String>(&mut __map) {
                                    serde::export::Ok(__val) => __val,
                                    serde::export::Err(__err) => {
                                        return serde::export::Err(__err);
                                    }
                                },
                            );
                        }
                        _ => {
                            let _ = match serde::de::MapAccess::next_value::<serde::de::IgnoredAny>(
                                &mut __map,
                            ) {
                                serde::export::Ok(__val) => __val,
                                serde::export::Err(__err) => {
                                    return serde::export::Err(__err);
                                }
                            };
                        }
                    }
                }
                let __field0 = match __field0 {
                    serde::export::Some(__field0) => __field0,
                    serde::export::None => match serde::private::de::missing_field("s") {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    },
                };
                let __field1 = match __field1 {
                    serde::export::Some(__field1) => __field1,
                    serde::export::None => match serde::private::de::missing_field("b") {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    },
                };
                let __field2 = match __field2 {
                    serde::export::Some(__field2) => __field2,
                    serde::export::None => match serde::private::de::missing_field("o") {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    },
                };
                serde::export::Ok(PackLocation {
                    store: __field0,
                    bucket: __field1,
                    object: __field2,
                })
            }
        }
        const FIELDS: &'static [&'static str] = &["s", "b", "o"];
        serde::Deserializer::deserialize_struct(
            __deserializer,
            "PackLocation",
            FIELDS,
            __Visitor {
                marker: serde::export::PhantomData::<PackLocation>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}
