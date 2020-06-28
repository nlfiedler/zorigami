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
use crate::domain::entities::schedule::{DayOfMonth, DayOfWeek, Schedule, TimeRange};

impl serde::Serialize for DayOfWeek {
    fn serialize<__S>(&self, __serializer: __S) -> serde::export::Result<__S::Ok, __S::Error>
    where
        __S: serde::Serializer,
    {
        match *self {
            DayOfWeek::Sun => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 0u32, "Sun")
            }
            DayOfWeek::Mon => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 1u32, "Mon")
            }
            DayOfWeek::Tue => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 2u32, "Tue")
            }
            DayOfWeek::Wed => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 3u32, "Wed")
            }
            DayOfWeek::Thu => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 4u32, "Thu")
            }
            DayOfWeek::Fri => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 5u32, "Fri")
            }
            DayOfWeek::Sat => {
                serde::Serializer::serialize_unit_variant(__serializer, "DayOfWeek", 6u32, "Sat")
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for DayOfWeek {
    fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
    where
        __D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
            __field2,
            __field3,
            __field4,
            __field5,
            __field6,
        }
        struct __FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "variant identifier")
            }
            fn visit_u64<__E>(self, __value: u64) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    0u64 => serde::export::Ok(__Field::__field0),
                    1u64 => serde::export::Ok(__Field::__field1),
                    2u64 => serde::export::Ok(__Field::__field2),
                    3u64 => serde::export::Ok(__Field::__field3),
                    4u64 => serde::export::Ok(__Field::__field4),
                    5u64 => serde::export::Ok(__Field::__field5),
                    6u64 => serde::export::Ok(__Field::__field6),
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"variant index 0 <= i < 7",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "Sun" => serde::export::Ok(__Field::__field0),
                    "Mon" => serde::export::Ok(__Field::__field1),
                    "Tue" => serde::export::Ok(__Field::__field2),
                    "Wed" => serde::export::Ok(__Field::__field3),
                    "Thu" => serde::export::Ok(__Field::__field4),
                    "Fri" => serde::export::Ok(__Field::__field5),
                    "Sat" => serde::export::Ok(__Field::__field6),
                    _ => serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS)),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"Sun" => serde::export::Ok(__Field::__field0),
                    b"Mon" => serde::export::Ok(__Field::__field1),
                    b"Tue" => serde::export::Ok(__Field::__field2),
                    b"Wed" => serde::export::Ok(__Field::__field3),
                    b"Thu" => serde::export::Ok(__Field::__field4),
                    b"Fri" => serde::export::Ok(__Field::__field5),
                    b"Sat" => serde::export::Ok(__Field::__field6),
                    _ => {
                        let __value = &serde::export::from_utf8_lossy(__value);
                        serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS))
                    }
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
            marker: serde::export::PhantomData<DayOfWeek>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = DayOfWeek;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "enum DayOfWeek")
            }
            fn visit_enum<__A>(self, __data: __A) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::EnumAccess<'de>,
            {
                match match serde::de::EnumAccess::variant(__data) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    (__Field::__field0, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Sun)
                    }
                    (__Field::__field1, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Mon)
                    }
                    (__Field::__field2, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Tue)
                    }
                    (__Field::__field3, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Wed)
                    }
                    (__Field::__field4, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Thu)
                    }
                    (__Field::__field5, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Fri)
                    }
                    (__Field::__field6, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(DayOfWeek::Sat)
                    }
                }
            }
        }
        const VARIANTS: &'static [&'static str] =
            &["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
        serde::Deserializer::deserialize_enum(
            __deserializer,
            "DayOfWeek",
            VARIANTS,
            __Visitor {
                marker: serde::export::PhantomData::<DayOfWeek>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}

impl serde::Serialize for TimeRange {
    fn serialize<__S>(&self, __serializer: __S) -> serde::export::Result<__S::Ok, __S::Error>
    where
        __S: serde::Serializer,
    {
        let mut _serde_state = match serde::Serializer::serialize_struct(
            __serializer,
            "TimeRange",
            false as usize + 1 + 1,
        ) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        match serde::ser::SerializeStruct::serialize_field(&mut _serde_state, "start", &self.start)
        {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        match serde::ser::SerializeStruct::serialize_field(&mut _serde_state, "stop", &self.stop) {
            serde::export::Ok(__val) => __val,
            serde::export::Err(__err) => {
                return serde::export::Err(__err);
            }
        };
        serde::ser::SerializeStruct::end(_serde_state)
    }
}

impl<'de> serde::Deserialize<'de> for TimeRange {
    fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
    where
        __D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
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
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"field index 0 <= i < 2",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "start" => serde::export::Ok(__Field::__field0),
                    "stop" => serde::export::Ok(__Field::__field1),
                    _ => serde::export::Ok(__Field::__ignore),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"start" => serde::export::Ok(__Field::__field0),
                    b"stop" => serde::export::Ok(__Field::__field1),
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
            marker: serde::export::PhantomData<TimeRange>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = TimeRange;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "struct TimeRange")
            }
            #[inline]
            fn visit_seq<__A>(
                self,
                mut __seq: __A,
            ) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::SeqAccess<'de>,
            {
                let __field0 = match match serde::de::SeqAccess::next_element::<u32>(&mut __seq) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    serde::export::Some(__value) => __value,
                    serde::export::None => {
                        return serde::export::Err(serde::de::Error::invalid_length(
                            0usize,
                            &"struct TimeRange with 2 elements",
                        ));
                    }
                };
                let __field1 = match match serde::de::SeqAccess::next_element::<u32>(&mut __seq) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    serde::export::Some(__value) => __value,
                    serde::export::None => {
                        return serde::export::Err(serde::de::Error::invalid_length(
                            1usize,
                            &"struct TimeRange with 2 elements",
                        ));
                    }
                };
                serde::export::Ok(TimeRange {
                    start: __field0,
                    stop: __field1,
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
                let mut __field0: serde::export::Option<u32> = serde::export::None;
                let mut __field1: serde::export::Option<u32> = serde::export::None;
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
                                    <__A::Error as serde::de::Error>::duplicate_field("start"),
                                );
                            }
                            __field0 = serde::export::Some(
                                match serde::de::MapAccess::next_value::<u32>(&mut __map) {
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
                                    <__A::Error as serde::de::Error>::duplicate_field("stop"),
                                );
                            }
                            __field1 = serde::export::Some(
                                match serde::de::MapAccess::next_value::<u32>(&mut __map) {
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
                    serde::export::None => match serde::private::de::missing_field("start") {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    },
                };
                let __field1 = match __field1 {
                    serde::export::Some(__field1) => __field1,
                    serde::export::None => match serde::private::de::missing_field("stop") {
                        serde::export::Ok(__val) => __val,
                        serde::export::Err(__err) => {
                            return serde::export::Err(__err);
                        }
                    },
                };
                serde::export::Ok(TimeRange {
                    start: __field0,
                    stop: __field1,
                })
            }
        }
        const FIELDS: &'static [&'static str] = &["start", "stop"];
        serde::Deserializer::deserialize_struct(
            __deserializer,
            "TimeRange",
            FIELDS,
            __Visitor {
                marker: serde::export::PhantomData::<TimeRange>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}

impl serde::Serialize for DayOfMonth {
    fn serialize<__S>(&self, __serializer: __S) -> serde::export::Result<__S::Ok, __S::Error>
    where
        __S: serde::Serializer,
    {
        match *self {
            DayOfMonth::First(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                0u32,
                "First",
                __field0,
            ),
            DayOfMonth::Second(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                1u32,
                "Second",
                __field0,
            ),
            DayOfMonth::Third(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                2u32,
                "Third",
                __field0,
            ),
            DayOfMonth::Fourth(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                3u32,
                "Fourth",
                __field0,
            ),
            DayOfMonth::Fifth(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                4u32,
                "Fifth",
                __field0,
            ),
            DayOfMonth::Day(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "DayOfMonth",
                5u32,
                "Day",
                __field0,
            ),
        }
    }
}

impl<'de> serde::Deserialize<'de> for DayOfMonth {
    fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
    where
        __D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
            __field2,
            __field3,
            __field4,
            __field5,
        }
        struct __FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "variant identifier")
            }
            fn visit_u64<__E>(self, __value: u64) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    0u64 => serde::export::Ok(__Field::__field0),
                    1u64 => serde::export::Ok(__Field::__field1),
                    2u64 => serde::export::Ok(__Field::__field2),
                    3u64 => serde::export::Ok(__Field::__field3),
                    4u64 => serde::export::Ok(__Field::__field4),
                    5u64 => serde::export::Ok(__Field::__field5),
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"variant index 0 <= i < 6",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "First" => serde::export::Ok(__Field::__field0),
                    "Second" => serde::export::Ok(__Field::__field1),
                    "Third" => serde::export::Ok(__Field::__field2),
                    "Fourth" => serde::export::Ok(__Field::__field3),
                    "Fifth" => serde::export::Ok(__Field::__field4),
                    "Day" => serde::export::Ok(__Field::__field5),
                    _ => serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS)),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"First" => serde::export::Ok(__Field::__field0),
                    b"Second" => serde::export::Ok(__Field::__field1),
                    b"Third" => serde::export::Ok(__Field::__field2),
                    b"Fourth" => serde::export::Ok(__Field::__field3),
                    b"Fifth" => serde::export::Ok(__Field::__field4),
                    b"Day" => serde::export::Ok(__Field::__field5),
                    _ => {
                        let __value = &serde::export::from_utf8_lossy(__value);
                        serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS))
                    }
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
            marker: serde::export::PhantomData<DayOfMonth>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = DayOfMonth;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "enum DayOfMonth")
            }
            fn visit_enum<__A>(self, __data: __A) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::EnumAccess<'de>,
            {
                match match serde::de::EnumAccess::variant(__data) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    (__Field::__field0, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<DayOfWeek>(__variant),
                        DayOfMonth::First,
                    ),
                    (__Field::__field1, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<DayOfWeek>(__variant),
                        DayOfMonth::Second,
                    ),
                    (__Field::__field2, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<DayOfWeek>(__variant),
                        DayOfMonth::Third,
                    ),
                    (__Field::__field3, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<DayOfWeek>(__variant),
                        DayOfMonth::Fourth,
                    ),
                    (__Field::__field4, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<DayOfWeek>(__variant),
                        DayOfMonth::Fifth,
                    ),
                    (__Field::__field5, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<u8>(__variant),
                        DayOfMonth::Day,
                    ),
                }
            }
        }
        const VARIANTS: &'static [&'static str] =
            &["First", "Second", "Third", "Fourth", "Fifth", "Day"];
        serde::Deserializer::deserialize_enum(
            __deserializer,
            "DayOfMonth",
            VARIANTS,
            __Visitor {
                marker: serde::export::PhantomData::<DayOfMonth>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}

impl serde::Serialize for Schedule {
    fn serialize<__S>(&self, __serializer: __S) -> serde::export::Result<__S::Ok, __S::Error>
    where
        __S: serde::Serializer,
    {
        match *self {
            Schedule::Hourly => {
                serde::Serializer::serialize_unit_variant(__serializer, "Schedule", 0u32, "Hourly")
            }
            Schedule::Daily(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "Schedule",
                1u32,
                "Daily",
                __field0,
            ),
            Schedule::Weekly(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "Schedule",
                2u32,
                "Weekly",
                __field0,
            ),
            Schedule::Monthly(ref __field0) => serde::Serializer::serialize_newtype_variant(
                __serializer,
                "Schedule",
                3u32,
                "Monthly",
                __field0,
            ),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Schedule {
    fn deserialize<__D>(__deserializer: __D) -> serde::export::Result<Self, __D::Error>
    where
        __D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
            __field2,
            __field3,
        }
        struct __FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "variant identifier")
            }
            fn visit_u64<__E>(self, __value: u64) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    0u64 => serde::export::Ok(__Field::__field0),
                    1u64 => serde::export::Ok(__Field::__field1),
                    2u64 => serde::export::Ok(__Field::__field2),
                    3u64 => serde::export::Ok(__Field::__field3),
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"variant index 0 <= i < 4",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "Hourly" => serde::export::Ok(__Field::__field0),
                    "Daily" => serde::export::Ok(__Field::__field1),
                    "Weekly" => serde::export::Ok(__Field::__field2),
                    "Monthly" => serde::export::Ok(__Field::__field3),
                    _ => serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS)),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"Hourly" => serde::export::Ok(__Field::__field0),
                    b"Daily" => serde::export::Ok(__Field::__field1),
                    b"Weekly" => serde::export::Ok(__Field::__field2),
                    b"Monthly" => serde::export::Ok(__Field::__field3),
                    _ => {
                        let __value = &serde::export::from_utf8_lossy(__value);
                        serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS))
                    }
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
            marker: serde::export::PhantomData<Schedule>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = Schedule;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "enum Schedule")
            }
            fn visit_enum<__A>(self, __data: __A) -> serde::export::Result<Self::Value, __A::Error>
            where
                __A: serde::de::EnumAccess<'de>,
            {
                match match serde::de::EnumAccess::variant(__data) {
                    serde::export::Ok(__val) => __val,
                    serde::export::Err(__err) => {
                        return serde::export::Err(__err);
                    }
                } {
                    (__Field::__field0, __variant) => {
                        match serde::de::VariantAccess::unit_variant(__variant) {
                            serde::export::Ok(__val) => __val,
                            serde::export::Err(__err) => {
                                return serde::export::Err(__err);
                            }
                        };
                        serde::export::Ok(Schedule::Hourly)
                    }
                    (__Field::__field1, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<Option<TimeRange>>(__variant),
                        Schedule::Daily,
                    ),
                    (__Field::__field2, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<
                            Option<(DayOfWeek, Option<TimeRange>)>,
                        >(__variant),
                        Schedule::Weekly,
                    ),
                    (__Field::__field3, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<
                            Option<(DayOfMonth, Option<TimeRange>)>,
                        >(__variant),
                        Schedule::Monthly,
                    ),
                }
            }
        }
        const VARIANTS: &'static [&'static str] = &["Hourly", "Daily", "Weekly", "Monthly"];
        serde::Deserializer::deserialize_enum(
            __deserializer,
            "Schedule",
            VARIANTS,
            __Visitor {
                marker: serde::export::PhantomData::<Schedule>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}