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
use crate::domain::entities::Checksum;

impl serde::Serialize for Checksum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            Checksum::SHA1(ref s) => serializer.serialize_newtype_variant("Checksum", 0, "SHA1", s),
            Checksum::SHA256(ref s) => {
                serializer.serialize_newtype_variant("Checksum", 1, "SHA256", s)
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for Checksum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        enum __Field {
            __field0,
            __field1,
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
                    _ => serde::export::Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(__value),
                        &"variant index 0 <= i < 2",
                    )),
                }
            }
            fn visit_str<__E>(self, __value: &str) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    "SHA1" => serde::export::Ok(__Field::__field0),
                    "SHA256" => serde::export::Ok(__Field::__field1),
                    _ => serde::export::Err(serde::de::Error::unknown_variant(__value, VARIANTS)),
                }
            }
            fn visit_bytes<__E>(self, __value: &[u8]) -> serde::export::Result<Self::Value, __E>
            where
                __E: serde::de::Error,
            {
                match __value {
                    b"SHA1" => serde::export::Ok(__Field::__field0),
                    b"SHA256" => serde::export::Ok(__Field::__field1),
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
            marker: serde::export::PhantomData<Checksum>,
            lifetime: serde::export::PhantomData<&'de ()>,
        }
        impl<'de> serde::de::Visitor<'de> for __Visitor<'de> {
            type Value = Checksum;
            fn expecting(
                &self,
                __formatter: &mut serde::export::Formatter,
            ) -> serde::export::fmt::Result {
                serde::export::Formatter::write_str(__formatter, "enum Checksum")
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
                        serde::de::VariantAccess::newtype_variant::<String>(__variant),
                        Checksum::SHA1,
                    ),
                    (__Field::__field1, __variant) => serde::export::Result::map(
                        serde::de::VariantAccess::newtype_variant::<String>(__variant),
                        Checksum::SHA256,
                    ),
                }
            }
        }
        const VARIANTS: &'static [&'static str] = &["SHA1", "SHA256"];
        deserializer.deserialize_enum(
            "Checksum",
            VARIANTS,
            __Visitor {
                marker: serde::export::PhantomData::<Checksum>,
                lifetime: serde::export::PhantomData,
            },
        )
    }
}
