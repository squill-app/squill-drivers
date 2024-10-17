use proc_macro::TokenStream;
use quote::quote;

/// A procedural macro that generates an implementation of the `Decode` trait for a given type using Serde.
///
/// The generated implementation assumes that the type can be deserialized from a JSON string.
///
/// #example
/// ```rust,ignore
/// use serde::{Deserialize, Serialize};
//  use squill_drivers::serde::Decode;
//
// #[derive(Debug, Deserialize, Decode)]
// struct Person {
//     name: String,
//     age: u32,
// }
/// ```
#[proc_macro_derive(Decode)]
pub fn decode_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    // Get the name of the type to which the macro is applied
    let name = input.ident;

    // Generate the implementation of the trait for the type
    let expanded = quote! {
        impl squill_drivers::Decode for #name {

            fn decode(array: &dyn arrow_array::array::Array, index: usize) -> Self {
                match Self::try_decode(array, index) {
                    Ok(value) => value,
                    Err(e) => panic!("Unable to decode (reason: {:?})", e),
                }
            }

            fn try_decode(array: &dyn arrow_array::array::Array, index: usize) -> squill_drivers::Result<Self> {
                use squill_drivers::Error;
                let json_str = array
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| Error::InvalidType {
                        expected: "StringArray".to_string(),
                        actual: array.data_type().to_string(),
                    })?
                    .value(index);

                serde_json::from_str(json_str)
                    .map_err(|e| Error::InternalError { error: format!("Deserialization error: {:?}", e).into() })
            }
        }
    };

    // Convert the generated code into a TokenStream and return it
    TokenStream::from(expanded)
}
