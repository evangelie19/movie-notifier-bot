use proc_macro::{Delimiter, Group, TokenStream, TokenTree};

#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut block: Option<Group> = None;
    for token in item.clone() {
        if let TokenTree::Group(group) = token {
            if group.delimiter() == Delimiter::Brace {
                block = Some(group);
            }
        }
    }
    let body = block.expect("ожидается блок функции main");
    let inner = body.stream().to_string();
    let expanded = format!(
        "fn main() {{ tokio::runtime::block_on(async move {{ {inner} }}) }}"
    );
    expanded.parse().expect("не удалось сгенерировать main")
}
