use html5gum::{
    Emitter, IoReader, Span, Tokenizer,
    emitters::callback::{CallbackEmitter, CallbackEvent},
};
use std::io::Read;
use url::Url;

pub fn anchors<'a, S>(r: S) -> impl Iterator<Item = Result<Url, url::ParseError>>
where
    S: Read,
{
    Tokenizer::new_with_emitter(IoReader::new(r), emitter("a", "href")).flatten()
}

pub fn images<'a, S>(r: S) -> impl Iterator<Item = Result<Url, url::ParseError>>
where
    S: Read,
{
    // now the input needs to be a result that we are parsing
    Tokenizer::new_with_emitter(IoReader::new(r), emitter("img", "src")).flatten()
}

fn emitter(tag: &str, attr: &str) -> impl Emitter<Token = Result<Url, url::ParseError>> {
    let mut is_tag = false;
    let mut is_attr = false;

    CallbackEmitter::new(
        move |event: CallbackEvent<'_>, _span: Span<()>| match event {
            CallbackEvent::OpenStartTag { name } => {
                is_tag = name == tag.as_bytes();
                is_attr = false;
                None
            }
            CallbackEvent::AttributeName { name } => {
                is_attr = name == attr.as_bytes();
                None
            }
            CallbackEvent::AttributeValue { value } if is_tag && is_attr => {
                Some(Url::parse(&String::from_utf8_lossy(value)))
            }
            _ => None,
        },
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn anchors_ok() -> anyhow::Result<()> {
        // See more https://stackoverflow.com/a/63650247
        // See more https://github.com/cloudflare/lol-html
        // https://docs.rs/html5ever/0.25.1/html5ever/tokenizer/struct.Tokenizer.html#method.feed
        let input =
            "<h1>Hello world</h1><a href=\"http://a.com\">bar</a><a href=\"http://b.com\">bar</a>";
        let reader = BufReader::new(input.as_bytes());

        let links = anchors(reader).collect::<Vec<_>>();
        assert!(links.iter().all(|r| r.is_ok()));
        assert_eq!(links.len(), 2);
        Ok(())
    }

    #[test]
    fn images_ok() -> anyhow::Result<()> {
        // See more https://stackoverflow.com/a/63650247
        // See more https://github.com/cloudflare/lol-html
        // https://docs.rs/html5ever/0.25.1/html5ever/tokenizer/struct.Tokenizer.html#method.feed
        let input = "<h1>Hello world</h1><img class=\"test\" src=\"http://a.com\" /><a href=\"http://b.com\">bar</a>";
        let reader = BufReader::new(input.as_bytes());

        let links = images(reader).collect::<Vec<_>>();
        assert!(links.iter().all(|r| r.is_ok()));
        assert_eq!(links.len(), 1);

        let Some(Ok(first)) = links.first() else {
            return Err(anyhow::anyhow!("expected exactly one Ok link"));
        };
        assert_eq!(first.as_str(), "http://a.com/"); //? why the slash
        Ok(())
    }
}
