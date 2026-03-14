use maud::{Markup, html};
use url::Url;

pub fn series(count: usize, base_url: Url) -> Markup {
    html! {
        @for i in (1..=count).rev() {
            @let path = format!("{}/{}", "chapters", i);
            div {
                a href=(base_url.join(path.as_str()).unwrap()) {}
            }
        }
    }
}

pub fn chapter(count: usize, base_url: Url, chapter_id: usize) -> Markup {
    html! {
        @for i in 1..=count {
            @let path = format!("{}/{}-{}.png", "images", chapter_id, i);
            img src=(base_url.join(path.as_str()).unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn series_ok() -> anyhow::Result<()> {
        let url = Url::parse("http://example.com")?;
        let render = series(2, url);

        assert_eq!(
            render.0,
            "<div><a href=\"http://example.com/chapters/2\"></a></div><div><a href=\"http://example.com/chapters/1\"></a></div>"
        );
        Ok(())
    }

    #[test]
    fn chapter_ok() -> anyhow::Result<()> {
        let url = Url::parse("http://example.com")?;
        let render = chapter(1, url, 1);
        // https://developer.mozilla.org/en-US/docs/Glossary/Void_element#self-closing_tags
        assert_eq!(render.0, "<img src=\"http://example.com/images/1-1.png\">");
        Ok(())
    }
}
