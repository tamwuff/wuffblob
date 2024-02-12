pub static DEFAULT_FIXED: &str = include_str!("mime.types");
pub static DEFAULT_REGEX: &str = include_str!("mime_overrides.regex");

trait MimeTypesPrivate {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffBlobError>;
}

pub trait MimeTypes: MimeTypesPrivate {
    fn get_desired_mime_type(&self, basename: &std::ffi::OsStr) -> Option<&'static str>;
}

#[derive(Debug)]
pub struct MimeTypesFixed {
    data: std::collections::BTreeMap<std::ffi::OsString, &'static str>,
}

impl MimeTypesFixed {
    fn new() -> Self {
        MimeTypesFixed {
            data: std::collections::BTreeMap::<std::ffi::OsString, &'static str>::new(),
        }
    }
}

impl MimeTypesPrivate for MimeTypesFixed {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffBlobError> {
        let mut spec_as_osstring: std::ffi::OsString = std::ffi::OsStr::new(spec).to_os_string();
        spec_as_osstring.make_ascii_lowercase();
        let _ = self.data.insert(spec_as_osstring, mime_type);
        Ok(())
    }
}

impl MimeTypes for MimeTypesFixed {
    fn get_desired_mime_type(&self, basename: &std::ffi::OsStr) -> Option<&'static str> {
        let mut as_slice: &[u8] = basename.as_encoded_bytes();
        let mut i: usize = as_slice.len() - 1;
        loop {
            if as_slice[i] == ('.' as u8) {
                break;
            } else if i == 0 {
                // Can't find extension...!
                return None;
            }
            i -= 1;
        }
        let ext: &std::ffi::OsStr =
            unsafe { std::ffi::OsStr::from_encoded_bytes_unchecked(&as_slice[(i + 1)..]) };

        let mut ext_as_osstring: std::ffi::OsString = std::ffi::OsStr::new(ext).to_os_string();
        ext_as_osstring.make_ascii_lowercase();
        self.data.get(&ext_as_osstring).copied()
    }
}

#[derive(Debug)]
pub struct MimeTypesRegex {
    data: Vec<(regex::Regex, &'static str)>,
}

impl MimeTypesRegex {
    fn new() -> Self {
        MimeTypesRegex {
            data: Vec::<(regex::Regex, &'static str)>::new(),
        }
    }
}

impl MimeTypesPrivate for MimeTypesRegex {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffBlobError> {
        self.data.push((regex::Regex::new(spec)?, mime_type));
        Ok(())
    }
}

impl MimeTypes for MimeTypesRegex {
    fn get_desired_mime_type(&self, basename: &std::ffi::OsStr) -> Option<&'static str> {
        if let Some(as_str) = basename.to_str() {
            for (ref r, mime_type) in &self.data {
                if r.is_match(as_str) {
                    return Some(mime_type);
                }
            }
        }
        None
    }
}

pub fn new(
    data: &'static str,
    is_regex: bool,
) -> Result<Box<dyn MimeTypes + std::marker::Send + std::marker::Sync>, crate::error::WuffBlobError>
{
    let r1: regex::Regex = regex::RegexBuilder::new(r"^\s*([^#]\S*)\s+(\S.*?)\s*$")
        .multi_line(true)
        .crlf(true)
        .build()
        .expect("regex");
    let r2: regex::Regex = regex::Regex::new(r"").expect("regex");
    let mut mime_types: Box<dyn MimeTypes + std::marker::Send + std::marker::Sync> = {
        if is_regex {
            Box::new(MimeTypesRegex::new())
        } else {
            Box::new(MimeTypesFixed::new())
        }
    };
    for m in r1.captures_iter(data) {
        let mime_type: &'static str = m.get(1).unwrap().as_str();
        let specs: &'static str = m.get(2).unwrap().as_str();
        for spec in specs.split_whitespace() {
            mime_types.add_mapping(spec, mime_type)?;
        }
    }
    Ok(mime_types)
}

#[test]
fn fixed_with_c_and_txt() {
    let mut mime_types = MimeTypesFixed::new();
    mime_types.add_mapping("c", "text/x-c").unwrap();
    mime_types.add_mapping("txt", "text/plain").unwrap();
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.txt")),
        Some("text/plain")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.c")),
        Some("text/x-c")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.c.txt")),
        Some("text/plain")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.txt.c")),
        Some("text/x-c")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("txt")),
        None
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("c")),
        None
    );
}

#[test]
fn regex_with_readme_and_makefile() {
    let mut mime_types = MimeTypesRegex::new();
    mime_types
        .add_mapping("^Makefile$", "text/x-makefile")
        .unwrap();
    mime_types.add_mapping("^README$", "text/plain").unwrap();
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("README")),
        Some("text/plain")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("Makefile")),
        Some("text/x-makefile")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("xREADME")),
        None
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("MakefileX")),
        None
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("EADME")),
        None
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("Makefil")),
        None
    );
}

#[test]
fn parse_apache_format() {
    let mime_types = new("# This is a comment\n  # This is another comment\n# image/jpeg jpg\n  text/plain txt text\n text/x-c c\n#\n", false).unwrap();
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.txt")),
        Some("text/plain")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.text")),
        Some("text/plain")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.c")),
        Some("text/x-c")
    );
    assert_eq!(
        mime_types.get_desired_mime_type(std::ffi::OsStr::new("foo.jpg")),
        None
    );
}