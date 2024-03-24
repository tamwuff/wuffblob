pub static DEFAULT_FIXED: &str = include_str!("mime.types");
pub static DEFAULT_REGEX: &str = include_str!("mime_overrides.regex");

// for the benefit of unit tests elsewhere in the library
pub static MINIMAL_FIXED: &str = "text/plain txt\napplication/pdf pdf\n";

pub trait MimeTypes {
    fn get_desired_mime_type(
        &self,
        basename: &std::ffi::OsStr,
    ) -> Option<&'static str>;
}

trait MimeTypesPrivate: MimeTypes {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffError>;
}

// "Fixed" is the traditional mime.types from the Apache project, where it's
// all based on the file "extension".
//
// Does not handle things like README, or Makefile.

#[derive(Debug)]
pub struct MimeTypesFixed {
    data: std::collections::BTreeMap<std::ffi::OsString, &'static str>,
}

impl MimeTypesFixed {
    fn new() -> Self {
        MimeTypesFixed {
            data: std::collections::BTreeMap::new(),
        }
    }
}

impl MimeTypesPrivate for MimeTypesFixed {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffError> {
        let mut spec_as_osstring: std::ffi::OsString =
            std::ffi::OsStr::new(spec).to_os_string();
        spec_as_osstring.make_ascii_lowercase();
        let _ = self.data.insert(spec_as_osstring, mime_type);
        Ok(())
    }
}

impl MimeTypes for MimeTypesFixed {
    fn get_desired_mime_type(
        &self,
        basename: &std::ffi::OsStr,
    ) -> Option<&'static str> {
        let as_slice: &[u8] = basename.as_encoded_bytes();
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
        let ext: &std::ffi::OsStr = unsafe {
            std::ffi::OsStr::from_encoded_bytes_unchecked(&as_slice[(i + 1)..])
        };

        let mut ext_as_osstring: std::ffi::OsString =
            std::ffi::OsStr::new(ext).to_os_string();
        ext_as_osstring.make_ascii_lowercase();
        self.data.get(&ext_as_osstring).copied()
    }
}

// "Regex" hijacks the mime.types file format, and looks superficially
// similar, but instead of file "extensions", they are regexes.
//
// Nobody ships a mime.types file in this format. The file format being the
// same as the apache mime.types, doesn't do us any good, except that it's
// easier to remember how to construct one file format, than to remember
// the unique quirks of two different file formats.

#[derive(Debug)]
pub struct MimeTypesRegex {
    data: Vec<(regex::Regex, &'static str)>,
}

impl MimeTypesRegex {
    fn new() -> Self {
        MimeTypesRegex { data: Vec::new() }
    }
}

impl MimeTypesPrivate for MimeTypesRegex {
    fn add_mapping(
        &mut self,
        spec: &'static str,
        mime_type: &'static str,
    ) -> Result<(), crate::error::WuffError> {
        self.data.push((regex::Regex::new(spec)?, mime_type));
        Ok(())
    }
}

impl MimeTypes for MimeTypesRegex {
    fn get_desired_mime_type(
        &self,
        basename: &std::ffi::OsStr,
    ) -> Option<&'static str> {
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

// Putting it all together...

pub fn new(
    data: &'static str,
    is_regex: bool,
) -> Result<Box<dyn MimeTypes + Send + Sync>, crate::error::WuffError> {
    let r: regex::Regex =
        regex::RegexBuilder::new(r"^\s*([^#]\S*)\s+(\S.*?)\s*$")
            .multi_line(true)
            .crlf(true)
            .build()
            .expect("regex");

    // Hmmmm. What I really want to do is to make a Box<dyn MimeTypesPrivate>,
    // set it to either a MimeTypesRegex or a MimeTypesFixed, and then stop
    // caring about which one it is. I should be able to fill up my
    // Box<dyn MimeTypesPrivate> full of stuff no matter what it is
    // underneath. But then I run into problems when I have to return a
    // Box<dyn MimeTypes>. Because I don't have a Box<dyn MimeTypes>, and the
    // thing I do have is a Box<dyn MimeTypesPrivate> and is not convertable
    // to a Box<dyn MimeTypes>.
    //
    // I guess I can copy/paste the code that fills them up... Hnnnngg...

    if is_regex {
        let mut mime_types: MimeTypesRegex = MimeTypesRegex::new();

        // copied/pasted for loop :-(
        for m in r.captures_iter(data) {
            let mime_type: &'static str = m.get(1).unwrap().as_str();
            let specs: &'static str = m.get(2).unwrap().as_str();
            for spec in specs.split_whitespace() {
                mime_types.add_mapping(spec, mime_type)?;
            }
        }

        Ok(Box::new(mime_types))
    } else {
        let mut mime_types: MimeTypesFixed = MimeTypesFixed::new();

        // copied/pasted for loop :-(
        for m in r.captures_iter(data) {
            let mime_type: &'static str = m.get(1).unwrap().as_str();
            let specs: &'static str = m.get(2).unwrap().as_str();
            for spec in specs.split_whitespace() {
                mime_types.add_mapping(spec, mime_type)?;
            }
        }

        Ok(Box::new(mime_types))
    }
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
