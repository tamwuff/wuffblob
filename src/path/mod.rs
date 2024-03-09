#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct WuffPath {
    components: Vec<std::ffi::OsString>,
}

// If you have an OsStr, you should be able to convert it losslessly to a
// WuffPath and back. If converting from an OsStr to a WuffPath and then back
// to an OsStr ever gives you back a different OsStr than the one you started
// with, it is a bug!!
//
// That does not, however, mean that all WuffPaths are equally, shall we say,
// "kosher". It just means that if you have an OsStr that is treif, you can
// convert it to a WuffPath that is treif, and you can convert it back to the
// same treif OsStr.
//
// Also, the same does not hold if you have a WuffPath. Converting it to an
// OsStr and then back to a WuffPath may in fact give you a different WuffPath
// than the one you started with. This is because WuffPaths allow a special
// "componentless" value which is not representable as an OsStr.
//
// There are also plenty of validation and canonicalization routines in here,
// too.
//
// I would have liked to have been able to use Path, but I wanted something
// where the separator character was always '/', and where I could have
// tighter control over splitting and joining.

impl WuffPath {
    pub fn from_osstr(s: &std::ffi::OsStr) -> WuffPath {
        let mut components: Vec<std::ffi::OsString> = Vec::new();
        components.push(std::ffi::OsString::new());
        let as_slice: &[u8] = s.as_encoded_bytes();
        for i in 0..as_slice.len() {
            if as_slice[i] == ('/' as u8) {
                components.push(std::ffi::OsString::new());
            } else {
                let c: &std::ffi::OsStr = unsafe {
                    std::ffi::OsStr::from_encoded_bytes_unchecked(
                        &as_slice[i..=i],
                    )
                };
                components.last_mut().unwrap().push(c);
            }
        }
        WuffPath {
            components: components,
        }
    }

    pub fn new() -> WuffPath {
        WuffPath {
            components: Vec::new(),
        }
    }

    pub fn push(&mut self, s: &std::ffi::OsStr) {
        self.components.push(s.into());
    }

    pub fn to_osstring(&self) -> std::ffi::OsString {
        let mut s: std::ffi::OsString = std::ffi::OsString::new();
        let mut first: bool = true;
        for component in &self.components {
            if first {
                first = false;
            } else {
                s.push(std::ffi::OsStr::new("/"));
            }
            s.push(component);
        }
        s
    }

    pub fn is_componentless(&self) -> bool {
        self.components.is_empty()
    }

    pub fn is_canonical(&self) -> bool {
        // If it's componentless, it's not canonical.
        if self.is_componentless() {
            return false;
        }

        // If any of its components are ".", "..", or contain a NUL byte or
        // a slash (*inside* the component), it's not canonical. If it starts
        // or ends with a slash, or contains two slashes in a row, it's not
        // canonical.
        for component in &self.components {
            if component.is_empty()
                || (component == std::ffi::OsStr::new("."))
                || (component == std::ffi::OsStr::new(".."))
            {
                return false;
            }
            for byte in component.as_encoded_bytes() {
                if (*byte == 0u8) || (*byte == ('/' as u8)) {
                    return false;
                }
            }
        }

        true
    }

    // Componentless paths aren't canonical, but that doesn't make them
    // worthless. If you take a componentless path and you stick a path
    // component onto the end, you get a canonical path. So that means that a
    // componentless path is really a canonical path in potentia. It is just
    // in need of a component.
    pub fn canonicalize_or_componentless(mut self) -> Option<WuffPath> {
        // If it's componentless, well, then, it's componentless.
        if self.is_componentless() {
            return Some(self);
        }

        // If it starts with a slash, it's not canonicalizable.
        if (self.components.len() > 1) && self.components[0].is_empty() {
            return None;
        }

        // Try just removing all the dots, and all the consecutive slashes.
        // See if that fixes it.
        self.components
            .retain(|component: &std::ffi::OsString| -> bool {
                (!component.is_empty())
                    && (component != std::ffi::OsStr::new("."))
            });

        // We didn't check for ".." or for NUL or '/' bytes. Do that now.
        if self.is_componentless() || self.is_canonical() {
            Some(self)
        } else {
            None
        }
    }

    pub fn canonicalize(self) -> Option<WuffPath> {
        let canonicalized_or_componentless: Option<WuffPath> =
            self.canonicalize_or_componentless();
        match canonicalized_or_componentless {
            Some(ref canonicalized) => {
                if canonicalized.is_componentless() {
                    return None;
                }
            }
            _ => {}
        }
        canonicalized_or_componentless
    }

    // This method will not work right if we are not in canonical form.
    // Please only call this if is_canonical() has returned true!
    pub fn basename(&self) -> Option<&std::ffi::OsString> {
        self.components.last()
    }

    // This method will not work right if we are not in canonical form.
    // Please only call this if is_canonical() has returned true!
    //
    // This method considers the componentless path to be the ultimate parent
    // of everything. Sometimes it is useful to consider the componentless
    // path; sometimes it isn't useful. It's up to you whether you pay
    // attention to it or not, as it goes past.
    pub fn all_parents<F>(&self, mut cb: F)
    where
        F: FnMut(&WuffPath),
    {
        let mut len: usize = self.components.len();
        if len == 0 {
            return;
        }
        let mut parent: WuffPath = self.clone();
        while len > 0 {
            len -= 1;
            parent.components.truncate(len);
            cb(&parent);
        }
    }

    // Please only use this if you have checked that everything is either
    // canonical or componentless!
    pub fn check_for_overlap<'a, T>(
        paths: T,
    ) -> Result<(), crate::error::WuffError>
    where
        T: IntoIterator<Item = &'a WuffPath>,
    {
        let mut as_set: std::collections::BTreeSet<&'a WuffPath> =
            std::collections::BTreeSet::new();
        for path in paths {
            // caller should have ensured this
            assert!(path.is_componentless() || path.is_canonical());

            if !as_set.insert(path) {
                return Err(format!("duplicate path {}", path).into());
            }
        }
        for path in &as_set {
            let mut conflict: Option<&'a WuffPath> = None;
            let mut cb = |parent: &WuffPath| {
                if let Some(wp) = as_set.get(parent) {
                    conflict = Some(wp);
                }
            };
            path.all_parents(&mut cb);
            if let Some(wp) = conflict {
                return Err(
                    format!("paths {} and {} overlap", wp, path).into()
                );
            }
        }
        Ok(())
    }
}

impl From<&std::ffi::OsStr> for WuffPath {
    fn from(s: &std::ffi::OsStr) -> WuffPath {
        WuffPath::from_osstr(s)
    }
}

impl From<&str> for WuffPath {
    fn from(s: &str) -> WuffPath {
        WuffPath::from_osstr(std::ffi::OsStr::new(s))
    }
}

impl TryFrom<&std::path::Path> for WuffPath {
    type Error = crate::error::WuffError;

    fn try_from(
        path: &std::path::Path,
    ) -> Result<WuffPath, crate::error::WuffError> {
        // Per the docs, Windows has something called a "prefix" and paths can
        // have prefixes, or roots, or both, or neither.
        //
        // I'm fine with letting paths through here if they have roots, because
        // a root is just "/" and we know how to deal with that.
        //
        // If a path has a "prefix" there is no valid WuffPath representation
        // of it.
        let mut components: Vec<std::ffi::OsString> = Vec::new();
        for component in path.components() {
            components.push(match component {
                std::path::Component::Prefix(_) => {
                    return Err(
                        format!("{:?}: prefixes not supported", path).into()
                    );
                }
                std::path::Component::RootDir => std::ffi::OsString::new(),
                std::path::Component::CurDir => {
                    std::ffi::OsStr::new(".").to_os_string()
                }
                std::path::Component::ParentDir => {
                    std::ffi::OsStr::new("..").to_os_string()
                }
                std::path::Component::Normal(s) => s.to_os_string(),
            });
        }
        Ok(WuffPath {
            components: components,
        })
    }
}

impl std::fmt::Display for WuffPath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let s: std::ffi::OsString = self.to_osstring();
        write!(f, "{:?}", s)
    }
}

#[test]
fn empty() {
    let p: WuffPath = "".into();
    assert_eq!(p.components, vec!(std::ffi::OsString::new()));
    assert_eq!(p.to_osstring(), std::ffi::OsString::new());
}

#[test]
fn nonempty() {
    let p: WuffPath = "a".into();
    assert_eq!(
        p.components,
        vec!(Into::<std::ffi::OsString>::into(String::from("a")))
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("a"))
    );
}

#[test]
fn slash() {
    let p: WuffPath = "/".into();
    assert_eq!(
        p.components,
        vec!(std::ffi::OsString::new(), std::ffi::OsString::new())
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("/"))
    );
}

#[test]
fn two_components() {
    let p: WuffPath = "foo/bar".into();
    assert_eq!(
        p.components,
        vec!(
            Into::<std::ffi::OsString>::into(String::from("foo")),
            Into::<std::ffi::OsString>::into(String::from("bar"))
        )
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("foo/bar"))
    );
}

#[test]
fn two_components_starting_with_slash() {
    let p: WuffPath = "/foo/bar".into();
    assert_eq!(
        p.components,
        vec!(
            std::ffi::OsString::new(),
            Into::<std::ffi::OsString>::into(String::from("foo")),
            Into::<std::ffi::OsString>::into(String::from("bar"))
        )
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("/foo/bar"))
    );
}

#[test]
fn two_components_ending_with_slash() {
    let p: WuffPath = "foo/bar/".into();
    assert_eq!(
        p.components,
        vec!(
            Into::<std::ffi::OsString>::into(String::from("foo")),
            Into::<std::ffi::OsString>::into(String::from("bar")),
            std::ffi::OsString::new()
        )
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("foo/bar/"))
    );
}

#[test]
fn empty_is_not_canonical() {
    let p: WuffPath = "".into();
    assert!(!p.is_canonical());
}

#[test]
fn ends_with_slash_is_not_canonical() {
    let p: WuffPath = "foo/bar/".into();
    assert!(!p.is_canonical());
}

#[test]
fn starts_with_slash_is_not_canonical() {
    let p: WuffPath = "/foo/bar".into();
    assert!(!p.is_canonical());
}

#[test]
fn two_slashes_is_not_canonical() {
    let p: WuffPath = "foo//bar".into();
    assert!(!p.is_canonical());
}

#[test]
fn has_dot_is_not_canonical() {
    let p: WuffPath = "foo/./bar".into();
    assert!(!p.is_canonical());
}

#[test]
fn has_dotdot_is_not_canonical() {
    let p: WuffPath = "foo/../bar".into();
    assert!(!p.is_canonical());
}

#[test]
fn has_dot_somethingorother_is_canonical() {
    let p: WuffPath = "foo/.bar".into();
    assert!(p.is_canonical());
}

#[test]
fn has_null_is_not_canonical() {
    let mut s = std::ffi::OsString::new();
    s.push(std::ffi::OsStr::new("foo/baaa"));
    unsafe {
        s.push(std::ffi::OsStr::from_encoded_bytes_unchecked(&[0u8; 1]));
    }
    s.push(std::ffi::OsStr::new("aaaaar/baz"));
    let p = WuffPath::from_osstr(&s);
    assert!(!p.is_canonical());
}

// This is simulating the case where on some different OS (say, classic Mac)
// we might have an embedded slash within *one component* of a filename.
#[test]
fn has_slash_is_not_canonical() {
    let p = WuffPath {
        components: vec![
            Into::<std::ffi::OsString>::into(String::from("foo/bar")),
            Into::<std::ffi::OsString>::into(String::from("baz")),
        ],
    };
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("foo/bar/baz"))
    );
    assert!(!p.is_canonical());
}

#[test]
fn canonical_is_canonical() {
    let p: WuffPath = "foo/bar/baz".into();
    assert!(p.is_canonical());
}

#[test]
fn canonicalize_absolute_path_should_fail() {
    let p: WuffPath = "/foo/bar".into();
    let c = p.canonicalize();
    assert!(c.is_none());
}

#[test]
fn canonicalize_path_with_dotdot_should_fail() {
    let p: WuffPath = "foo/../bar".into();
    let c = p.canonicalize();
    assert!(c.is_none());
}

#[test]
fn canonicalize_mildly_funky_path() {
    let p: WuffPath = "foo//.//bar//".into();
    let c = p.canonicalize();
    assert!(c.is_some());
    assert_eq!(
        c.unwrap().to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("foo/bar"))
    );
}

#[test]
fn slash_is_not_canonicalizable_even_if_we_allow_componentless() {
    let p: WuffPath = "/".into();
    let c = p.canonicalize_or_componentless();
    assert!(c.is_none());
}

#[test]
fn empty_is_not_canonicalizable_unless_we_allow_componentless() {
    let p: WuffPath = "".into();
    let c = p.canonicalize();
    assert!(c.is_none());
}

#[test]
fn empty_is_canonicalizable_if_we_allow_componentless() {
    let p: WuffPath = "".into();
    let c = p.canonicalize_or_componentless();
    assert!(c.is_some());
    assert!(c.unwrap().is_componentless());
}

#[test]
fn componentless_can_be_canonicalized_to_componentless() {
    let p = WuffPath::new();
    let c = p.canonicalize_or_componentless();
    assert!(c.is_some());
    assert!(c.unwrap().is_componentless());
}

#[test]
fn dots_and_slashes_can_be_canonicalized_to_componentless() {
    let p: WuffPath = ".//.//.//".into();
    let c = p.canonicalize_or_componentless();
    assert!(c.is_some());
    assert!(c.unwrap().is_componentless());
}

#[test]
fn dots_starting_with_slash_cannot_be_canonicalized() {
    let p: WuffPath = "/.//.//.//".into();
    let c = p.canonicalize_or_componentless();
    assert!(c.is_none());
}

#[test]
fn componentless_has_no_parents() {
    let mut v = Vec::<WuffPath>::new();
    let cb = |wp: &WuffPath| {
        v.push(wp.clone());
    };
    let p = WuffPath::new();
    p.all_parents(cb);
    assert!(v.is_empty());
}

#[test]
fn plenty_of_parents() {
    let mut v = Vec::<WuffPath>::new();
    let cb = |wp: &WuffPath| {
        v.push(wp.clone());
    };
    let p: WuffPath = "foo/bar/baz".into();
    p.all_parents(cb);
    assert_eq!(v, vec!("foo/bar".into(), "foo".into(), WuffPath::new()));
}

#[test]
fn from_path_sensible_nice_ok_happy() {
    let s: &std::path::Path = std::path::Path::new("foo/bar");
    let p = TryInto::<WuffPath>::try_into(s);
    assert!(p.is_ok());
    let p: WuffPath = p.unwrap();
    assert_eq!(
        p.components,
        vec!(
            Into::<std::ffi::OsString>::into(String::from("foo")),
            Into::<std::ffi::OsString>::into(String::from("bar")),
        )
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("foo/bar"))
    );
}

#[test]
fn from_path_gnarly_lots_of_things() {
    let s: &std::path::Path = std::path::Path::new("/foo/../bar");
    let p = TryInto::<WuffPath>::try_into(s);
    assert!(p.is_ok());
    let p: WuffPath = p.unwrap();
    assert_eq!(
        p.components,
        vec!(
            std::ffi::OsString::new(),
            Into::<std::ffi::OsString>::into(String::from("foo")),
            Into::<std::ffi::OsString>::into(String::from("..")),
            Into::<std::ffi::OsString>::into(String::from("bar")),
        )
    );
    assert_eq!(
        p.to_osstring(),
        Into::<std::ffi::OsString>::into(String::from("/foo/../bar"))
    );
}

#[test]
fn empty_vec_has_no_overlap() {
    let v: Vec<WuffPath> = Vec::new();
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_ok());
}

#[test]
fn non_overlapping_vec_has_no_overlap() {
    let v: Vec<WuffPath> = vec!["foo/bar".into(), "foo/baz".into()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_ok());
}

#[test]
fn duplicates_are_overlap() {
    let v: Vec<WuffPath> = vec!["foo/bar".into(), "foo/bar".into()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_err());
}

#[test]
fn parent_child_is_overlap() {
    let v: Vec<WuffPath> = vec!["foo/bar".into(), "foo/bar/baz/woof".into()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_err());
}

// try the other order...
#[test]
fn child_parent_is_overlap() {
    let v: Vec<WuffPath> = vec!["foo/bar/baz/woof".into(), "foo/bar".into()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_err());
}

#[test]
fn componentless_with_anything_else_is_overlap() {
    let v: Vec<WuffPath> = vec![WuffPath::new(), "foo/bar/baz/woof".into()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_err());
}

// and the other order...
#[test]
fn anything_with_componentless_is_overlap() {
    let v: Vec<WuffPath> = vec!["foo/bar/baz/woof".into(), WuffPath::new()];
    let result = WuffPath::check_for_overlap(&v);
    assert!(result.is_err());
}
