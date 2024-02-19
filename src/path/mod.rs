#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct WuffPath {
    pub components: Vec<std::ffi::OsString>,
}

// You should be able to convert losslessly between OsStr and WuffPath. If
// converting from an OsStr to a WuffPath and then back to an OsStr ever gives
// you back a different OsStr than the one you started with, it is a bug!!
//
// That does not, however, mean that all WuffPaths are equally, shall we say,
// "kosher". It just means that if you have an OsStr that is treif, you can
// convert it to a WuffPath that is treif, and you can convert it back to the
// same treif OsStr.
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
                let c: &std::ffi::OsStr =
                    unsafe { std::ffi::OsStr::from_encoded_bytes_unchecked(&as_slice[i..=i]) };
                components.last_mut().unwrap().push(c);
            }
        }
        WuffPath {
            components: components,
        }
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

    pub fn is_canonical(&self) -> bool {
        // If it's empty, it's not canonical.
        if self.components.is_empty() {
            return false;
        }

        // If any of its components are ".", "..", or contain a NUL byte,
        // it's not canonical. If it starts or ends with a slash, or contains
        // two slashes in a row, it's not canonical.
        for component in &self.components {
            if component.is_empty()
                || (component == std::ffi::OsStr::new("."))
                || (component == std::ffi::OsStr::new(".."))
            {
                return false;
            }
            for byte in component.as_encoded_bytes() {
                if *byte == 0u8 {
                    return false;
                }
            }
        }

        true
    }

    pub fn canonicalize(&self) -> Option<WuffPath> {
        // If it's empty or starts with a slash, it's not canonicalizable.
        if self.components.is_empty() || self.components[0].is_empty() {
            return None;
        }

        // Try just removing all the dots, and all the consecutive slashes.
        // See if that fixes it.
        let mut cloned: WuffPath = self.clone();
        cloned
            .components
            .retain(|component: &std::ffi::OsString| -> bool {
                (!component.is_empty()) && (component != std::ffi::OsStr::new("."))
            });

        // We didn't check for ".." or for NUL bytes. Do that now.
        if cloned.is_canonical() {
            Some(cloned)
        } else {
            None
        }
    }

    // This method will not work right if we are not in canonical form.
    // Please only call this if is_canonical() has returned true!
    pub fn all_parents<F>(&self, mut cb: F)
    where
        F: FnMut(&WuffPath),
    {
        let mut len: usize = self.components.len();
        if len <= 1 {
            return;
        }
        let mut parent: WuffPath = self.clone();
        while len > 1 {
            len -= 1;
            parent.components.truncate(len);
            cb(&parent);
        }
    }
}

impl From<&str> for WuffPath {
    fn from(s: &str) -> WuffPath {
        WuffPath::from_osstr(std::ffi::OsStr::new(s))
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
fn single_has_no_parents() {
    let mut v = Vec::<std::ffi::OsString>::new();
    let cb = |wp: &WuffPath| {
        v.push(wp.to_osstring());
    };
    let p: WuffPath = "foo".into();
    p.all_parents(cb);
    assert!(v.is_empty());
}

#[test]
fn plenty_of_parents() {
    let mut v = Vec::<std::ffi::OsString>::new();
    let cb = |wp: &WuffPath| {
        v.push(wp.to_osstring());
    };
    let p: WuffPath = "foo/bar/baz".into();
    p.all_parents(cb);
    assert_eq!(
        v,
        vec!(
            Into::<std::ffi::OsString>::into(String::from("foo/bar")),
            Into::<std::ffi::OsString>::into(String::from("foo")),
        )
    );
}
