// BPH = "Bulletproof hashing"
//
// No need to use this module if you're hashing something in the usual
// straightforward way. You don't need the overhead.
//
// This module is for when you're hashing something and you expect parts
// of it to be repeated, and you want to make very very sure that either
// (a) you get the right answer, or (b) if you don't get the right answer,
// it's not a silent failure, you know that you didn't get the right answer.

#[derive(Debug)]
pub struct BulletproofHasher {
    hasher: md5::Md5,
    off: u64,
}

impl BulletproofHasher {
    pub fn new() -> BulletproofHasher {
        BulletproofHasher {
            hasher: <md5::Md5 as md5::Digest>::new(),
            off: 0u64,
        }
    }

    pub fn update(&mut self, off: u64, mut buf: &[u8]) {
        if off > self.off {
            return;
        }
        let chop_off: usize = (self.off - off) as usize;
        if chop_off >= buf.len() {
            return;
        }
        buf = &buf[chop_off..];
        <md5::Md5 as md5::Digest>::update(&mut self.hasher, buf);
        self.off += buf.len() as u64;
    }

    pub fn digest(
        &self,
        off: u64,
    ) -> Result<[u8; 16], crate::error::WuffError> {
        if off == self.off {
            Ok(<md5::Md5 as md5::Digest>::finalize(self.hasher.clone())
                .as_slice()
                .try_into()
                .unwrap())
        } else {
            Err(crate::error::WuffError::from(format!(
                "hashed up to {}, size was asserted as {}",
                self.off, off
            )))
        }
    }
}

#[test]
fn empty() {
    // md5 /dev/null
    let correct_hash = [
        0xd4u8, 0x1du8, 0x8cu8, 0xd9u8, 0x8fu8, 0x00u8, 0xb2u8, 0x04u8,
        0xe9u8, 0x80u8, 0x09u8, 0x98u8, 0xecu8, 0xf8u8, 0x42u8, 0x7eu8,
    ];
    let mut hasher = BulletproofHasher::new();
    let hash = hasher.digest(0);
    assert!(hash.is_ok());
    assert_eq!(hash.unwrap(), correct_hash);
}

#[test]
fn one_shot_correct_length() {
    // echo -n 'Hello, world!' | md5
    let correct_hash = [
        0x6cu8, 0xd3u8, 0x55u8, 0x6du8, 0xebu8, 0x0du8, 0xa5u8, 0x4bu8,
        0xcau8, 0x06u8, 0x0bu8, 0x4cu8, 0x39u8, 0x47u8, 0x98u8, 0x39u8,
    ];
    let mut hasher = BulletproofHasher::new();
    hasher.update(0, b"Hello, world!");
    let hash = hasher.digest(13);
    assert!(hash.is_ok());
    assert_eq!(hash.unwrap(), correct_hash);
}

#[test]
fn one_shot_incorrect_length() {
    let mut hasher = BulletproofHasher::new();
    hasher.update(0, b"Hello, world!");
    let hash = hasher.digest(15); // correct length is 13
    assert!(hash.is_err());
}

#[test]
fn gap() {
    let mut hasher = BulletproofHasher::new();
    hasher.update(1, b"ello, world!");
    let hash = hasher.digest(13);
    assert!(hash.is_err());
}

#[test]
fn multi_pass() {
    // echo -n 'Hello, world!' | md5
    let correct_hash = [
        0x6cu8, 0xd3u8, 0x55u8, 0x6du8, 0xebu8, 0x0du8, 0xa5u8, 0x4bu8,
        0xcau8, 0x06u8, 0x0bu8, 0x4cu8, 0x39u8, 0x47u8, 0x98u8, 0x39u8,
    ];
    let mut hasher = BulletproofHasher::new();

    // 0 - 3
    hasher.update(0, b"Hel");

    // 4 - 9, should be ignored
    hasher.update(4, b"o, wo");

    // 3 - 7
    hasher.update(3, b"lo, ");

    // 5 - 9
    hasher.update(5, b", wo");

    // 0 - 3 again, should be ignored
    hasher.update(0, b"Hel");

    // 5 - 9 again, should be ignored
    hasher.update(5, b", wo");

    // 5 - 10
    hasher.update(5, b", wor");

    // 100 - 103, should be ignored
    hasher.update(100, b"foo");

    // 10 - 13
    hasher.update(10, b"ld!");

    let hash = hasher.digest(13);
    assert!(hash.is_ok());
    assert_eq!(hash.unwrap(), correct_hash);
}
