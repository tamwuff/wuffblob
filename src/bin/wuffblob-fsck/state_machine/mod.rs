#[derive(Debug, Clone)]
pub enum RepairType {
    ContentType,
    Md5,
}

#[derive(Debug, Clone)]
pub struct ProposedRepair {
    pub repair_type: RepairType,
    pub problem_statement: String,
    pub question: &'static str,
    pub action: &'static str,
}

#[derive(Debug, Clone)]
pub enum FileCheckerState {
    // I don't need to be put into any queue. Terminal state.
    Terminal,

    // Please put me into the user-interface queue. I don't need any response
    // from the user, but I would like a message to be printed.
    Message(String),

    // Please put me into the user-interface queue. We can fix this, but we
    // need to keep the user informed and/or ask them whether to go ahead.
    UserInteraction(ProposedRepair),

    // Please put me into the queue to be hashed.
    Hash,

    // Please put me into the queue for the final properties update.
    UpdateProperties,
}

#[derive(Debug)]
pub struct FileChecker {
    pub path: wuffblob::path::WuffPath,
    pub desired_content_type: &'static str,
    pub is_dir: bool,
    pub properties: azure_storage_blobs::blob::BlobProperties,
    pub properties_dirty: bool,
    pub state: FileCheckerState,
    pub empirical_md5: Option<[u8; 16]>,
    pub possible_repairs: Vec<ProposedRepair>,
}

impl FileChecker {
    pub fn new(
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        path: wuffblob::path::WuffPath,
        is_dir: bool,
        properties: azure_storage_blobs::blob::BlobProperties,
    ) -> FileChecker {
        let mut checker: FileChecker = FileChecker {
            path: path,
            desired_content_type: "",
            is_dir: is_dir,
            properties: properties,
            properties_dirty: false,
            state: FileCheckerState::Terminal,
            empirical_md5: None,
            possible_repairs: Vec::<ProposedRepair>::new(),
        };
        if !checker.path.is_canonical() {
            checker.state = FileCheckerState::Message("path is not in canonical form".to_string());
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.any_not_repaired = true;
            });
        } else if checker.is_dir {
            // not much else to check!
        } else {
            checker.desired_content_type = ctx.base_ctx.get_desired_mime_type(&checker.path);
            if checker.properties.content_md5.is_none() || !ctx.preen {
                checker.state = FileCheckerState::Hash;
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.hash_required += 1u64;
                    stats.hash_bytes_required += checker.properties.content_length;
                });
            } else {
                checker.analyze(ctx);
            }
        }
        checker
    }

    pub fn provide_user_input(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>, answer: bool) {
        if let FileCheckerState::UserInteraction(ref possible_repair) = self.state {
            if answer {
                match possible_repair.repair_type {
                    RepairType::ContentType => {
                        self.properties.content_type = self.desired_content_type.to_string();
                    }
                    RepairType::Md5 => {
                        self.properties.content_md5 = Some(
                            azure_storage::prelude::ConsistencyMD5::decode(base64::Engine::encode(
                                &base64::prelude::BASE64_STANDARD,
                                self.empirical_md5.as_ref().unwrap(),
                            ))
                            .unwrap(),
                        );
                    }
                }
                self.properties_dirty = true;
            } else {
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.any_not_repaired = true;
                });
            }
            if let Some(next_possible_repair) = self.possible_repairs.pop() {
                self.state = FileCheckerState::UserInteraction(next_possible_repair);
            } else if self.properties_dirty {
                self.state = FileCheckerState::UpdateProperties;
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.user_input_complete += 1u64;
                    stats.propupd_required += 1u64;
                });
            } else {
                self.state = FileCheckerState::Terminal;
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.user_input_complete += 1u64;
                });
            }
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::UserInteraction",
                &self.state
            );
        }
    }

    pub fn message_printed(&mut self) {
        if let FileCheckerState::Message(_) = self.state {
            self.state = FileCheckerState::Terminal;
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Message",
                &self.state
            );
        }
    }

    pub fn hash_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if let FileCheckerState::Hash = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Hash",
                &self.state
            );
        }

        self.state = FileCheckerState::Message(format!("{}", err));
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.any_not_repaired = true;
        });
    }

    pub fn provide_hash(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        empirical_md5: &[u8; 16],
    ) {
        if let FileCheckerState::Hash = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Hash",
                &self.state
            );
        }

        self.empirical_md5 = Some(*empirical_md5); // array is Copy
        self.analyze(ctx);
    }

    pub fn update_properties_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if let FileCheckerState::UpdateProperties = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::UpdateProperties",
                &self.state
            );
        }

        self.state = FileCheckerState::Message(format!("{}", err));
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.any_not_repaired = true;
        });
    }

    pub fn properties_updated(&mut self) {
        if let FileCheckerState::UpdateProperties = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::UpdateProperties",
                &self.state
            );
        }
        self.state = FileCheckerState::Terminal;
    }

    fn analyze(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if self.properties.content_type != self.desired_content_type {
            self.possible_repairs.push(ProposedRepair {
                repair_type: RepairType::ContentType,
                problem_statement: format!(
                    "Content type is {} but it should be {}",
                    self.properties.content_type, self.desired_content_type
                ),
                question: "Adjust",
                action: "Adjusted",
            });
        }
        if let Some(ref empirical_md5) = self.empirical_md5 {
            if let Some(ref md5_from_metadata) = self.properties.content_md5 {
                if md5_from_metadata.as_slice() != empirical_md5 {
                    self.possible_repairs.push(ProposedRepair {
                        repair_type: RepairType::Md5,
                        problem_statement: format!(
                            "Metadata lists MD5 hash as {} but it is actually {}",
                            wuffblob::util::hex_encode(md5_from_metadata.as_slice()),
                            wuffblob::util::hex_encode(empirical_md5)
                        ),
                        question: "Update hash",
                        action: "Hash updated",
                    });
                }
            } else {
                self.possible_repairs.push(ProposedRepair {
                    repair_type: RepairType::Md5,
                    problem_statement: format!(
                        "Metadata does not list an MD5 hash. Correct hash is {}",
                        wuffblob::util::hex_encode(empirical_md5)
                    ),
                    question: "Update hash",
                    action: "Hash updated",
                });
            }
        }
        if let Some(possible_repair) = self.possible_repairs.pop() {
            self.state = FileCheckerState::UserInteraction(possible_repair);
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.user_input_required += 1;
            });
        } else {
            self.state = FileCheckerState::Terminal;
        }
    }
}

#[test]
fn non_canonical_path_gets_tossed_right_away() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let file_checker = FileChecker::new(
        &ctx,
        "foo/../bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, None),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Message(_)),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}

#[test]
fn dir_immediately_goes_to_good() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar".into(),
        true, // is_dir
        wuffblob::util::fake_blob_properties_directory(),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(!stats.any_not_repaired);
}

// Files with no hashes should get hashed, no matter what, preen mode or not.
#[test]
fn preen_file_with_no_hash_gets_hashed() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = true;
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, None),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );
}

// Files with no hashes should get hashed, no matter what, preen mode or not.
#[test]
fn nonpreen_file_with_no_hash_gets_hashed() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = false;
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, None),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );
}

#[test]
fn preen_file_with_hash_does_not_get_hashed() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = true;
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(!stats.any_not_repaired);
}

#[test]
fn nonpreen_file_with_hash_gets_hashed() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = false;
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );
}

// In non-preen mode, we'd hash it. But in preen mode, we go straight to
// noticing the problem with the content type.
#[test]
fn preen_file_with_bad_content_type_suggests_repair() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = true;
    let file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );
}

// It's non-preen, so they'll ask us to hash it, but when we hash it, plot
// twist, the hash was correct in the first place! Haha. Made you look.
#[test]
fn nonpreen_file_with_correct_hash_goes_to_good() {
    let hash = [23u8; 16];
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = false;
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&hash)),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );

    // We hashed it, surprise, it was the correct hash all along!
    file_checker.provide_hash(&ctx, &hash);

    // It should be good to go.
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(!stats.any_not_repaired);
}

// If it wants us to hash it, and we try to hash it, and the hash fails, it
// should go to Message.
#[test]
fn hash_failed_goes_to_message() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, None),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );

    // Oops, the hash failed.
    file_checker.hash_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));

    // It should be in Message now.
    assert!(
        matches!(file_checker.state, FileCheckerState::Message(_)),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}

// Hoo boy. This file is a file with problems. It's got a crazy content type,
// AND its hash is all screwy. But don't worry. Don't you worry about a thing,
// kid. We can fix ya right up.
#[test]
fn file_with_crazy_everything_goes_and_gets_itself_fixed() {
    let incorrect_hash = [23u8; 16];
    let correct_hash = [42u8; 16];
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&incorrect_hash)),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_hash(&ctx, &correct_hash);

    // Actually the order shouldn't matter. But we happen to know that the
    // first repair it will propose, will be the hash.
    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::Md5,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_user_input(&ctx, true);

    // Now it should suggest the content type.
    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_user_input(&ctx, true);

    // We said yes to both, so it should be ready for its properties update.
    assert!(
        matches!(file_checker.state, FileCheckerState::UpdateProperties),
        "State: {:?}",
        &file_checker.state
    );
    assert!(file_checker.properties_dirty);
    assert_eq!(file_checker.properties.content_type, "text/plain");
    assert_eq!(
        file_checker.properties.content_md5.unwrap().as_slice(),
        &correct_hash
    );

    // We didn't say no to nothin', so it can't fault us for nothin'. Not
    // yet, leastways.
    let stats = ctx.get_stats();
    assert!(!stats.any_not_repaired);
}

// But what happens when we say no to some of the suggested repairs? What
// then, wise guy???
#[test]
fn file_that_doesnt_really_actually_want_to_get_fully_fixed() {
    let incorrect_hash = [23u8; 16];
    let correct_hash = [42u8; 16];
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&incorrect_hash)),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_hash(&ctx, &correct_hash);

    // Actually the order shouldn't matter. But we happen to know that the
    // first repair it will propose, will be the hash.
    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::Md5,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );

    // NO
    //
    // WE LIKE OUR HASHES THIS WAY
    //
    // THEY ARE VERY FLAVORFUL WHEN YOU COOK THEM LIKE THIS
    //
    // YOU SEE
    //
    // THESE HASHES ARE NOT REALLY INCORRECT
    //
    // THEY ARE JUST CORRECT IN A WAY THAT TASTES BAD TO YOU
    //
    // BECAUSE YOU DID NOT GROW UP EATING HASHES THAT WERE PREPARED LIKE THIS
    //
    // SO WHEN WE SAY GO LICK A BEAN
    //
    // WE MEAN THAT ONLY IN THE VERY MOST KIND AND LOVING WAY
    //
    // BUT SRSLY DUDE GO LICK A DAMN BEAN
    file_checker.provide_user_input(&ctx, false);

    // Now it should suggest the content type.
    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_user_input(&ctx, true);

    // Now it should be ready for its properties update
    assert!(
        matches!(file_checker.state, FileCheckerState::UpdateProperties),
        "State: {:?}",
        &file_checker.state
    );
    assert!(file_checker.properties_dirty);
    assert_eq!(file_checker.properties.content_type, "text/plain");
    // Should still have the incorrect hash...
    assert_eq!(
        file_checker.properties.content_md5.unwrap().as_slice(),
        &incorrect_hash
    );

    // And it should remember that we didn't say yes to everything.
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}

// And if we say no to *everything*?
#[test]
fn file_that_seriously_is_not_ok_with_being_fixed_goes_straight_to_terminal() {
    let incorrect_hash = [23u8; 16];
    let correct_hash = [42u8; 16];
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&incorrect_hash)),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Hash),
        "State: {:?}",
        &file_checker.state
    );

    file_checker.provide_hash(&ctx, &correct_hash);

    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::Md5,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );
    file_checker.provide_user_input(&ctx, false);

    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );
    file_checker.provide_user_input(&ctx, false);

    // It should skip UpdateProperties and go straight to Terminal
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );

    // Nothing should have been fixed with the properties
    assert!(!file_checker.properties_dirty);
    assert_eq!(file_checker.properties.content_type, "text/squirrel");
    assert_eq!(
        file_checker.properties.content_md5.unwrap().as_slice(),
        &incorrect_hash
    );

    // But it should remember that we didn't say yes.
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}

// If it offers to fix something, we say yes, we fix it, and it succeeds, it
// should go to Terminal.
#[test]
fn got_fixed_goes_to_good() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = true;
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&[23u8; 16])),
    );

    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );
    file_checker.provide_user_input(&ctx, true);

    assert!(
        matches!(file_checker.state, FileCheckerState::UpdateProperties),
        "State: {:?}",
        &file_checker.state
    );
    assert!(file_checker.properties_dirty);
    assert_eq!(file_checker.properties.content_type, "text/plain");

    // Let's pretend the property change worked.
    file_checker.properties_updated();

    // It should be good to go.
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(!stats.any_not_repaired);
}

// If it offers to fix something, we say yes, we fix it, and it fails, it
// should go to Message.
#[test]
fn didnt_get_fixed() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    std::sync::Arc::get_mut(&mut ctx).unwrap().preen = true;
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/squirrel", 42, Some(&[23u8; 16])),
    );

    assert!(
        matches!(
            file_checker.state,
            FileCheckerState::UserInteraction(ProposedRepair {
                repair_type: RepairType::ContentType,
                ..
            })
        ),
        "State: {:?}",
        &file_checker.state
    );
    file_checker.provide_user_input(&ctx, true);

    assert!(
        matches!(file_checker.state, FileCheckerState::UpdateProperties),
        "State: {:?}",
        &file_checker.state
    );
    assert!(file_checker.properties_dirty);
    assert_eq!(file_checker.properties.content_type, "text/plain");

    // Let's pretend the property change failed.
    file_checker.update_properties_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));

    assert!(
        matches!(file_checker.state, FileCheckerState::Message(_)),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}

// The Message state is just a state that says, I'm really terminal, but I
// gotta say something first. Once we give it a chance to say what it has to,
// it should go to Terminal.
//
// We use a non-canonical path as the quickest way to get it to go into
// Message.
#[test]
fn message_goes_to_terminal_after_being_printed() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    let mut file_checker = FileChecker::new(
        &ctx,
        "foo/../bar.txt".into(),
        false, // is_dir
        wuffblob::util::fake_blob_properties_file("text/plain", 42, None),
    );
    assert!(
        matches!(file_checker.state, FileCheckerState::Message(_)),
        "State: {:?}",
        &file_checker.state
    );
    file_checker.message_printed();
    assert!(
        matches!(file_checker.state, FileCheckerState::Terminal),
        "State: {:?}",
        &file_checker.state
    );
    let stats = ctx.get_stats();
    assert!(stats.any_not_repaired);
}
