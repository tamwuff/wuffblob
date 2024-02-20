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

    pub fn provide_hash(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>, empirical_md5: [u8; 16]) {
        if let FileCheckerState::Hash = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Hash",
                &self.state
            );
        }

        self.empirical_md5 = Some(empirical_md5);
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
