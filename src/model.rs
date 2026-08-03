use zcash_keys::keys::UnifiedFullViewingKey;

/// One row from `zn_view_keys`.
#[derive(Clone)]
pub struct ViewKeyConfig {
    pub recipient_address: String,
    pub ufvk: UnifiedFullViewingKey,
    pub reserve_table_name: String,
    pub birthday_height: Option<u32>,
    /// Derived from the UFVK's transparent component at index 0.
    /// None if the UFVK has no transparent component.
    pub transparent_script: Option<Vec<u8>>,
}

/// The full set of active view keys loaded from the database.
#[derive(Clone, Default)]
pub struct KeySet {
    /// Keyed by `recipient_address`.
    pub keys: std::collections::HashMap<String, ViewKeyConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Mempool,
    Confirmed,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mempool => "mempool",
            Self::Confirmed => "confirmed",
        }
    }
}

/// A detected payment output ready to be written to a reserve table.
#[derive(Clone, Debug)]
pub struct DetectedOutput {
    pub recipient_address: String,
    pub reserve_table_name: String,
    pub txid: String,
    pub pool: &'static str,
    pub output_index: i32,
    pub amount_zats: i64,
    pub sender_address: String,
    pub memo: Option<String>,
    pub status: Status,
    pub blockheight: Option<i64>,
}
